// Package breakingit exports k6 metrics to the Breaking-IT pg-proxy.
package breakingit

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/gofrs/uuid/v5"
	"go.k6.io/k6/v2/metrics"
	"go.k6.io/k6/v2/output"
)

// MetricGroup groups samples by timestamp
type MetricGroup struct {
	Timestamp  int64
	Tags       map[string]string
	Values     map[string]float64
	LastUpdate time.Time
	// HTTP error data (only populated when K6_HTTP_ERROR_DATA is enabled)
	ErrorReqHeaders string
	ErrorReqBody    string
	ErrorResHeaders string
	ErrorResBody    string
}

// excludedTag identifies tags omitted from database rows to reduce cardinality.
func excludedTag(tag string) bool {
	switch tag {
	case "proto", "tls_version", "url", "scenario", "expected_response",
		"from_cache", "from_prefetch_cache", "from_service_worker":
		return true
	default:
		return false
	}
}

// Retry configuration constants
const (
	maxRetryTimeMs   = 20000 // 20 seconds
	initialBackoffMs = 1100  // Start with 1.1 seconds
	maxBackoffMs     = 5000  // Cap at 5 seconds
)

// Logger writes k6 metric samples to pg-proxy using COPY via multipart batch
type Logger struct {
	out        io.Writer
	proxyURL   string
	database   string
	token      string
	httpClient *http.Client

	// batch buffers
	mu          sync.Mutex
	bufHTTP     *bytes.Buffer
	bufVUs      *bytes.Buffer
	bufTrans    *bytes.Buffer
	bufErrors   *bytes.Buffer
	batchTimer  *time.Timer
	firstData   chan struct{}
	firstQueued bool
	cleanupDone chan struct{}
	flushing    bool // flag to prevent recursive flush calls

	// pending screenshots to upsert via /upsert endpoint (low-volume)
	pendingScreenshots [][]any

	// merging state
	pendingHTTPGroups map[int64]*MetricGroup // http and browser (unified)
	cleanupStop       chan struct{}
	batchDone         chan struct{} // closed when batchLoop exits
	envTags           map[string]string
	customPatterns    map[string]string
	isSynthetic       bool
	transactionSource transactionSource
}

// New constructs a pg-proxy output from the configured environment.
func New(params output.Params) (output.Output, error) {
	// Expect params.ConfigArgument to be pg-proxy base url, e.g. https://proxy/ingest
	base := params.ConfigArgument
	if base == "" {
		return nil, fmt.Errorf("pg-proxy base URL required, e.g. https://host:8080")
	}
	u, err := url.Parse(base)
	if err != nil {
		return nil, fmt.Errorf("invalid pg-proxy URL: %w", err)
	}
	// Allow env overrides
	db := params.Environment["PG_PROXY_DB"]
	if db == "" {
		db = "breakingit"
	}
	token := params.Environment["PG_PROXY_TOKEN"]
	if token == "" {
		return nil, fmt.Errorf("PG_PROXY_TOKEN is required for Authorization")
	}
	transactionSource, err := parseTransactionSource(params.Environment["BREAKINGIT_TRANSACTION_SOURCE"])
	if err != nil {
		return nil, err
	}

	// Preserve the private-proxy TLS default; callers can require certificate verification.
	insecure := true
	if value := params.Environment["PG_PROXY_INSECURE_SKIP_TLS_VERIFY"]; value != "" {
		insecure, err = strconv.ParseBool(value)
		if err != nil {
			return nil, fmt.Errorf("invalid PG_PROXY_INSECURE_SKIP_TLS_VERIFY: %w", err)
		}
	}
	tr := &http.Transport{TLSClientConfig: &tls.Config{
		InsecureSkipVerify: insecure, //nolint:gosec // Explicit private-proxy configuration, independent of test traffic.
	}}
	client := &http.Client{Transport: tr, Timeout: 30 * time.Second}

	// Environment tags from generator
	envTags := make(map[string]string)
	isSynthetic := strings.ToLower(params.Environment["syntheticMonitoring"]) == "true"
	for _, key := range []string{"nodeName", "location", "scenarioName", "runId"} {
		if value := params.Environment[key]; value != "" {
			envTags[key] = value
		}
	}

	// normalize snake_case keys expected by DB
	if v, ok := envTags["runId"]; ok {
		envTags["run_id"] = v
	}
	if v, ok := envTags["nodeName"]; ok {
		envTags["node_name"] = v
	}
	if v, ok := envTags["scenarioName"]; ok {
		envTags["scenario_name"] = v
	}

	customPatterns := parseCustomPatterns(params.StdOut, params.Environment["CUSTOM_URL_PATTERNS"])

	l := &Logger{
		out:               params.StdOut,
		proxyURL:          strings.TrimRight(u.String(), "/"),
		database:          db,
		token:             token,
		httpClient:        client,
		bufHTTP:           &bytes.Buffer{},
		bufVUs:            &bytes.Buffer{},
		bufTrans:          &bytes.Buffer{},
		bufErrors:         &bytes.Buffer{},
		pendingHTTPGroups: make(map[int64]*MetricGroup),
		cleanupStop:       make(chan struct{}),
		batchDone:         make(chan struct{}),
		envTags:           envTags,
		customPatterns:    customPatterns,
		isSynthetic:       isSynthetic,
		transactionSource: transactionSource,
	}
	l.firstData = make(chan struct{}, 1)
	l.cleanupDone = make(chan struct{})
	l.batchTimer = time.NewTimer(time.Second)
	go l.batchLoop()
	go l.cleanupProcessor()
	return l, nil
}

// Description returns the output name.
func (l *Logger) Description() string { return "breakingit" }

// Start satisfies the output lifecycle interface.
func (l *Logger) Start() error { return nil }

// Stop flushes remaining samples and stops background processing.
func (l *Logger) Stop() error {
	close(l.cleanupStop)
	<-l.cleanupDone
	<-l.batchDone
	// Finish incomplete groups before the final batch.
	for _, g := range l.pendingHTTPGroups {
		l.processHTTPGroup(g)
	}
	l.flush()
	return nil
}

func (l *Logger) batchLoop() {
	defer close(l.batchDone)
	defer l.batchTimer.Stop()
	select {
	case <-l.firstData:
	case <-l.cleanupStop:
		return
	}
	if !l.batchTimer.Stop() {
		select {
		case <-l.batchTimer.C:
		default:
		}
	}
	for {
		l.flush()
		l.batchTimer.Reset(time.Second)
		select {
		case <-l.batchTimer.C:
		case <-l.cleanupStop:
			return
		}
	}
}

func (l *Logger) cleanupProcessor() {
	defer close(l.cleanupDone)
	t := time.NewTicker(100 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			now := time.Now()
			toProcessHTTP := make([]*MetricGroup, 0)
			l.mu.Lock()
			for ts, g := range l.pendingHTTPGroups {
				if now.Sub(g.LastUpdate) > time.Second {
					toProcessHTTP = append(toProcessHTTP, g)
					delete(l.pendingHTTPGroups, ts)
				}
			}
			l.mu.Unlock()
			for _, g := range toProcessHTTP {
				l.processHTTPGroup(g)
			}
		case <-l.cleanupStop:
			return
		}
	}
}

func (l *Logger) flush() {
	l.mu.Lock()
	// Prevent recursive flush calls
	if l.flushing {
		l.mu.Unlock()
		return
	}
	hasBatchData := l.bufHTTP.Len() > 0 || l.bufVUs.Len() > 0 || l.bufTrans.Len() > 0 || l.bufErrors.Len() > 0
	hasScreenshots := len(l.pendingScreenshots) > 0
	if !hasBatchData && !hasScreenshots {
		l.mu.Unlock()
		return
	}

	// Transfer ownership so producers can fill the next batch during network I/O.
	bufHTTPSnapshot := l.bufHTTP.Bytes()
	bufVUsSnapshot := l.bufVUs.Bytes()
	bufTransSnapshot := l.bufTrans.Bytes()
	bufErrorsSnapshot := l.bufErrors.Bytes()
	l.bufHTTP, l.bufVUs = &bytes.Buffer{}, &bytes.Buffer{}
	l.bufTrans, l.bufErrors = &bytes.Buffer{}, &bytes.Buffer{}
	// Snapshot pending screenshots
	screenshotsSnapshot := l.pendingScreenshots
	l.pendingScreenshots = nil
	l.flushing = true
	l.mu.Unlock()

	// Send screenshots via upsert endpoint (separate from batch COPY path)
	if len(screenshotsSnapshot) > 0 {
		_, _ = fmt.Fprintf(l.out, "[screenshot] flush: sending %d screenshot(s) to upsert endpoint\n",
			len(screenshotsSnapshot))
		l.upsertScreenshots(screenshotsSnapshot)
	}

	// Skip batch if no COPY data
	if !hasBatchData {
		l.mu.Lock()
		l.flushing = false
		l.mu.Unlock()
		return
	}

	body, boundary := l.batchBody(bufHTTPSnapshot, bufVUsSnapshot, bufTransSnapshot, bufErrorsSnapshot)
	l.sendBatch(body, boundary)
	l.mu.Lock()
	l.flushing = false
	l.mu.Unlock()
}

// upsertScreenshots sends screenshots to the pg-proxy upsert endpoint.
// Uses INSERT ... ON CONFLICT DO NOTHING on the proxy side, so duplicate hashes are silently
// skipped.
func (l *Logger) upsertScreenshots(rows [][]any) {
	payload := map[string]any{
		"columns": []string{"hash", "data", "mime_type", "size_bytes"},
		"rows":    rows,
	}
	jsonBody, err := json.Marshal(payload)
	if err != nil {
		_, _ = fmt.Fprintf(l.out, "screenshot upsert marshal error: %v\n", err)
		return
	}
	_, _ = fmt.Fprintf(l.out, "[screenshot] upsert: POSTing %d bytes to %s/upsert/public.sm_screenshots\n",
		len(jsonBody), l.proxyURL)
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost,
		l.proxyURL+"/upsert/public.sm_screenshots", bytes.NewReader(jsonBody))
	if err != nil {
		_, _ = fmt.Fprintf(l.out, "screenshot upsert request error: %v\n", err)
		return
	}
	req.Header.Set("Authorization", "Bearer "+l.token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := l.httpClient.Do(req)
	if err != nil {
		_, _ = fmt.Fprintf(l.out, "screenshot upsert request error: %v\n", err)
		return
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		_, _ = fmt.Fprintf(l.out, "screenshot upsert error status=%d body=%s\n", resp.StatusCode, string(b))
	} else {
		_, _ = fmt.Fprintf(l.out, "screenshot upsert ok rows=%d json_bytes=%d\n", len(rows), len(jsonBody))
	}
}

// httpMetricCanonicalKey maps http_* and browser_http_* metric names to the
// canonical key used in MetricGroup.Values (http_req_* or data_received).
// Returns "" if the metric is not an HTTP-like request metric.
func httpMetricCanonicalKey(metricName string) string {
	switch metricName {
	case "http_req_duration", "browser_http_req_duration":
		return "http_req_duration"
	case "http_req_blocked", "browser_http_req_blocked":
		return "http_req_blocked"
	case "http_req_connecting", "browser_http_req_connecting":
		return "http_req_connecting"
	case "http_req_tls_handshaking", "browser_http_req_tls_handshaking":
		return "http_req_tls_handshaking"
	case "http_req_sending", "browser_http_req_sending":
		return "http_req_sending"
	case "http_req_waiting", "browser_http_req_waiting":
		return "http_req_waiting"
	case "http_req_receiving", "browser_http_req_receiving":
		return "http_req_receiving"
	case "http_req_failed", "browser_http_req_failed":
		return "http_req_failed"
	case "browser_data_received":
		return "data_received"
	default:
		return ""
	}
}

// AddMetricSamples routes metric samples to the corresponding database rows.
func (l *Logger) AddMetricSamples(samples []metrics.SampleContainer) {
	for _, sc := range samples {
		for _, s := range sc.GetSamples() {
			l.addMetricSample(s)
		}
	}
}

// formatRequestData combines request headers and body into a single string for the request_headers
// column.
func formatRequestData(headers, body string) string {
	if headers != "" && body != "" {
		return headers + "\n" + body
	}
	if headers != "" {
		return headers
	}
	return body
}

func (l *Logger) processHTTPGroup(g *MetricGroup) {
	if g == nil {
		return
	}
	// Map k6 HTTP metrics to requests_raw(_sm) schema
	// Non-SM: time, run_id, location, transaction_name, sampler_name, success, request_size,
	// response_size, response_code, response_connect_time, response_latency, response_time
	// SM (existing table: run_id last): time, scenario_name, location, node_name, thread_group_name,
	// transaction_name, sampler_name, success, request_size, response_size, response_code,
	// response_connect_time, response_latency, response_time, run_id

	// sampler_name: use path (normalized URL path if name was URL, or name itself capped at 64 chars
	// if not)
	samplerName := g.Tags["path"]
	if samplerName == "" {
		samplerName = g.Tags["name"]
	}
	// transaction_name: only set when we have an explicit "transaction" tag or "group" (from
	// group()/group_duration).
	// If neither is set, leave empty (do not use sampler_name for browser).
	isBrowser := g.Tags["resource_type"] != ""
	var transactionName string
	if g.Tags["transaction"] != "" {
		transactionName = g.Tags["transaction"]
	} else if g.Tags["group"] != "" {
		transactionName = strings.TrimPrefix(g.Tags["group"], "::")
		// Native group paths start with "::". Keep these even when their name matches
		// the request path; only discard the path/name fallback from copyTagsForHTTP.
		if isBrowser && !strings.HasPrefix(g.Tags["group"], "::") &&
			(transactionName == samplerName || transactionName == "/") {
			transactionName = ""
		}
	}
	// Browser OPTIONS requests (CORS preflight): append _preflight to sampler_name
	if isBrowser && g.Tags["method"] == "OPTIONS" {
		samplerName += "_preflight"
	}

	// success is the inverse of http_req_failed
	success := "true"
	if g.Values["http_req_failed"] != 0 {
		success = "false"
	}

	// response_code from status tag
	responseCode := g.Tags["status"]

	// Timing fields (cast to integer strings) - matching JMeter definitions
	// http_req_duration = sending + waiting + receiving (same for k6 HTTP and browser)
	// See: https://grafana.com/docs/k6/latest/using-k6/metrics/reference/
	connectTime, latency, responseTime := requestTimings(g.Values)

	// response_size from browser_data_received when present (browser only); k6 HTTP has no per-request
	// size in this group
	var responseSize any = ""
	if v, ok := g.Values["data_received"]; ok && v >= 0 {
		responseSize = int64(v)
	}

	var row []any
	if l.isSynthetic {
		// SM table has run_id last (existing table order)
		row = []any{
			time.Unix(0, g.Timestamp),
			g.Tags["scenario_name"],
			g.Tags["location"],
			g.Tags["node_name"],
			g.Tags["scenario"], // use k6 scenario tag as thread_group_name
			transactionName,
			samplerName,
			success,
			"", // request_size unknown
			responseSize,
			responseCode,
			connectTime,
			latency,
			responseTime,
			fmt.Sprintf("%v", l.envTags["run_id"]),
		}
	} else {
		row = []any{
			time.Unix(0, g.Timestamp),
			g.Tags["run_id"],
			g.Tags["location"],
			transactionName,
			samplerName,
			success,
			"", // request_size unknown
			responseSize,
			responseCode,
			connectTime,
			latency,
			responseTime,
		}
	}
	l.mu.Lock()
	l.writeBinaryRow(l.bufHTTP, row)
	// If not successful, also emit into requests_error(_sm)
	if success == "false" {
		l.writeHTTPError(g, transactionName, samplerName, responseCode, responseTime, connectTime, isBrowser)
	}

	l.mu.Unlock()
}

func (l *Logger) processWebSocketMetric(s metrics.Sample, ts int64) {
	// Process ws_connecting and ws_ping metrics as single-value requests
	m := s.Tags.Map()

	// Extract tags similar to HTTP metrics
	tags := make(map[string]string)
	copyTagsForHTTP(tags, m, l.envTags, l.customPatterns)

	// transaction_name: only set when group tag is present (strip leading "::" if present)
	transactionName := ""
	if groupVal, ok := m["group"]; ok && groupVal != "" {
		transactionName = strings.TrimPrefix(groupVal, "::")
	}

	// sampler_name: derive from name tag (if not URL, capped at 64), else process url tag, else metric
	// name
	samplerName := l.webSocketSamplerName(s)

	// response_code from status tag (101 for successful WebSocket connection)
	responseCode := m["status"]
	if responseCode == "" {
		responseCode = "101" // Default to 101 for WebSocket
	}

	// success based on status code (101 = success)
	success := "true"
	if responseCode != "101" {
		success = "false"
	}

	// response_time is the metric value
	responseTime := int64(s.Value)

	var row []any
	if l.isSynthetic {
		// SM requests_raw_sm (existing table: run_id last)
		row = []any{
			time.Unix(0, ts),
			tags["scenario_name"],
			tags["location"],
			tags["node_name"],
			tags["scenario"], // thread_group_name
			transactionName,
			samplerName,
			success,
			"", // request_size
			"", // response_size
			responseCode,
			"", // response_connect_time
			"", // response_latency
			responseTime,
			fmt.Sprintf("%v", l.envTags["run_id"]),
		}
	} else {
		// Non-SM: time, run_id, location, transaction_name, sampler_name, success, request_size,
		// response_size, response_code, response_connect_time, response_latency, response_time
		row = []any{
			time.Unix(0, ts),
			tags["run_id"],
			tags["location"],
			transactionName,
			samplerName,
			success,
			"", // request_size
			"", // response_size
			responseCode,
			"", // response_connect_time
			"", // response_latency
			responseTime,
		}
	}
	l.mu.Lock()
	l.writeBinaryRow(l.bufHTTP, row)
	l.mu.Unlock()
}

func (l *Logger) processCustomTrendMetric(s metrics.Sample, ts int64) {
	// Process custom_* Trend metrics with minimal schema
	m := s.Tags.Map()

	// Extract basic tags from envTags (already normalized to snake_case)
	tags := make(map[string]string)
	maps.Copy(tags, l.envTags)

	// Ensure per-sample scenario tag is propagated so SM thread_group_name is set
	if scen, ok := m["scenario"]; ok && scen != "" {
		tags["scenario"] = fmt.Sprintf("%v", scen)
	}

	// transaction_name: only set when group tag is present (strip leading "::" if present)
	transactionName := ""
	if groupVal, ok := m["group"]; ok && groupVal != "" {
		transactionName = strings.TrimPrefix(groupVal, "::")
	}

	// sampler_name: use 'name' tag if present (process as URL or plain name), otherwise metric name
	var samplerName string
	nameRaw := m["name"]
	if nameRaw != "" {
		// Check if name is a URL or plain name
		_, _, path, _ := processNameTag(nameRaw, l.customPatterns)
		// If name was a URL, path contains the normalized path; if not, path contains the name (capped at
		// 64)
		samplerName = path
	} else {
		// Fallback to metric name
		samplerName = s.Metric.Name
	}

	// success: always "true" for custom metrics
	success := "true"

	// response_time is the metric value
	responseTime := int64(s.Value)

	var row []any
	if l.isSynthetic {
		// SM requests_raw_sm (existing table: run_id last)
		row = []any{
			time.Unix(0, ts),
			tags["scenario_name"],
			tags["location"],
			tags["node_name"],
			tags["scenario"], // thread_group_name
			transactionName,
			samplerName,
			success,
			"", // request_size
			"", // response_size
			"", // response_code
			"", // response_connect_time
			"", // response_latency
			responseTime,
			fmt.Sprintf("%v", l.envTags["run_id"]),
		}
	} else {
		// Non-SM: time, run_id, location, transaction_name, sampler_name, success, request_size,
		// response_size, response_code, response_connect_time, response_latency, response_time
		row = []any{
			time.Unix(0, ts),
			tags["run_id"],
			tags["location"],
			transactionName,
			samplerName,
			success,
			"", // request_size
			"", // response_size
			"", // response_code
			"", // response_connect_time
			"", // response_latency
			responseTime,
		}
	}
	l.mu.Lock()
	l.writeBinaryRow(l.bufHTTP, row)
	l.mu.Unlock()
}

func (l *Logger) processBrowserError(s metrics.Sample, ts int64) {
	m := s.Tags.Map()
	errMsg := m["error"]
	if errMsg == "" {
		errMsg = "browser error"
	}

	// Derive sampler_name from 'name' tag or fallback to error message (capped at 64)
	samplerName := m["name"]
	if samplerName == "" {
		samplerName = truncateUTF8(errMsg, 64)
	}

	// transaction_name from 'transaction' tag, fallback to 'group' tag
	transactionName := ""
	if txnVal := m["transaction"]; txnVal != "" {
		transactionName = txnVal
	} else if groupVal := m["group"]; groupVal != "" {
		transactionName = strings.TrimPrefix(groupVal, "::")
	}

	// screenshot from the "screenshot" tag (base64-encoded PNG when K6_BROWSER_SCREENSHOT_ON_ERROR is
	// enabled)
	screenshot := m["screenshot"]
	screenshotHash := ""
	if screenshot != "" {
		// Calculate SHA-256 hash of the screenshot data for deduplication
		h := sha256.Sum256([]byte(screenshot))
		screenshotHash = hex.EncodeToString(h[:])
		_, _ = fmt.Fprintf(l.out, "[screenshot] received browser error with screenshot len=%d hash=%s\n",
			len(screenshot), screenshotHash)
	} else {
		_, _ = fmt.Fprintf(l.out, "[screenshot] received browser error WITHOUT screenshot (tag count=%d)\n", len(m))
	}

	// Generate UUIDv7
	id := generateUUIDv7(ts)

	// Write screenshot + error row under a single lock to ensure both are
	// captured in the same flush snapshot (prevents the error row referencing
	// a hash that was never sent to sm_screenshots).
	if l.isSynthetic {
		row := []any{
			time.Unix(0, ts), // time
			fmt.Sprintf("%v", l.envTags["scenario_name"]), // scenario_name
			fmt.Sprintf("%v", l.envTags["location"]),      // location
			fmt.Sprintf("%v", l.envTags["node_name"]),     // node_name
			m["scenario"],                          // thread_group_name
			transactionName,                        // transaction_name
			samplerName,                            // sampler_name
			"",                                     // response_code
			"",                                     // response_time
			"",                                     // connection_time
			"",                                     // url
			"",                                     // assertions
			errMsg,                                 // response_message
			"",                                     // request_headers
			"",                                     // response_headers
			screenshotHash,                         // response_data (screenshot hash reference)
			id,                                     // id (UUIDv7)
			fmt.Sprintf("%v", l.envTags["run_id"]), // run_id last
		}
		l.mu.Lock()
		if screenshotHash != "" {
			l.pendingScreenshots = append(l.pendingScreenshots, []any{
				screenshotHash, screenshot, "image/png", len(screenshot),
			})
			_, _ = fmt.Fprintf(l.out, "[screenshot] queued for upsert, pendingScreenshots=%d\n", len(l.pendingScreenshots))
		}
		l.writeBinaryRow(l.bufErrors, row)
		l.mu.Unlock()
	} else {
		row := []any{
			time.Unix(0, ts),                          // time
			fmt.Sprintf("%v", l.envTags["run_id"]),    // run_id
			fmt.Sprintf("%v", l.envTags["location"]),  // location
			fmt.Sprintf("%v", l.envTags["node_name"]), // node_name
			transactionName,                           // transaction_name
			samplerName,                               // sampler_name
			"",                                        // response_code
			"",                                        // response_time
			"",                                        // connection_time
			"",                                        // url
			"",                                        // assertions
			errMsg,                                    // response_message
			"",                                        // request_headers
			"",                                        // response_headers
			screenshotHash,                            // response_data (screenshot hash reference)
			id,                                        // id (UUIDv7)
		}
		l.mu.Lock()
		if screenshotHash != "" {
			l.pendingScreenshots = append(l.pendingScreenshots, []any{
				screenshotHash, screenshot, "image/png", len(screenshot),
			})
		}
		l.writeBinaryRow(l.bufErrors, row)
		l.mu.Unlock()
	}
}

// generateUUIDv7 generates a UUIDv7 using the provided timestamp (nanoseconds since Unix epoch)
func generateUUIDv7(timestampNanos int64) string {
	// Convert nanoseconds to time.Time for the UUID generator
	t := time.Unix(0, timestampNanos)

	// Create a UUIDv7 with the specific timestamp
	// UUIDv7 format: 48-bit timestamp (ms) + 4-bit version + 12-bit rand_a + 2-bit variant + 62-bit
	// rand_b
	timestampMs := uint64(t.UnixMilli())

	// Build UUID bytes manually with the correct timestamp
	var uuidBytes [16]byte

	// First 48 bits: timestamp in milliseconds (big-endian)
	uuidBytes[0] = byte((timestampMs >> 40) & 0xff)
	uuidBytes[1] = byte((timestampMs >> 32) & 0xff)
	uuidBytes[2] = byte((timestampMs >> 24) & 0xff)
	uuidBytes[3] = byte((timestampMs >> 16) & 0xff)
	uuidBytes[4] = byte((timestampMs >> 8) & 0xff)
	uuidBytes[5] = byte(timestampMs & 0xff)

	// Generate random bytes for the rest
	randBytes := make([]byte, 10)
	rand.Read(randBytes)

	// Bytes 6-7: version (7) in high nibble + random
	uuidBytes[6] = 0x70 | (randBytes[0] & 0x0F) // Version 7
	uuidBytes[7] = randBytes[1]

	// Bytes 8-15: variant (10) in high 2 bits + random
	uuidBytes[8] = 0x80 | (randBytes[2] & 0x3F) // Variant 10
	copy(uuidBytes[9:], randBytes[3:])

	u, _ := uuid.FromBytes(uuidBytes[:])
	return u.String()
}

// URL parsing, returns scheme, host, path, cleanURL
func processURL(raw string, customPatterns map[string]string) (string, string, string, string) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", "", "", raw
	}
	scheme := u.Scheme
	host := u.Hostname()
	p := u.Path
	if p == "" {
		p = "/"
	}
	uuidPattern := regexp.MustCompile(`[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}`)
	p = uuidPattern.ReplaceAllString(p, "{uuid}")
	hexPattern := regexp.MustCompile(`[0-9a-f]{12,}`)
	p = hexPattern.ReplaceAllString(p, "{hex}")
	for pattern, replacement := range customPatterns {
		re := regexp.MustCompile(pattern)
		p = re.ReplaceAllString(p, replacement)
	}
	cleanURL := fmt.Sprintf("%s://%s%s", scheme, host, p)
	return scheme, host, p, cleanURL
}

// truncateUTF8 safely truncates a string to maxRunes characters without breaking UTF-8 sequences
func truncateUTF8(s string, maxRunes int) string {
	if utf8.RuneCountInString(s) <= maxRunes {
		return s
	}
	runes := []rune(s)
	return string(runes[:maxRunes])
}

// processNameTag processes the "name" tag: if it's a URL, parse and return path; otherwise return
// as-is (capped at 64 chars)
func processNameTag(nameRaw string, customPatterns map[string]string) (string, string, string, string) {
	// Try to parse as URL
	u, err := url.Parse(nameRaw)
	if err != nil || u.Scheme == "" {
		// Not a valid URL, use as-is but cap at 64 chars (rune-safe)
		nameRaw = truncateUTF8(nameRaw, 64)
		return "", "", nameRaw, nameRaw
	}
	// Valid URL - extract path (without query string)
	scheme := u.Scheme
	host := u.Hostname()
	p := u.Path
	if p == "" {
		p = "/"
	}
	// Apply normalization patterns
	uuidPattern := regexp.MustCompile(`[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}`)
	p = uuidPattern.ReplaceAllString(p, "{uuid}")
	hexPattern := regexp.MustCompile(`[0-9a-f]{12,}`)
	p = hexPattern.ReplaceAllString(p, "{hex}")
	for pattern, replacement := range customPatterns {
		re := regexp.MustCompile(pattern)
		p = re.ReplaceAllString(p, replacement)
	}
	cleanURL := fmt.Sprintf("%s://%s%s", scheme, host, p)
	return scheme, host, p, cleanURL
}

func copyTagsForHTTP(dst, tags, env, customPatterns map[string]string) {
	// start with env tags
	for k, v := range env {
		if !excludedTag(k) {
			// map snake_case for consistency
			if k == "nodeName" {
				dst["node_name"] = v
				continue
			}
			if k == "scenarioName" {
				dst["scenario_name"] = v
				continue
			}
			if k == "runId" {
				dst["run_id"] = v
				continue
			}
			dst[k] = v
		}
	}
	// name -> derive host/path (if URL) or use as-is (if not URL, capped at 64 chars)
	if name, ok := tags["name"]; ok {
		nameStr := fmt.Sprintf("%v", name)
		scheme, host, path, clean := processNameTag(nameStr, customPatterns)
		dst["name"] = clean
		if scheme != "" {
			// It was a URL, store scheme and hostname
			dst["hostname"] = host
			dst["path"] = path
		} else {
			// It was not a URL, path contains the name (capped at 64)
			dst["path"] = path
		}
	}
	for k, v := range tags {
		if !excludedTag(k) {
			// preserve normalized fields
			if k == "name" || k == "path" || k == "hostname" || k == "scheme" {
				continue
			}
			dst[k] = fmt.Sprintf("%v", v)
		}
	}
	// Explicitly include k6 'scenario' tag for use as thread_group_name in SM HTTP
	if v, ok := tags["scenario"]; ok {
		dst["scenario"] = fmt.Sprintf("%v", v)
	}
	// Browser samples often lack "group"; use path or name so processHTTPGroup gets a transaction name
	if dst["group"] == "" {
		dst["group"] = dst["path"]
		if dst["group"] == "" {
			dst["group"] = dst["name"]
		}
		if dst["group"] == "" {
			dst["group"] = "/"
		}
	}
}

func (l *Logger) processHTTPSample(s metrics.Sample, ts int64, canonicalKey string) {
	l.mu.Lock()
	g, ok := l.pendingHTTPGroups[ts]
	if !ok {
		g = &MetricGroup{Timestamp: ts, Tags: map[string]string{}, Values: map[string]float64{}, LastUpdate: time.Now()}
		l.pendingHTTPGroups[ts] = g
		copyTagsForHTTP(g.Tags, s.Tags.Map(), l.envTags, l.customPatterns)
	}
	g.Values[canonicalKey] = s.Value
	if canonicalKey == "http_req_failed" {
		g.ErrorReqHeaders = s.HTTPErrorReqHeaders
		g.ErrorReqBody = s.HTTPErrorReqBody
		g.ErrorResHeaders = s.HTTPErrorResHeaders
		g.ErrorResBody = s.HTTPErrorResBody
	}
	g.LastUpdate = time.Now()
	// Process when we have the 8 required metrics (including http_req_failed); data_received is
	// optional (browser only)
	if _, hasFailed := g.Values["http_req_failed"]; hasFailed && len(g.Values) >= 8 {
		delete(l.pendingHTTPGroups, ts)
		l.mu.Unlock()
		l.processHTTPGroup(g)
		return
	}
	l.mu.Unlock()
}

func (l *Logger) processPageSample(s metrics.Sample, ts int64) {
	m := s.Tags.Map()
	// Prefer explicit 'page' tag when available
	pageRaw := fmt.Sprintf("%v", m["page"])
	var transactionName string
	if pageRaw != "" && pageRaw != "<nil>" {
		transactionName = pageRaw
	} else {
		// Derive a stable transaction_name: prefer name, fallback to group, then '/'
		nameRaw := fmt.Sprintf("%v", m["name"])
		if nameRaw == "" || nameRaw == "<nil>" {
			nameRaw = fmt.Sprintf("%v", m["group"]) // sometimes present
		}
		if nameRaw == "" || nameRaw == "<nil>" {
			nameRaw = "/"
		}
		// Process name: if URL, use path; if not URL, use as-is (capped at 64)
		_, _, path, _ := processNameTag(nameRaw, l.customPatterns)
		transactionName = path
	}
	// Non-SM transactions table columns:
	//   time, run_id, location, transaction_name, success, request_size, response_size, response_time
	// SM transactions_sm (run_id last): time, scenario_name, location, node_name, thread_group_name,
	// transaction_name, success, request_size, response_size, response_time, run_id
	isSM := l.isSynthetic
	var row []any
	if isSM {
		row = []any{
			time.Unix(0, ts),
			fmt.Sprintf("%v", l.envTags["scenario_name"]),
			fmt.Sprintf("%v", l.envTags["location"]),
			fmt.Sprintf("%v", l.envTags["node_name"]),
			fmt.Sprintf("%v", m["scenario"]), // thread_group_name from k6 scenario tag
			transactionName,
			"true",
			"",
			"",
			int64(s.Value),
			fmt.Sprintf("%v", l.envTags["run_id"]),
		}
	} else {
		row = []any{
			time.Unix(0, ts),
			fmt.Sprintf("%v", l.envTags["run_id"]),
			fmt.Sprintf("%v", l.envTags["location"]),
			transactionName,
			"true",
			"",
			"",
			int64(s.Value),
		}
	}
	l.mu.Lock()
	l.writeBinaryRow(l.bufTrans, row)
	l.mu.Unlock()
}

func (l *Logger) processGroupSample(s metrics.Sample, ts int64) {
	success := strconv.FormatBool(!s.GroupFailed)
	m := s.Tags.Map()
	// Derive transaction_name from 'group' tag, fallback to 'name'
	nameRaw := fmt.Sprintf("%v", m["group"])
	if nameRaw == "" || nameRaw == "<nil>" {
		nameRaw = fmt.Sprintf("%v", m["name"])
	}
	nameRaw = strings.TrimPrefix(nameRaw, "::")
	if nameRaw == "" || nameRaw == "<nil>" {
		nameRaw = "/"
	}
	isSM := l.isSynthetic
	var row []any
	if isSM {
		// SM transactions_sm (run_id last)
		row = []any{
			time.Unix(0, ts),
			fmt.Sprintf("%v", l.envTags["scenario_name"]),
			fmt.Sprintf("%v", l.envTags["location"]),
			fmt.Sprintf("%v", l.envTags["node_name"]),
			fmt.Sprintf("%v", m["scenario"]), // thread_group_name from k6 scenario
			nameRaw,
			success,
			"",
			"",
			int64(s.Value),
			fmt.Sprintf("%v", l.envTags["run_id"]),
		}
	} else {
		row = []any{
			time.Unix(0, ts),
			fmt.Sprintf("%v", l.envTags["run_id"]),
			fmt.Sprintf("%v", l.envTags["location"]),
			nameRaw,
			success,
			"",
			"",
			int64(s.Value),
		}
	}
	l.mu.Lock()
	l.writeBinaryRow(l.bufTrans, row)
	l.mu.Unlock()
}

func (l *Logger) processVUsSample(s metrics.Sample, ts int64) {
	row := []any{
		time.Unix(0, ts),
		fmt.Sprintf("%v", l.envTags["run_id"]),
		fmt.Sprintf("%v", l.envTags["location"]),
		fmt.Sprintf("%v", l.envTags["node_name"]),
		int64(s.Value), // active_threads
		"",             // started_threads (unknown)
		"",             // finished_threads (unknown)
	}
	l.mu.Lock()
	l.writeBinaryRow(l.bufVUs, row)
	l.mu.Unlock()
}

func (l *Logger) processCheckSample(s metrics.Sample, ts int64) {
	m := s.Tags.Map()
	checkMsg := m["check"] // e.g. "is status 301"
	if checkMsg == "" {
		checkMsg = "check failed"
	}
	// Derive transaction_name from 'group' tag, strip leading '::'
	transactionName := ""
	if groupVal := m["group"]; groupVal != "" {
		transactionName = strings.TrimPrefix(groupVal, "::")
	}
	responseCode := m["status"] // may be empty
	// Use 'name' tag for sampler_name (this is passed from k6 check tags)
	samplerName := m["name"]
	if samplerName == "" {
		// Fallback to check message or generic name
		samplerName = checkMsg
	}
	// URL can be from 'url' tag or empty
	url := m["url"]
	// Get request/response data and error_response_time if provided via custom tags
	requestData := m["request_data"]
	responseData := m["response_data"]
	responseTime := m["error_response_time"] // response time in ms from check tags
	// Generate UUIDv7 using the measurement timestamp
	id := generateUUIDv7(ts)
	// Build row per schema (includes id field)
	if l.isSynthetic {
		// SM requests_error_sm (existing table: run_id last): time, scenario_name, ..., id, run_id
		row := []any{
			time.Unix(0, ts), // time
			fmt.Sprintf("%v", l.envTags["scenario_name"]), // scenario_name
			fmt.Sprintf("%v", l.envTags["location"]),      // location
			fmt.Sprintf("%v", l.envTags["node_name"]),     // node_name
			m["scenario"],                          // thread_group_name
			transactionName,                        // transaction_name
			samplerName,                            // sampler_name
			responseCode,                           // response_code
			responseTime,                           // response_time
			"",                                     // connection_time
			url,                                    // url
			"",                                     // assertions
			checkMsg,                               // response_message
			requestData,                            // request_headers (used for request data)
			"",                                     // response_headers
			responseData,                           // response_data
			id,                                     // id (UUIDv7 generated from measurement timestamp)
			fmt.Sprintf("%v", l.envTags["run_id"]), // run_id last
		}
		l.mu.Lock()
		l.writeBinaryRow(l.bufErrors, row)
		l.mu.Unlock()
	} else {
		row := []any{
			time.Unix(0, ts),                          // time
			fmt.Sprintf("%v", l.envTags["run_id"]),    // run_id
			fmt.Sprintf("%v", l.envTags["location"]),  // location
			fmt.Sprintf("%v", l.envTags["node_name"]), // node_name
			transactionName,                           // transaction_name
			samplerName,                               // sampler_name
			responseCode,                              // response_code
			responseTime,                              // response_time
			"",                                        // connection_time
			url,                                       // url
			"",                                        // assertions
			checkMsg,                                  // response_message
			requestData,                               // request_headers (used for request data)
			"",                                        // response_headers
			responseData,                              // response_data
			id,                                        // id (UUIDv7 generated from measurement timestamp)
		}
		l.mu.Lock()
		l.writeBinaryRow(l.bufErrors, row)
		l.mu.Unlock()
	}
}

func (l *Logger) batchBody(
	bufHTTPSnapshot, bufVUsSnapshot, bufTransSnapshot, bufErrorsSnapshot []byte,
) ([]byte, string) {
	var gzBody bytes.Buffer
	gz, _ := gzip.NewWriterLevel(&gzBody, gzip.BestSpeed)
	mw := multipart.NewWriter(gz)

	if len(bufHTTPSnapshot) > 0 {
		h := make(textproto.MIMEHeader)
		table := "public.requests_raw"
		if l.isSynthetic {
			table = "public.requests_raw_sm"
		}
		h.Set("X-Table", table)
		h.Set("X-Format", "binary")
		pw, _ := mw.CreatePart(h)
		_, _ = pw.Write([]byte(binaryCopyHeader))
		_, _ = pw.Write(bufHTTPSnapshot)
		_, _ = pw.Write([]byte{0xff, 0xff})
	}
	if len(bufVUsSnapshot) > 0 && !l.isSynthetic {
		h := make(textproto.MIMEHeader)
		h.Set("X-Table", "public.virtual_users")
		h.Set("X-Format", "binary")
		pw, _ := mw.CreatePart(h)
		_, _ = pw.Write([]byte(binaryCopyHeader))
		_, _ = pw.Write(bufVUsSnapshot)
		_, _ = pw.Write([]byte{0xff, 0xff})
	}
	if len(bufTransSnapshot) > 0 {
		h := make(textproto.MIMEHeader)
		table := "public.transactions"
		if l.isSynthetic {
			table = "public.transactions_sm"
		}
		h.Set("X-Table", table)
		h.Set("X-Format", "binary")
		pw, _ := mw.CreatePart(h)
		_, _ = pw.Write([]byte(binaryCopyHeader))
		_, _ = pw.Write(bufTransSnapshot)
		_, _ = pw.Write([]byte{0xff, 0xff})
	}
	if len(bufErrorsSnapshot) > 0 {
		h := make(textproto.MIMEHeader)
		table := "public.requests_error"
		if l.isSynthetic {
			table = "public.requests_error_sm"
		}
		h.Set("X-Table", table)
		h.Set("X-Format", "binary")
		pw, _ := mw.CreatePart(h)
		_, _ = pw.Write([]byte(binaryCopyHeader))
		_, _ = pw.Write(bufErrorsSnapshot)
		_, _ = pw.Write([]byte{0xff, 0xff})
	}
	_ = mw.Close()

	_ = gz.Close()

	return gzBody.Bytes(), mw.Boundary()
}

func (l *Logger) sendBatch(body []byte, boundary string) bool {
	start := time.Now()
	delay := time.Duration(initialBackoffMs) * time.Millisecond
	for attempts := 1; time.Since(start) < maxRetryTimeMs*time.Millisecond; attempts++ {
		err := l.postBatch(body, boundary)
		if err == nil {
			return true
		}
		elapsed := time.Since(start)
		_, _ = fmt.Fprintf(l.out, "pg-proxy ingest attempt %d failed (%v elapsed): %v\n", attempts, elapsed, err)
		if elapsed+delay >= maxRetryTimeMs*time.Millisecond {
			break
		}
		time.Sleep(delay)
		delay = min(delay*2, maxBackoffMs*time.Millisecond)
	}
	return false
}

func (l *Logger) postBatch(body []byte, boundary string) error {
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost,
		l.proxyURL+"/ingest/batch", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+l.token)
	req.Header.Set("Content-Type", "multipart/mixed; boundary="+boundary)
	req.Header.Set("Content-Encoding", "gzip")
	resp, err := l.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("status=%d body=%s", resp.StatusCode, body)
	}
	return nil
}

// writeHTTPError requires the buffer lock to be held by the caller.
func (l *Logger) writeHTTPError(g *MetricGroup,
	transactionName, samplerName, responseCode string, responseTime, connectTime int64, isBrowser bool,
) {
	// Build response_message from tags
	errText := fmt.Sprintf("%v", g.Tags["error"]) // may be empty
	if errText == "<nil>" {
		errText = ""
	}
	errMsg := fmt.Sprintf("%v", g.Tags["error_message"])
	if errMsg == "" || errMsg == "<nil>" {
		// fallback to error_code with prefix
		errorCode := fmt.Sprintf("%v", g.Tags["error_code"])
		if errorCode != "" && errorCode != "<nil>" {
			errMsg = fmt.Sprintf("K6 Error code %s", errorCode)
		} else {
			errMsg = ""
		}
	}
	// Construct responseMessage: use errText if available, errMsg if available, or both with " - "
	// separator
	var responseMessage string
	switch {
	case errText != "" && errMsg != "":
		responseMessage = fmt.Sprintf("%s - %s", errText, errMsg)
	case errText != "":
		responseMessage = errText
	case errMsg != "":
		responseMessage = errMsg
	default:
		responseMessage = ""
	}
	responseMessage = strings.TrimSpace(responseMessage)
	// Browser 4xx/5xx often have no "error" tag; use status for response_message
	if responseMessage == "" && responseCode != "" && responseCode != "0" {
		responseMessage = "HTTP " + responseCode
	}
	nameRaw := g.Tags["name"]
	// samplerName already derived; ensure non-empty for k6 HTTP (browser: leave empty when no
	// transaction name)
	if samplerName == "" || samplerName == "<nil>" {
		if !isBrowser {
			samplerName = transactionName
			if samplerName == "" || samplerName == "<nil>" {
				samplerName = "/"
			}
		}
	}
	// Generate UUIDv7 using the measurement timestamp
	id := generateUUIDv7(g.Timestamp)
	if l.isSynthetic {
		// SM requests_error_sm (existing table: run_id last): time, scenario_name, ..., id, run_id
		erow := []any{
			time.Unix(0, g.Timestamp),
			g.Tags["scenario_name"],
			g.Tags["location"],
			g.Tags["node_name"],
			g.Tags["scenario"],
			transactionName,
			samplerName,
			responseCode,
			responseTime,
			connectTime,
			nameRaw,
			"",
			responseMessage,
			formatRequestData(g.ErrorReqHeaders, g.ErrorReqBody), // request_headers (includes post payload)
			g.ErrorResHeaders, // response_headers
			g.ErrorResBody,    // response_data
			id,                // id (UUIDv7)
			fmt.Sprintf("%v", l.envTags["run_id"]),
		}
		l.writeBinaryRow(l.bufErrors, erow)
	} else {
		erow := []any{
			time.Unix(0, g.Timestamp),
			g.Tags["run_id"],
			g.Tags["location"],
			g.Tags["node_name"],
			transactionName,
			samplerName,
			responseCode,
			responseTime,
			connectTime,
			nameRaw,
			"",
			responseMessage,
			formatRequestData(g.ErrorReqHeaders, g.ErrorReqBody), // request_headers (includes post payload)
			g.ErrorResHeaders, // response_headers
			g.ErrorResBody,    // response_data
			id,                // id (UUIDv7)
		}
		l.writeBinaryRow(l.bufErrors, erow)
	}
}

func (l *Logger) webSocketSamplerName(s metrics.Sample) string {
	if s.Metric.Name == "ws_ping" {
		return "WSPing"
	}
	tags := s.Tags.Map()
	if name := tags["name"]; name != "" {
		_, _, path, _ := processNameTag(name, l.customPatterns)
		return path
	}
	if raw := tags["url"]; raw != "" {
		_, _, path, _ := processURL(raw, l.customPatterns)
		if path != "" {
			return path
		}
	}
	return s.Metric.Name
}

func (l *Logger) addMetricSample(s metrics.Sample) {
	ts := s.Time.UnixNano()
	canonicalKey := httpMetricCanonicalKey(s.Metric.Name)
	if canonicalKey != "" {
		l.processHTTPSample(s, ts, canonicalKey)
		return
	}
	if (s.Metric.Name == "pages" && l.transactionSource == transactionsGroups) ||
		(s.Metric.Name == "group_duration" && l.transactionSource == transactionsPages) {
		return
	}
	// pages -> transactions
	if s.Metric.Name == "pages" {
		l.processPageSample(s, ts)
		return
	}
	// Native HTTP and browser groups -> transactions.
	if s.Metric.Name == "group_duration" {
		l.processGroupSample(s, ts)
		return
	}
	// vus -> virtual_users (active_threads)
	if s.Metric.Name == "vus" && !l.isSynthetic {
		l.processVUsSample(s, ts)
		return
	}
	// checks -> requests_error(_sm) when value == 0 (failed check)
	if s.Metric.Name == "checks" && s.Value == 0 {
		l.processCheckSample(s, ts)
		return
	}
	// browser_errors -> requests_error(_sm)
	if s.Metric.Name == "browser_errors" {
		l.processBrowserError(s, ts)
		return
	}
	// WebSocket metrics -> requests_raw(_sm)
	if s.Metric.Name == "ws_connecting" || s.Metric.Name == "ws_ping" {
		l.processWebSocketMetric(s, ts)
		return
	}
	// Custom Trend metrics (custom_*) -> requests_raw(_sm) with minimal schema
	if strings.HasPrefix(s.Metric.Name, "custom_") {
		l.processCustomTrendMetric(s, ts)
		return
	}
	// removed http_req_failed-specific emission; handled in processHTTPGroup when success == false
}

func parseCustomPatterns(out io.Writer, patterns string) map[string]string {
	// Parse CUSTOM_URL_PATTERNS env (pattern==replacement comma-separated)
	customPatterns := make(map[string]string)
	if patterns != "" {
		_, _ = fmt.Fprintf(out, "Found CUSTOM_URL_PATTERNS environment variable: %s\n", patterns)
		for pair := range strings.SplitSeq(patterns, ",") {
			parts := strings.Split(pair, "==")
			if len(parts) == 2 {
				customPatterns[parts[0]] = parts[1]
				_, _ = fmt.Fprintf(out, "Added custom pattern: %s -> %s\n", parts[0], parts[1])
			} else {
				_, _ = fmt.Fprintf(out, "Warning: Invalid pattern format: %s\n", pair)
			}
		}
		_, _ = fmt.Fprintf(out, "Total custom patterns loaded: %d\n", len(customPatterns))
	} else {
		_, _ = fmt.Fprintf(out, "No CUSTOM_URL_PATTERNS environment variable found\n")
	}

	return customPatterns
}

// requestTimings converts k6 timings to the JMeter-style fields expected by BreakTest.
func requestTimings(values map[string]float64) (connectTime, latency, responseTime int64) {
	connect := values["http_req_connecting"] + values["http_req_tls_handshaking"]
	return int64(connect),
		int64(connect + values["http_req_sending"] + values["http_req_waiting"]),
		int64(connect + values["http_req_duration"])
}
