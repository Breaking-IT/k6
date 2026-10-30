package breakingit

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/klauspost/compress/zstd"
	"go.k6.io/k6/v2/metrics"
	"go.k6.io/k6/v2/output"
)

// CustomOutput reuses BreakTest's metric mappings, with bounded direct binary delivery.
// Only the sender owns incarnation/sequence. An ACK is acceptance, not an fsync guarantee.
type customGroupKey struct {
	time int64
	tags *metrics.TagSet
}

type CustomOutput struct {
	groups map[customGroupKey]*MetricGroup

	mapper                           *Logger
	client                           *http.Client
	base, token, runID               string
	incarnation                      string
	sequence                         uint64
	retryTimeout                     time.Duration
	encoder                          *zstd.Encoder
	queue                            chan customBatch
	done, tickStop, tickDone, failed chan struct{}
	batchMu                          sync.Mutex
	parts                            map[string]*bytes.Buffer
	batchBytes                       int
	addMu                            sync.Mutex // Serializes sample mapping and pending-group completion.
	failureMu                        sync.Mutex
	failure                          error
	stopCallback                     func(error)
	started                          bool
	stopOnce                         sync.Once
	stopErr                          error
}
type customBatch struct {
	body        []byte
	contentType string
}

// NewCustom is registered separately from timescaledb-breakingit in full and lean builds.
func NewCustom(params output.Params) (output.Output, error) {
	env := params.Environment
	u, err := url.Parse(params.ConfigArgument)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return nil, errors.New("Custom DB requires an HTTP(S) base URL without credentials, query or fragment")
	}
	runID, token := env["runId"], env["CUSTOM_STORAGE_TOKEN"]
	if !regexp.MustCompile(`^[A-Za-z0-9_-]+$`).MatchString(runID) || token == "" {
		return nil, errors.New("Custom DB requires runId and a run-scoped CUSTOM_STORAGE_TOKEN")
	}
	if strings.EqualFold(env["syntheticMonitoring"], "true") {
		return nil, errors.New("Custom DB k6 output does not support synthetic monitoring")
	}
	if strings.EqualFold(env["K6_BROWSER_SCREENSHOT_ON_ERROR"], "true") {
		return nil, errors.New("Custom DB k6 output does not yet support browser screenshot storage")
	}
	source, err := parseTransactionSource(env["BREAKINGIT_TRANSACTION_SOURCE"])
	if err != nil {
		return nil, err
	}
	patterns := map[string]string{}
	for _, pair := range strings.Split(env["CUSTOM_URL_PATTERNS"], ",") {
		if pair == "" {
			continue
		}
		parts := strings.Split(pair, "==")
		if len(parts) != 2 {
			return nil, errors.New("invalid CUSTOM_URL_PATTERNS entry")
		}
		patterns[parts[0]] = parts[1]
	}
	tags := map[string]string{"run_id": runID, "location": env["location"], "node_name": env["nodeName"], "scenario_name": env["scenarioName"]}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxConnsPerHost = 1
	c := &CustomOutput{
		base: strings.TrimRight(u.String(), "/") + "/runs/" + runID, token: token, runID: runID,
		client:       &http.Client{Transport: transport, Timeout: 10 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }},
		retryTimeout: 30 * time.Second, queue: make(chan customBatch, 4), done: make(chan struct{}),
		tickStop: make(chan struct{}), tickDone: make(chan struct{}), failed: make(chan struct{}), parts: map[string]*bytes.Buffer{},
	}
	c.groups = map[customGroupKey]*MetricGroup{}
	c.mapper = &Logger{out: params.StdOut, envTags: tags, customPatterns: patterns, transactionSource: source,
		bufHTTP: &bytes.Buffer{}, bufVUs: &bytes.Buffer{}, bufTrans: &bytes.Buffer{}, bufErrors: &bytes.Buffer{},
		pendingHttpGroups: map[int64]*MetricGroup{}, rowSink: c.addRow}
	return c, nil
}

func (c *CustomOutput) Description() string { return "custom-db-breakingit" }
func (c *CustomOutput) SetTestRunStopCallback(callback func(error)) {
	c.failureMu.Lock()
	c.stopCallback = callback
	c.failureMu.Unlock()
}
func (c *CustomOutput) err() error { c.failureMu.Lock(); defer c.failureMu.Unlock(); return c.failure }
func (c *CustomOutput) fail(err error) {
	if err == nil {
		return
	}
	c.failureMu.Lock()
	if c.failure != nil {
		c.failureMu.Unlock()
		return
	}
	c.failure = fmt.Errorf("CUSTOM_STORAGE_FAILED: final publication forbidden: %w", err)
	failure, callback := c.failure, c.stopCallback
	close(c.failed)
	c.failureMu.Unlock()
	// Never invoke the engine callback under a mapping/buffer lock.
	if callback != nil {
		go callback(failure)
	}
}
func (c *CustomOutput) Start() error {
	// An empty HELLO is sufficient for a fresh source. Do not claim the Java
	// monotonic sample-clock contract: k6 output callbacks cannot prove it.
	var hello struct {
		Incarnation string  `json:"incarnation"`
		Accepted    *uint64 `json:"accepted_sequence"`
		Sealed      bool    `json:"sealed"`
	}
	if err := c.request("/hello", []byte(`{}`), "application/json", 0, &hello); err != nil {
		return err
	}
	if hello.Sealed || hello.Incarnation == "" || hello.Accepted == nil || *hello.Accepted != 0 {
		return errors.New("Custom DB requires a fresh unfinished producer source")
	}
	c.incarnation = hello.Incarnation
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest), zstd.WithEncoderConcurrency(1))
	if err != nil {
		return err
	}
	c.encoder = encoder
	c.started = true
	go c.sender()
	go func() {
		defer close(c.tickDone)
		tick := time.NewTicker(200 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-c.tickStop:
				return
			case <-c.failed:
				return
			case <-tick.C:
				c.addMu.Lock()
				c.completeGroups(false)
				c.flush()
				c.addMu.Unlock()
			}
		}
	}()
	return nil
}

func (c *CustomOutput) AddMetricSamples(samples []metrics.SampleContainer) {
	c.addMu.Lock()
	defer c.addMu.Unlock()
	if c.err() != nil {
		return
	}
	for _, sc := range samples {
		for _, sample := range sc.GetSamples() {
			if sample.Metric.Name == "browser_errors" && sample.Tags.Map()["screenshot"] != "" {
				c.fail(errors.New("browser screenshots require a Custom DB payload integration"))
				return
			}
		}
		for _, sample := range sc.GetSamples() {
			key := httpMetricCanonicalKey(sample.Metric.Name)
			if key == "" {
				c.mapper.AddMetricSamples([]metrics.SampleContainer{sample})
				continue
			}
			id := customGroupKey{sample.Time.UnixNano(), sample.Tags}
			g := c.groups[id]
			if g == nil {
				g = &MetricGroup{Timestamp: id.time, Tags: map[string]string{}, Values: map[string]float64{}}
				copyTagsForHTTP(g.Tags, sample.Tags.Map(), c.mapper.envTags, c.mapper.customPatterns)
				c.groups[id] = g
			}
			if _, duplicate := g.Values[key]; duplicate {
				c.fail(errors.New("ambiguous HTTP metric identity: duplicate metric before request completion"))
				return
			}
			g.Values[key] = sample.Value
			if key == "http_req_failed" {
				g.ErrorReqHeaders = sample.HTTPErrorReqHeaders
				g.ErrorReqBody = sample.HTTPErrorReqBody
				g.ErrorResHeaders = sample.HTTPErrorResHeaders
				g.ErrorResBody = sample.HTTPErrorResBody
			}
		}
		c.completeGroups(false)

		if c.err() != nil {
			return
		}
		// Bound incomplete metric joins as well as outgoing payloads. Complete HTTP
		// containers usually leave this map empty; browser fragments may remain.
		n := 0
		for _, g := range c.groups {
			n += 256 + len(g.ErrorReqHeaders) + len(g.ErrorReqBody) + len(g.ErrorResHeaders) + len(g.ErrorResBody)
			for k, v := range g.Tags {
				n += len(k) + len(v) + 32
			}
			n += len(g.Values) * 64
		}
		if len(c.groups) > 4096 || n > customBatchBytes {
			c.fail(errors.New("incomplete metric join budget exceeded"))
			return
		}
	}
}

// Complete only after every request metric has arrived. Browser failure payloads
// may arrive much later than duration metrics; a timer is not proof of success.
func (c *CustomOutput) completeGroups(final bool) {
	for key, g := range c.groups {
		complete := true
		for _, name := range []string{"http_req_duration", "http_req_blocked", "http_req_connecting", "http_req_tls_handshaking", "http_req_sending", "http_req_waiting", "http_req_receiving", "http_req_failed"} {
			if _, ok := g.Values[name]; !ok {
				complete = false
				break
			}
		}
		if complete {
			delete(c.groups, key)
			c.mapper.processHttpGroup(g)
		} else if final {
			c.fail(errors.New("incomplete HTTP metric group at final drain"))
			return
		}
	}
}
func (c *CustomOutput) addRow(table string, row []string) error {
	if err := c.err(); err != nil {
		return err
	}
	data, err := encodeCustomRow(table, row)
	if err != nil {
		c.fail(err)
		return err
	}
	c.batchMu.Lock()
	defer c.batchMu.Unlock()
	if c.batchBytes+len(data) > customBatchBytes {
		if err = c.enqueueLocked(); err != nil {
			return err
		}
	}
	if c.parts[table] == nil {
		c.parts[table] = &bytes.Buffer{}
	}
	_, _ = c.parts[table].Write(data)
	c.batchBytes += len(data)
	return nil
}
func (c *CustomOutput) flush() {
	c.batchMu.Lock()
	defer c.batchMu.Unlock()
	if err := c.enqueueLocked(); err != nil {
		c.fail(err)
	}
}
func (c *CustomOutput) enqueueLocked() error {
	if c.batchBytes == 0 {
		return nil
	}
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	for _, table := range []string{"requests_raw", "transactions", "requests_error", "virtual_users"} {
		data := c.parts[table]
		if data == nil {
			continue
		}
		header := textproto.MIMEHeader{}
		header.Set("X-Table", "public."+table)
		header.Set("X-Format", "binary")
		part, err := writer.CreatePart(header)
		if err != nil {
			return err
		}
		_, _ = part.Write([]byte("PGCOPY\n\xff\r\n\x00\x00\x00\x00\x00\x00\x00\x00\x00"))
		_, _ = part.Write(data.Bytes())
		_, _ = part.Write([]byte{255, 255})
	}
	if err := writer.Close(); err != nil {
		return err
	}
	batch := customBatch{body.Bytes(), "multipart/mixed; boundary=" + writer.Boundary()}
	select {
	case <-c.failed:
		return c.err()
	case c.queue <- batch:
		c.parts = map[string]*bytes.Buffer{}
		c.batchBytes = 0
		return nil
	}
}
func (c *CustomOutput) sender() {
	defer close(c.done)
	for batch := range c.queue {
		if c.err() != nil {
			continue
		}
		compressed := c.encoder.EncodeAll(batch.body, nil)
		var ack struct {
			Accepted *uint64 `json:"accepted"`
		}
		next := c.sequence + 1
		if err := c.request("/ingest/batch", compressed, batch.contentType, next, &ack); err != nil {
			c.fail(err)
			continue
		}
		if ack.Accepted == nil || *ack.Accepted != next {
			c.fail(errors.New("invalid Custom DB batch acknowledgement"))
			continue
		}
		c.sequence = next
	}
}

func (c *CustomOutput) Stop() error {
	c.stopOnce.Do(func() {
		if !c.started {
			return
		}
		close(c.tickStop)
		<-c.tickDone
		c.addMu.Lock()
		if c.err() == nil {
			c.completeGroups(true)
			c.flush()
		}
		c.addMu.Unlock()
		close(c.queue)
		<-c.done
		if c.err() == nil {
			var ack struct {
				Accepted *uint64 `json:"accepted_sequence"`
				Finished bool    `json:"finished"`
			}
			err := c.request("/finish", nil, "application/octet-stream", c.sequence, &ack)
			if err == nil && (ack.Accepted == nil || *ack.Accepted != c.sequence || !ack.Finished) {
				err = errors.New("invalid Custom DB final acknowledgement")
			}
			c.fail(err)
		}
		c.encoder.Close()
		c.client.CloseIdleConnections()
		c.stopErr = c.err()
	})
	return c.stopErr
}

// Retries keep the same sequence, multipart boundary and compressed bytes.
// A changed incarnation is deliberately fatal until k6 has a proven recovery clock.
func (c *CustomOutput) request(path string, body []byte, contentType string, seq uint64, result any) error {
	ctx, cancel := context.WithTimeout(context.Background(), c.retryTimeout)
	defer cancel()
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+path, bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+c.token)
		req.Header.Set("Content-Type", contentType)
		req.Header.Set("X-Run-Id", c.runID)
		if path != "/hello" {
			req.Header.Set("X-Incarnation", c.incarnation)
			req.Header.Set("X-Sequence", fmt.Sprint(seq))
		}
		if path == "/ingest/batch" {
			req.Header.Set("Content-Encoding", "zstd")
		}
		response, err := c.client.Do(req)
		if err == nil {
			data, readErr := io.ReadAll(io.LimitReader(response.Body, 64*1024+1))
			_ = response.Body.Close()
			if len(data) > 64*1024 {
				return errors.New("oversized Custom DB acknowledgement")
			}
			if response.StatusCode == http.StatusOK && readErr == nil {
				if err = json.Unmarshal(data, result); err != nil {
					return errors.New("invalid Custom DB acknowledgement JSON")
				}
				return nil
			}
			if readErr == nil && response.StatusCode != 429 && response.StatusCode < 500 {
				return fmt.Errorf("Custom DB %s rejected request (HTTP %d)", path, response.StatusCode)
			}
		}
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("Custom DB %s retry deadline: %w", path, ctx.Err())
		case <-c.failed:
			timer.Stop()
			return c.err()
		case <-timer.C:
		}
	}
}
