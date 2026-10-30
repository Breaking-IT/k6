package breakingit

import (
	"bytes"
	"encoding/binary"
	"encoding/csv"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
	"go.k6.io/k6/v2/metrics"
	"go.k6.io/k6/v2/output"
)

func newCustomTest(t *testing.T, endpoint string) *CustomOutput {
	t.Helper()
	out, err := NewCustom(output.Params{ConfigArgument: endpoint, StdOut: io.Discard, Environment: map[string]string{
		"runId": "run-123", "CUSTOM_STORAGE_TOKEN": "producer-token", "location": "Amsterdam", "nodeName": "node-1",
	}})
	require.NoError(t, err)
	return out.(*CustomOutput)
}
func decodeCustomBatch(t *testing.T, data []byte, content string) map[string][][][]byte {
	t.Helper()
	decoder, err := zstd.NewReader(nil)
	require.NoError(t, err)
	defer decoder.Close()
	plain, err := decoder.DecodeAll(data, nil)
	require.NoError(t, err)
	_, params, err := mime.ParseMediaType(content)
	require.NoError(t, err)
	reader := multipart.NewReader(bytes.NewReader(plain), params["boundary"])
	result := map[string][][][]byte{}
	for {
		part, err := reader.NextPart()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		require.Equal(t, "binary", part.Header.Get("X-Format"))
		data, err := io.ReadAll(part)
		require.NoError(t, err)
		require.Equal(t, []byte("PGCOPY\n\xff\r\n\x00"), data[:11])
		require.Equal(t, make([]byte, 8), data[11:19])
		r := bytes.NewReader(data[19:])
		table := strings.TrimPrefix(part.Header.Get("X-Table"), "public.")
		for {
			var n int16
			require.NoError(t, binary.Read(r, binary.BigEndian, &n))
			if n == -1 {
				break
			}
			row := make([][]byte, n)
			for i := range row {
				var length int32
				require.NoError(t, binary.Read(r, binary.BigEndian, &length))
				if length == -1 {
					continue
				}
				require.GreaterOrEqual(t, length, int32(0))
				row[i] = make([]byte, length)
				_, err = io.ReadFull(r, row[i])
				require.NoError(t, err)
			}
			result[table] = append(result[table], row)
		}
		require.Zero(t, r.Len())
	}
	return result
}
func TestCustomBinaryMatchesCSVMapping(t *testing.T) {
	t.Parallel()
	rows := []struct {
		table string
		row   []string
	}{
		{"requests_raw", []string{"2026-10-30T12:00:00.123456Z", "run-123", "NL", "Checkout", "/pay", "false", "", "123", "503", "-1", "25", "100"}},
		{"transactions", []string{"2026-10-30T12:00:00Z", "run-123", "NL", "Checkout", "true", "", "", "120"}},
		{"requests_error", []string{"2026-10-30T12:00:00Z", "run-123", "NL", "node", "Checkout", "/pay", "503", "100", "", "https://example.test/", "", "bad\nreply", "POST,\"a\"", "Header: value", "雪", "019a3524-3890-7000-8000-000000000001"}},
		{"virtual_users", []string{"2026-10-30T12:00:00Z", "run-123", "NL", "node", "10", "", ""}},
	}
	c := newCustomTest(t, "http://localhost")
	for _, r := range rows {
		require.NoError(t, c.addRow(r.table, r.row))
	}
	c.flush()
	batch := <-c.queue
	encoder, err := zstd.NewWriter(nil)
	require.NoError(t, err)
	defer encoder.Close()
	decoded := decodeCustomBatch(t, encoder.EncodeAll(batch.body, nil), batch.contentType)
	require.Len(t, decoded, 4)
	require.Nil(t, decoded["requests_raw"][0][9])
	require.Nil(t, decoded["requests_raw"][0][6])
	require.Equal(t, uint32(123), binary.BigEndian.Uint32(decoded["requests_raw"][0][7]))
	require.Equal(t, []byte("雪"), decoded["requests_error"][0][14])
	require.Len(t, decoded["requests_error"][0][15], 16)
	require.Equal(t, []byte{1}, decoded["transactions"][0][4])
	require.Equal(t, int64(846676800123456), int64(binary.BigEndian.Uint64(decoded["requests_raw"][0][0])))
	// Transaction mapping itself is shared and byte-for-byte equivalent before encoding.
	legacy := newTransactionTestLogger(false)
	sample := transactionTestSample("group_duration", map[string]string{"group": "::Checkout"}, true)
	legacy.AddMetricSamples([]metrics.SampleContainer{sample})
	expected, err := csv.NewReader(legacy.bufTrans).Read()
	require.NoError(t, err)
	var actual []string
	c.mapper.rowSink = func(table string, row []string) error {
		require.Equal(t, "transactions", table)
		actual = row
		return nil
	}
	c.AddMetricSamples([]metrics.SampleContainer{sample})
	require.Equal(t, expected, actual)
}

func TestCustomRetriesExactBytesAndFinalDrain(t *testing.T) {
	t.Parallel()
	var first []byte
	var content string
	var calls, finishes int
	var mu sync.Mutex
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		require.Equal(t, "Bearer producer-token", r.Header.Get("Authorization"))
		switch r.URL.Path {
		case "/runs/run-123/hello":
			fmt.Fprint(w, `{"incarnation":"one","accepted_sequence":0,"sealed":false}`)
		case "/runs/run-123/ingest/batch":
			require.Equal(t, "one", r.Header.Get("X-Incarnation"))
			require.Equal(t, "1", r.Header.Get("X-Sequence"))
			require.Equal(t, "zstd", r.Header.Get("Content-Encoding"))
			data, err := io.ReadAll(r.Body)
			require.NoError(t, err)
			calls++
			if calls == 1 {
				first = data
				content = r.Header.Get("Content-Type")
				w.WriteHeader(503)
				return
			}
			require.Equal(t, first, data)
			require.Equal(t, content, r.Header.Get("Content-Type"))
			fmt.Fprint(w, `{"accepted":1,"duplicate":true}`)
		case "/runs/run-123/finish":
			require.Equal(t, 2, calls)
			require.Equal(t, "1", r.Header.Get("X-Sequence"))
			finishes++
			fmt.Fprint(w, `{"accepted_sequence":1,"finished":true}`)
		default:
			t.Errorf("unexpected route %s", r.URL.Path)
			w.WriteHeader(404)
		}
	}))
	defer server.Close()
	c := newCustomTest(t, server.URL)
	require.NoError(t, c.Start())
	c.AddMetricSamples([]metrics.SampleContainer{transactionTestSample("group_duration", nil, false)})
	require.NoError(t, c.Stop())
	require.NoError(t, c.Stop())
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, 1, finishes)
	decoded := decodeCustomBatch(t, first, content)
	require.Len(t, decoded["transactions"], 1)
}

func TestCustomFailureForbidsFinish(t *testing.T) {
	for _, status := range []int{400, 401, 403, 409, 413, 429, 503, 200} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			var finished bool
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/hello") {
					fmt.Fprint(w, `{"incarnation":"one","accepted_sequence":0}`)
					return
				}
				if strings.HasSuffix(r.URL.Path, "/finish") {
					finished = true
				}
				w.WriteHeader(status)
				fmt.Fprint(w, `{"accepted":999}`)
			}))
			defer server.Close()
			c := newCustomTest(t, server.URL)
			c.retryTimeout = 50 * time.Millisecond
			stopped := make(chan error, 1)
			c.SetTestRunStopCallback(func(err error) { stopped <- err })
			require.NoError(t, c.Start())
			c.AddMetricSamples([]metrics.SampleContainer{transactionTestSample("pages", nil, false)})
			require.ErrorContains(t, c.Stop(), "final publication forbidden")
			select {
			case err := <-stopped:
				require.Error(t, err)
			case <-time.After(time.Second):
				t.Fatal("run not stopped")
			}
			require.False(t, finished)
		})
	}
}

func TestCustomConfigurationAndBounds(t *testing.T) {
	t.Parallel()
	for _, entry := range []struct{ k, v string }{{"syntheticMonitoring", "true"}, {"K6_BROWSER_SCREENSHOT_ON_ERROR", "true"}, {"runId", "../wrong"}, {"CUSTOM_STORAGE_TOKEN", ""}, {"BREAKINGIT_TRANSACTION_SOURCE", "wrong"}} {
		env := map[string]string{"runId": "test", "CUSTOM_STORAGE_TOKEN": "token"}
		env[entry.k] = entry.v
		_, err := NewCustom(output.Params{ConfigArgument: "http://localhost", Environment: env})
		require.Error(t, err)
	}
	_, err := NewCustom(output.Params{ConfigArgument: "http://user:password@localhost"})
	require.Error(t, err)
	c := newCustomTest(t, "http://localhost")
	row := []string{"2026-10-30T00:00:00Z", "run-123", "NL", strings.Repeat("x", customBatchBytes), "true", "", "", "1"}
	require.Error(t, c.addRow("transactions", row))
	require.Error(t, c.err())
	require.Empty(t, c.queue)
}

func TestCustomHTTPJoinsAndLateFailures(t *testing.T) {
	t.Parallel()
	c := newCustomTest(t, "http://localhost")
	var rows [][]string
	c.mapper.rowSink = func(table string, row []string) error {
		if table == "requests_raw" {
			rows = append(rows, row)
		}
		return nil
	}
	reg := metrics.NewRegistry()
	ts := time.Now()
	names := []string{"browser_http_req_duration", "browser_http_req_blocked", "browser_http_req_connecting", "browser_http_req_tls_handshaking", "browser_http_req_sending", "browser_http_req_waiting", "browser_http_req_receiving", "browser_data_received"}
	tagsA := reg.RootTagSet().With("name", "/a").With("resource_type", "fetch")
	tagsB := reg.RootTagSet().With("name", "/b").With("resource_type", "fetch")
	for _, tags := range []*metrics.TagSet{tagsA, tagsB} {
		samples := metrics.Samples{}
		for _, name := range names {
			samples = append(samples, metrics.Sample{TimeSeries: metrics.TimeSeries{Metric: reg.MustNewMetric(name, metrics.Trend), Tags: tags}, Time: ts, Value: 12})
		}
		c.AddMetricSamples([]metrics.SampleContainer{samples})
	}
	require.Empty(t, rows)
	c.completeGroups(false)
	require.Empty(t, rows) // A flush timer cannot infer request success.
	for _, tags := range []*metrics.TagSet{tagsA, tagsB} {
		sample := metrics.Sample{TimeSeries: metrics.TimeSeries{Metric: reg.MustNewMetric("browser_http_req_failed", metrics.Rate), Tags: tags}, Time: ts, Value: 1, HTTPErrorResBody: "failure"}
		c.AddMetricSamples([]metrics.SampleContainer{sample})
	}
	require.Len(t, rows, 2)
	require.Empty(t, c.groups)
	for _, row := range rows {
		require.Equal(t, "false", row[5])
		require.Equal(t, "12", row[7])
	}
}

func TestCustomIncompleteDrainFails(t *testing.T) {
	t.Parallel()
	c := newCustomTest(t, "http://localhost")
	c.AddMetricSamples([]metrics.SampleContainer{transactionTestSample("http_req_duration", nil, false)})
	c.completeGroups(true)
	require.ErrorContains(t, c.err(), "incomplete HTTP metric group")
}

func TestCustomQueueBackpressure(t *testing.T) {
	t.Parallel()
	c := newCustomTest(t, "http://localhost")
	row := []string{"2026-10-30T12:00:00Z", "run-123", "NL", "Checkout", "true", "", "", "1"}
	for range cap(c.queue) {
		require.NoError(t, c.addRow("transactions", row))
		c.flush()
	}
	blocked := make(chan struct{})
	go func() { defer close(blocked); _ = c.addRow("transactions", row); c.flush() }()
	select {
	case <-blocked:
		t.Fatal("full queue did not apply backpressure")
	case <-time.After(20 * time.Millisecond):
	}
	c.fail(fmt.Errorf("sender unavailable"))
	select {
	case <-blocked:
	case <-time.After(time.Second):
		t.Fatal("failure did not unblock producer")
	}
	require.Len(t, c.queue, cap(c.queue))
}

func TestCustomStartupRejectsNonFreshAndMissingAck(t *testing.T) {
	t.Parallel()
	for _, body := range []string{`{}`, `{"incarnation":"a"}`, `{"incarnation":"a","accepted_sequence":1}`, `{"incarnation":"a","accepted_sequence":0,"sealed":true}`} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, body) }))
		c := newCustomTest(t, server.URL)
		require.Error(t, c.Start())
		require.False(t, c.started)
		server.Close()
	}
}

func TestCustomHTTPDuplicateIdentityFails(t *testing.T) {
	t.Parallel()
	c := newCustomTest(t, "http://localhost")
	sample := transactionTestSample("http_req_duration", nil, false)
	c.AddMetricSamples([]metrics.SampleContainer{sample, sample})
	require.ErrorContains(t, c.err(), "ambiguous HTTP metric identity")
}
