package breakingit

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.k6.io/k6/v2/metrics"
	"go.k6.io/k6/v2/output"
)

func newTransactionTestLogger(synthetic bool) *Logger {
	return &Logger{
		out: io.Discard, isSynthetic: synthetic,
		bufHTTP: &bytes.Buffer{}, bufVUs: &bytes.Buffer{},
		bufTrans: &bytes.Buffer{}, bufErrors: &bytes.Buffer{},
		pendingHTTPGroups: make(map[int64]*MetricGroup),
		envTags: map[string]string{
			"run_id": "run-123", "location": "Amsterdam", "node_name": "node-1",
			"scenario_name": "checkout",
		},
	}
}

func transactionTestSample(name string, tags map[string]string, failed bool) metrics.Sample {
	registry := metrics.NewRegistry()
	return metrics.Sample{
		TimeSeries: metrics.TimeSeries{
			Metric: registry.MustNewMetric(name, metrics.Trend),
			Tags:   registry.RootTagSet().WithTagsFromMap(tags),
		},
		Time: time.Date(2026, 9, 24, 12, 0, 0, 123000000, time.UTC), Value: 123.987,
		GroupFailed: failed,
	}
}

func TestTransactionRows(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, metric, expectedName string
		tags                       map[string]string
		failed                     bool
	}{
		{"legacy page", "pages", "01_Home", map[string]string{"page": "01_Home", "group": "::wrapper"}, false},
		{"legacy URL fallback", "pages", "/home", map[string]string{"name": "https://example.test/home"}, false},
		{"legacy default", "pages", "/", nil, false},
		{"native success", "group_duration", "01_Home", map[string]string{"group": "::01_Home"}, false},
		{"native failure", "group_duration", "02_Pay", map[string]string{"group": "::02_Pay"}, true},
		{"nested", "group_duration", "Checkout::Payment", map[string]string{"group": "::Checkout::Payment"}, true},
		{"special characters", "group_duration", "A,\"B\"\nC", map[string]string{"group": "::A,\"B\"\nC"}, false},
		{"group fallback", "group_duration", "fallback", map[string]string{"name": "fallback"}, false},
		{"group default", "group_duration", "/", nil, false},
	}
	for _, synthetic := range []bool{false, true} {
		for _, test := range tests {
			t.Run(fmt.Sprintf("synthetic=%t/%s", synthetic, test.name), func(t *testing.T) {
				t.Parallel()
				l := newTransactionTestLogger(synthetic)
				sample := transactionTestSample(test.metric, test.tags, test.failed)
				sample.Tags = sample.Tags.With("scenario", "browser_checkout")
				l.AddMetricSamples([]metrics.SampleContainer{sample})
				rows, err := readBinaryRows(l.bufTrans, map[bool]string{false: "tsssbiii", true: "tsssssbiiiu"}[l.isSynthetic], false)
				require.NoError(t, err)
				expected := []string{
					"2026-09-24T12:00:00.123Z", "run-123", "Amsterdam", test.expectedName,
					fmt.Sprint(!test.failed), "", "", "123",
				}
				if synthetic {
					expected = []string{
						"2026-09-24T12:00:00.123Z", "checkout", "Amsterdam", "node-1",
						"browser_checkout", test.expectedName, fmt.Sprint(!test.failed),
						"", "", "123", syntheticRunUUID("run-123").String(),
					}
				}
				assert.Equal(t, [][]string{expected}, rows)
			})
		}
	}
}

func TestTransactionSource(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		value string
		want  []string
	}{
		{"", []string{"legacy", "native"}},
		{"both", []string{"legacy", "native"}},
		{"pages", []string{"legacy"}},
		{"groups", []string{"native"}},
	} {
		t.Run(test.value, func(t *testing.T) {
			t.Parallel()
			l := newTransactionTestLogger(false)
			var err error
			l.transactionSource, err = parseTransactionSource(test.value)
			require.NoError(t, err)
			l.AddMetricSamples([]metrics.SampleContainer{metrics.Samples{
				transactionTestSample("pages", map[string]string{"page": "legacy"}, false),
				transactionTestSample("group_duration", map[string]string{"group": "::native"}, false),
			}})
			rows, err := readBinaryRows(l.bufTrans, map[bool]string{false: "tsssbiii", true: "tsssssbiiiu"}[l.isSynthetic], false)
			require.NoError(t, err)
			var names []string
			for _, row := range rows {
				names = append(names, row[3])
			}
			assert.Equal(t, test.want, names)
			// Filtering transaction durations must not drop failed checks.
			check := transactionTestSample("checks", map[string]string{"group": "::native", "check": "bad"}, false)
			check.Value = 0
			l.AddMetricSamples([]metrics.SampleContainer{check})
			errors, err := readBinaryRows(l.bufErrors, map[bool]string{false: "tssssssiissssssu", true: "tsssssssiissssssuu"}[l.isSynthetic], false)
			require.NoError(t, err)
			require.Len(t, errors, 1)
			assert.Equal(t, "native", errors[0][4])
		})
	}
}

func TestTransactionSourceConfiguration(t *testing.T) {
	t.Parallel()
	for _, value := range []string{"", "both", "groups", "pages", "invalid"} {
		exporter, err := New(output.Params{
			ConfigArgument: "http://127.0.0.1", StdOut: io.Discard,
			Environment: map[string]string{"BREAKINGIT_TRANSACTION_SOURCE": value, "PG_PROXY_TOKEN": "test-token"},
		})
		if value == "invalid" {
			require.ErrorContains(t, err, "BREAKINGIT_TRANSACTION_SOURCE")
			require.Nil(t, exporter)
			continue
		}
		require.NoError(t, err)
		t.Cleanup(func() { assert.NoError(t, exporter.Stop()) })
		want, err := parseTransactionSource(value)
		require.NoError(t, err)
		assert.Equal(t, want, exporter.(*Logger).transactionSource, "source=%q", value)
	}
}

func TestBrowserRequestKeepsExplicitGroup(t *testing.T) {
	t.Parallel()
	for _, synthetic := range []bool{false, true} {
		for _, test := range []struct{ group, path, want string }{
			{"::Checkout::Pay", "/pay", "Checkout::Pay"},
			{"::/home", "/home", "/home"},
			{"::/", "/", "/"},
			{"/home", "/home", ""},
			{"/", "/", ""},
		} {
			l := newTransactionTestLogger(synthetic)
			l.processHTTPGroup(&MetricGroup{
				Tags: map[string]string{"group": test.group, "path": test.path, "resource_type": "Document"},
			})
			rows, err := readBinaryRows(l.bufHTTP, map[bool]string{false: "tssssbiisiii", true: "tssssssbiisiiiu"}[l.isSynthetic], false)
			require.NoError(t, err)
			require.Len(t, rows, 1)
			index := 3
			if synthetic {
				index = 5
			}
			assert.Equal(t, test.want, rows[0][index], "group=%q synthetic=%t", test.group, synthetic)
		}
	}
}

func TestTransactionsSentToProxy(t *testing.T) {
	t.Parallel()
	for _, synthetic := range []bool{false, true} {
		t.Run(fmt.Sprint(synthetic), func(t *testing.T) {
			t.Parallel()
			type batch struct {
				table string
				rows  [][]string
			}
			batches := make(chan batch, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, "/ingest/batch", r.URL.Path)
				assert.Equal(t, "Bearer test-token", r.Header.Get("Authorization"))
				assert.Equal(t, "gzip", r.Header.Get("Content-Encoding"))
				gz, err := gzip.NewReader(r.Body)
				if !assert.NoError(t, err) {
					w.WriteHeader(http.StatusOK)
					return
				}
				defer func() { assert.NoError(t, gz.Close()) }()
				_, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
				assert.NoError(t, err)
				reader := multipart.NewReader(gz, params["boundary"])
				part, err := reader.NextPart()
				if !assert.NoError(t, err) {
					w.WriteHeader(http.StatusOK)
					return
				}
				assert.Equal(t, "binary", part.Header.Get("X-Format"))
				rows, err := readBinaryRows(part, map[bool]string{false: "tsssbiii", true: "tsssssbiiiu"}[synthetic], true)
				assert.NoError(t, err)
				batches <- batch{part.Header.Get("X-Table"), rows}
				_, err = reader.NextPart()
				assert.ErrorIs(t, err, io.EOF)
				w.WriteHeader(http.StatusOK)
			}))
			defer server.Close()
			l := newTransactionTestLogger(synthetic)
			l.proxyURL, l.token, l.httpClient = server.URL, "test-token", server.Client()
			l.AddMetricSamples([]metrics.SampleContainer{
				transactionTestSample("pages", map[string]string{"page": "legacy"}, false),
				transactionTestSample("group_duration", map[string]string{"group": "::native"}, true),
			})
			l.flush()
			select {
			case got := <-batches:
				wantTable, index := "public.transactions", 3
				if synthetic {
					wantTable, index = "public.transactions_sm", 5
				}
				assert.Equal(t, wantTable, got.table)
				require.Len(t, got.rows, 2)
				assert.Equal(t, []string{"legacy", "true"}, got.rows[0][index:index+2])
				assert.Equal(t, []string{"native", "false"}, got.rows[1][index:index+2])
			default:
				t.Fatal("no transaction batch received")
			}
			assert.Zero(t, l.bufTrans.Len())
		})
	}
}
