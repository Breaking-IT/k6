package breakingit

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.k6.io/k6/v2/metrics"
	"go.k6.io/k6/v2/output"
)

func TestFirstBatchImmediateThenOneSecond(t *testing.T) {
	t.Parallel()
	for _, synthetic := range []bool{false, true} {
		t.Run(fmt.Sprint(synthetic), func(t *testing.T) {
			t.Parallel()
			sent := make(chan time.Time, 10)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				sent <- time.Now()
				w.WriteHeader(http.StatusOK)
			}))
			defer server.Close()
			exporter, err := New(output.Params{ConfigArgument: server.URL, StdOut: io.Discard, Environment: map[string]string{"PG_PROXY_TOKEN": "test", "isSynthetic": fmt.Sprint(synthetic)}})
			require.NoError(t, err)
			stopped := false
			defer func() {
				if !stopped {
					require.NoError(t, exporter.Stop())
				}
			}()
			// Starting the output must not send an empty heartbeat.
			select {
			case <-sent:
				t.Fatal("empty batch")
			case <-time.After(50 * time.Millisecond):
			}
			sample := transactionTestSample("group_duration", map[string]string{"group": "::hello"}, false)
			exporter.AddMetricSamples([]metrics.SampleContainer{sample})
			var first time.Time
			select {
			case first = <-sent:
			case <-time.After(500 * time.Millisecond):
				t.Fatal("first data was delayed")
			}
			exporter.AddMetricSamples([]metrics.SampleContainer{sample})
			select {
			case at := <-sent:
				require.GreaterOrEqual(t, at.Sub(first), 900*time.Millisecond)
			case <-time.After(2 * time.Second):
				t.Fatal("second batch was delayed")
			}
			// Shutdown must flush rows without waiting for another tick.
			exporter.AddMetricSamples([]metrics.SampleContainer{sample})
			require.NoError(t, exporter.Stop())
			stopped = true
			require.Len(t, sent, 1)
		})
	}
}

func TestStopDrainsIncompleteHTTPGroups(t *testing.T) {
	t.Parallel()
	bodies := make(chan []byte, 4)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		bodies <- body
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	exporter, err := New(output.Params{ConfigArgument: server.URL, StdOut: io.Discard, Environment: map[string]string{"PG_PROXY_TOKEN": "test"}})
	require.NoError(t, err)
	exporter.AddMetricSamples([]metrics.SampleContainer{transactionTestSample("http_req_duration", nil, false)})
	require.NoError(t, exporter.Stop())
	require.Len(t, bodies, 1)
}
