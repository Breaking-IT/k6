package breakingit

import (
	"compress/gzip"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.k6.io/k6/v2/output"
)

// The second request is the follow-up tail of an already running flush.
// Stop must wait for it and for the third row appended while it is in flight.
func TestTimescaleStopJoinsFollowupFlush(t *testing.T) {
	first, second := make(chan struct{}), make(chan struct{})
	releaseFirst, releaseSecond := make(chan struct{}), make(chan struct{})
	var onceFirst, onceSecond sync.Once
	var mu sync.Mutex
	var received []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gz, err := gzip.NewReader(r.Body)
		if err != nil {
			t.Error(err)
			w.WriteHeader(400)
			return
		}
		defer gz.Close()
		_, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if err != nil {
			t.Error(err)
			w.WriteHeader(400)
			return
		}
		reader := multipart.NewReader(gz, params["boundary"])
		for {
			part, err := reader.NextPart()
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Error(err)
				return
			}
			body, _ := io.ReadAll(part)
			mu.Lock()
			received = append(received, strings.TrimSpace(string(body)))
			n := len(received)
			mu.Unlock()
			if n == 1 {
				close(first)
				<-releaseFirst
			}
			if n == 2 {
				close(second)
				<-releaseSecond
			}
		}
		w.WriteHeader(200)
	}))
	defer server.Close()
	defer onceFirst.Do(func() { close(releaseFirst) })
	defer onceSecond.Do(func() { close(releaseSecond) })
	out, err := New(output.Params{ConfigArgument: server.URL, StdOut: io.Discard, Environment: map[string]string{"PG_PROXY_TOKEN": "test-token"}})
	require.NoError(t, err)
	l := out.(*Logger)
	appendRow := func(row string) { l.mu.Lock(); l.bufHTTP.WriteString(row + "\n"); l.mu.Unlock() }
	await := func(ch <-chan struct{}) {
		select {
		case <-ch:
		case <-time.After(5 * time.Second):
			t.Fatal("flush did not reach handler")
		}
	}
	appendRow("first")
	go l.flush()
	await(first)
	appendRow("second")
	onceFirst.Do(func() { close(releaseFirst) })
	await(second)
	appendRow("third")
	stopped := make(chan error, 1)
	go func() { stopped <- l.Stop() }()
	select {
	case err := <-stopped:
		onceSecond.Do(func() { close(releaseSecond) })
		t.Fatalf("Stop returned before the in-flight tail was acknowledged: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	onceSecond.Do(func() { close(releaseSecond) })
	select {
	case err := <-stopped:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Stop failed to drain")
	}
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []string{"first", "second", "third"}, received)
}

func TestTimescaleStopDrainsUnexpiredMetricGroup(t *testing.T) {
	var mu sync.Mutex
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		mu.Lock()
		requests++
		mu.Unlock()
		w.WriteHeader(200)
	}))
	defer server.Close()
	out, err := New(output.Params{ConfigArgument: server.URL, StdOut: io.Discard, Environment: map[string]string{"PG_PROXY_TOKEN": "test-token"}})
	require.NoError(t, err)
	l := out.(*Logger)
	l.mu.Lock()
	now := time.Now()
	l.pendingHTTPGroups[now.UnixNano()] = &MetricGroup{Timestamp: now.UnixNano(), LastUpdate: now, Tags: map[string]string{"name": "tail-request"}, Values: map[string]float64{"http_req_duration": 12}}
	l.mu.Unlock()
	require.NoError(t, l.Stop())
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, 1, requests)
	require.Empty(t, l.pendingHTTPGroups)
}
