package breakingit

import (
	"compress/gzip"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBatchRetryPreservesConcurrentSamples(t *testing.T) {
	t.Parallel()
	logger := newTransactionTestLogger(false)
	logger.bufTrans.WriteString("original\n")
	requests := make(chan string, 3)
	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gz, err := gzip.NewReader(r.Body)
		if !assert.NoError(t, err) {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		defer func() { assert.NoError(t, gz.Close()) }()
		_, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if !assert.NoError(t, err) {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		part, err := multipart.NewReader(gz, params["boundary"]).NextPart()
		if !assert.NoError(t, err) {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		assert.Equal(t, "public.transactions", part.Header.Get("X-Table"))
		data, err := io.ReadAll(part)
		assert.NoError(t, err)
		requests <- string(data)
		attempts++
		if attempts == 1 {
			logger.mu.Lock()
			logger.bufTrans.WriteString("arrived during retry\n")
			logger.mu.Unlock()
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	logger.proxyURL, logger.httpClient = server.URL, server.Client()
	logger.flush()
	for _, want := range []string{"original\n", "original\n", "arrived during retry\n"} {
		select {
		case got := <-requests:
			assert.Equal(t, want, got)
		case <-time.After(5 * time.Second):
			t.Fatal("missing batch")
		}
	}
	require.Eventually(t, func() bool {
		logger.mu.Lock()
		defer logger.mu.Unlock()
		return !logger.flushing && logger.bufTrans.Len() == 0
	}, 5*time.Second, 10*time.Millisecond)
}
