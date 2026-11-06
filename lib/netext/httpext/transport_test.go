package httpext

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.k6.io/k6/v2/lib"
	"go.k6.io/k6/v2/metrics"
	"gopkg.in/guregu/null.v3"
)

func BenchmarkMeasureAndEmitMetrics(b *testing.B) {
	ctx := b.Context()
	samples := make(chan metrics.SampleContainer, 10)
	defer close(samples)
	go func() { // discard all metrics
		for range samples { //nolint:revive
		}
	}()
	logger := logrus.New()
	logger.Level = logrus.DebugLevel

	registry := metrics.NewRegistry()
	state := &lib.State{
		Options: lib.Options{
			SystemTags: &metrics.DefaultSystemTagSet,
		},
		BuiltinMetrics: metrics.RegisterBuiltinMetrics(registry),
		Samples:        samples,
		Logger:         logger,
	}
	t := transport{
		state:       state,
		ctx:         ctx,
		tagsAndMeta: &metrics.TagsAndMeta{Tags: registry.RootTagSet()},
	}

	unfRequest := &unfinishedRequest{
		tracer: &Tracer{},
		response: &http.Response{
			StatusCode: http.StatusOK,
		},
		request: &http.Request{
			URL: &url.URL{
				Host:   "example.com",
				Scheme: "https",
			},
		},
	}

	b.Run("no responseCallback", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			t.measureAndEmitMetrics(unfRequest)
		}
	})

	t.responseCallback = func(_ int) bool { return true }

	b.Run("responseCallback", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			t.measureAndEmitMetrics(unfRequest)
		}
	})
}

// A manually set `name` tag replaces the `url` tag, and the URL then reaches an
// output nowhere else. HTTPRequestURL keeps it in metadata, which is not indexed
// in atlas and so does not reintroduce the cardinality the overwrite avoids.
func TestRequestURLMetadata(t *testing.T) {
	t.Parallel()

	const requestURL = "https://example.com/orders/12345"

	run := func(t *testing.T, nameTag string, enabled bool) *Trail {
		t.Helper()

		samples := make(chan metrics.SampleContainer, 10)
		go func() { // discard all metrics
			for range samples { //nolint:revive
			}
		}()
		t.Cleanup(func() { close(samples) })

		registry := metrics.NewRegistry()
		tags := registry.RootTagSet()
		if nameTag != "" {
			tags = tags.With(metrics.TagName.String(), nameTag)
		}
		parsed, err := url.Parse(requestURL)
		require.NoError(t, err)

		tr := transport{
			ctx: t.Context(),
			state: &lib.State{
				Options: lib.Options{
					SystemTags:     &metrics.DefaultSystemTagSet,
					HTTPRequestURL: null.BoolFrom(enabled),
				},
				BuiltinMetrics: metrics.RegisterBuiltinMetrics(registry),
				Samples:        samples,
				Logger:         logrus.New(),
			},
			tagsAndMeta: &metrics.TagsAndMeta{Tags: tags},
		}
		return tr.measureAndEmitMetrics(&unfinishedRequest{
			tracer:   &Tracer{},
			response: &http.Response{StatusCode: http.StatusOK},
			request:  &http.Request{URL: parsed},
		}).trail
	}

	t.Run("no name tag keeps the url tag, so metadata is redundant", func(t *testing.T) {
		t.Parallel()
		trail := run(t, "", true)
		urlTag, _ := trail.Tags.Get(metrics.TagURL.String())
		assert.Equal(t, requestURL, urlTag)
		assert.NotContains(t, trail.Metadata, MetadataRequestURL)
	})

	t.Run("disabled leaves the url unrecoverable", func(t *testing.T) {
		t.Parallel()
		trail := run(t, "my request", false)
		urlTag, _ := trail.Tags.Get(metrics.TagURL.String())
		assert.Equal(t, "my request", urlTag)
		assert.NotContains(t, trail.Metadata, MetadataRequestURL)
	})

	t.Run("enabled keeps the url in metadata", func(t *testing.T) {
		t.Parallel()
		trail := run(t, "my request", true)
		urlTag, _ := trail.Tags.Get(metrics.TagURL.String())
		assert.Equal(t, "my request", urlTag, "the name tag must still win the indexed tag")
		assert.Equal(t, requestURL, trail.Metadata[MetadataRequestURL])
	})
}
