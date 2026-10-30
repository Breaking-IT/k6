package common

import (
	"net/url"
	"testing"
	"time"

	"github.com/chromedp/cdproto/network"
	"github.com/stretchr/testify/require"
	"go.k6.io/k6/v2/internal/js/modules/k6/browser/k6ext"
	"go.k6.io/k6/v2/internal/js/modules/k6/browser/k6ext/k6test"
	"go.k6.io/k6/v2/internal/js/modules/k6/browser/log"
	k6metrics "go.k6.io/k6/v2/metrics"
)

func TestBrowserCompletionPinsHeaderDelayAndOutputQueue(t *testing.T) {
	samples := make(chan k6metrics.SampleContainer, 10)
	tracker, unbind := k6metrics.BindRequestTracker(samples)
	defer unbind()
	vu := k6test.NewVU(t, k6test.WithSamples(samples))
	vu.ActivateVU()
	nm := &NetworkManager{vu: vu, customMetrics: k6ext.RegisterCustomMetrics(k6metrics.NewRegistry()), eventInterceptor: &EventInterceptorMock{}, logger: log.NewNullLogger()}
	u, _ := url.Parse("https://example.test/error")
	req := &Request{url: u, wallTime: time.Now().Add(-time.Second), tagsAndMeta: vu.State().Tags.GetCurrentValues()}
	completion := nm.beginBrowserCompletion()
	// A captured terminal event waits through delayed headers/body extraction.
	require.NotNil(t, completion.ticket)
	time.Sleep(1100 * time.Millisecond)
	require.False(t, tracker.Before().After(completion.at))
	resp := &Response{url: u.String(), status: 503, remoteAddress: &RemoteAddress{}, wallTime: completion.at.Add(-500 * time.Millisecond), timing: &network.ResourceTiming{}, body: []byte("error detail"), request: req}
	nm.emitResponseMetrics(resp, req, completion)
	require.False(t, tracker.Before().After(completion.at)) // buffered output still pins it
	frame := (<-samples).(k6metrics.RequestSamples)
	require.Len(t, frame.GetSamples(), 9)
	for _, sample := range frame.GetSamples() {
		require.Equal(t, completion.at, sample.Time)
	}
	require.Equal(t, float64(1), frame.GetSamples()[8].Value)
	frame.Ticket.Release()
	require.True(t, tracker.Before().After(completion.at))
}

func TestBrowserCacheSkipReleasesCompletionPin(t *testing.T) {
	samples := make(chan k6metrics.SampleContainer, 10)
	tracker, unbind := k6metrics.BindRequestTracker(samples)
	defer unbind()
	vu := k6test.NewVU(t, k6test.WithSamples(samples))
	vu.ActivateVU()
	nm := &NetworkManager{vu: vu, customMetrics: k6ext.RegisterCustomMetrics(k6metrics.NewRegistry()), eventInterceptor: &EventInterceptorMock{}, logger: log.NewNullLogger()}
	u, _ := url.Parse("https://example.test/cached")
	req := &Request{url: u, wallTime: time.Now(), tagsAndMeta: vu.State().Tags.GetCurrentValues(), fromMemoryCache: true}
	completion := nm.beginBrowserCompletion()
	nm.emitResponseMetrics(&Response{url: u.String(), remoteAddress: &RemoteAddress{}, wallTime: time.Now(), request: req, body: []byte("cached")}, req, completion)
	require.Empty(t, samples)
	time.Sleep(1100 * time.Millisecond)
	require.True(t, tracker.Before().After(completion.at))
}
