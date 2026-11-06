package common

import (
	"context"
	"fmt"
	"math"
	"net"
	"testing"
	"time"

	"go.k6.io/k6/v2/internal/features"
	"go.k6.io/k6/v2/internal/js/modules/k6/browser/k6ext"
	"go.k6.io/k6/v2/internal/js/modules/k6/browser/k6ext/k6test"
	"go.k6.io/k6/v2/internal/js/modules/k6/browser/log"

	k6mockresolver "go.k6.io/k6/v2/internal/lib/testutils/mockresolver"
	k6lib "go.k6.io/k6/v2/lib"
	k6types "go.k6.io/k6/v2/lib/types"
	k6metrics "go.k6.io/k6/v2/metrics"

	"github.com/chromedp/cdproto/cdp"
	"github.com/chromedp/cdproto/fetch"
	"github.com/chromedp/cdproto/network"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const mockHostname = "host.test"

type fakeSession struct {
	session
	cdpCalls []string
}

// Execute implements the cdp.Executor interface to record calls made to it and
// allow assertions in tests.
func (s *fakeSession) Execute(
	ctx context.Context, method string, params, res any,
) error {
	s.cdpCalls = append(s.cdpCalls, method)
	return nil
}

func newTestNetworkManager(t *testing.T, k6opts k6lib.Options) (*NetworkManager, *fakeSession) {
	t.Helper()

	session := &fakeSession{
		session: &Session{
			id: "1234",
		},
	}

	mr := k6mockresolver.New(map[string][]net.IP{
		mockHostname: {
			net.ParseIP("127.0.0.10"),
			net.ParseIP("127.0.0.11"),
			net.ParseIP("127.0.0.12"),
			net.ParseIP("2001:db8::10"),
			net.ParseIP("2001:db8::11"),
			net.ParseIP("2001:db8::12"),
		},
	})

	vu := k6test.NewVU(t)
	vu.ActivateVU()
	ctx := vu.Context()
	st := vu.State()
	st.Options = k6opts
	logger := log.New(st.Logger, "")
	timeoutSettings := NewTimeoutSettings(nil)
	frameManager := NewFrameManager(ctx, nil, nil, timeoutSettings, logger)

	nm := &NetworkManager{
		ctx:                           ctx,
		logger:                        logger,
		session:                       session,
		frameManager:                  frameManager,
		resolver:                      mr,
		vu:                            vu,
		reqIDToRequest:                map[network.RequestID]*Request{},
		reqIDToRequestWillBeSentEvent: make(map[network.RequestID]*network.EventRequestWillBeSent),
		reqIDToRequestPausedEvent:     make(map[network.RequestID]*fetch.EventRequestPaused),
	}

	return nm, session
}

func TestOnRequestPausedBlockedHostnames(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name, reqURL                  string
		blockedHostnames, expCDPCalls []string
	}{
		{
			name:             "ok_fail_simple",
			blockedHostnames: []string{"*.test"},
			reqURL:           fmt.Sprintf("http://%s/", mockHostname),
			expCDPCalls:      []string{"Fetch.failRequest"},
		},
		{
			name:             "ok_continue_simple",
			blockedHostnames: []string{"*.test"},
			reqURL:           "http://host.com/",
			expCDPCalls:      []string{"Fetch.continueRequest"},
		},
		{
			name:             "ok_continue_empty",
			blockedHostnames: nil,
			reqURL:           "http://host.com/",
			expCDPCalls:      []string{"Fetch.continueRequest"},
		},
		{
			name:             "ok_continue_ip",
			blockedHostnames: []string{"*.test"},
			reqURL:           "http://127.0.0.1:8000/",
			expCDPCalls:      []string{"Fetch.continueRequest"},
		},
		{
			name:             "err_url_continue",
			blockedHostnames: []string{"*.test"},
			reqURL:           ":::",
			expCDPCalls:      []string{"Fetch.continueRequest"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			blocked, err := k6types.NewNullHostnameTrie(tc.blockedHostnames)
			require.NoError(t, err)

			k6opts := k6lib.Options{BlockedHostnames: blocked}
			nm, session := newTestNetworkManager(t, k6opts)
			ev := &fetch.EventRequestPaused{
				RequestID: "1234",
				Request: &network.Request{
					Method: "GET",
					URL:    tc.reqURL,
				},
			}

			nm.onRequestPaused(ev)

			assert.Equal(t, tc.expCDPCalls, session.cdpCalls)
		})
	}
}

func TestOnRequestPausedBlockedIPs(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name, reqURL            string
		blockedIPs, expCDPCalls []string
	}{
		{
			name:        "ok_fail_simple",
			blockedIPs:  []string{"10.0.0.0/8", "192.168.0.0/16"},
			reqURL:      "http://10.0.0.1:8000/",
			expCDPCalls: []string{"Fetch.failRequest"},
		},
		{
			name:        "ok_fail_resolved_ip",
			blockedIPs:  []string{"127.0.0.10/32"},
			reqURL:      fmt.Sprintf("http://%s/", mockHostname),
			expCDPCalls: []string{"Fetch.failRequest"},
		},
		{
			name:        "ok_continue_resolved_ip",
			blockedIPs:  []string{"127.0.0.50/32"},
			reqURL:      fmt.Sprintf("http://%s/", mockHostname),
			expCDPCalls: []string{"Fetch.continueRequest"},
		},
		{
			name:        "ok_continue_simple",
			blockedIPs:  []string{"127.0.0.0/8"},
			reqURL:      "http://10.0.0.1:8000/",
			expCDPCalls: []string{"Fetch.continueRequest"},
		},
		{
			name:        "ok_continue_empty",
			blockedIPs:  nil,
			reqURL:      "http://127.0.0.1/",
			expCDPCalls: []string{"Fetch.continueRequest"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			blockedIPs := make([]*k6lib.IPNet, len(tc.blockedIPs))
			for i, ipcidr := range tc.blockedIPs {
				ipnet, err := k6lib.ParseCIDR(ipcidr)
				require.NoError(t, err)
				blockedIPs[i] = ipnet
			}

			k6opts := k6lib.Options{BlacklistIPs: blockedIPs}
			nm, session := newTestNetworkManager(t, k6opts)
			ev := &fetch.EventRequestPaused{
				RequestID: "1234",
				Request: &network.Request{
					Method: "GET",
					URL:    tc.reqURL,
				},
			}

			nm.onRequestPaused(ev)

			assert.Equal(t, tc.expCDPCalls, session.cdpCalls)
		})
	}
}

type EventInterceptorMock struct{}

func (m *EventInterceptorMock) urlTagName(_ string, _ string) (string, bool) {
	return "", false
}

func (m *EventInterceptorMock) onRequest(_ *Request) {}

func (m *EventInterceptorMock) onResponse(_ *Response) {}

func (m *EventInterceptorMock) onRequestFinished(_ *Request) {}

func (m *EventInterceptorMock) onRequestFailed(_ *Request) {}

func TestNetworkManagerEmitRequestResponseMetricsTimingSkew(t *testing.T) {
	t.Parallel()

	now := time.Now()
	type tm struct{ ts, wt time.Time }
	tests := []struct {
		name                       string
		req, res, wantReq, wantRes tm
	}{
		{
			name:    "ok",
			req:     tm{ts: now, wt: now},
			res:     tm{ts: now, wt: now},
			wantReq: tm{wt: now},
			wantRes: tm{wt: now},
		},
		{
			name:    "ok2",
			req:     tm{ts: now, wt: now},
			res:     tm{ts: now.Add(time.Minute)},
			wantReq: tm{wt: now},
			wantRes: tm{wt: now.Add(time.Minute)},
		},
		{
			name:    "ts_past",
			req:     tm{ts: now.Add(-time.Hour), wt: now},
			res:     tm{ts: now.Add(-time.Hour).Add(time.Minute)},
			wantReq: tm{wt: now},
			wantRes: tm{wt: now.Add(time.Minute)},
		},
		{
			name:    "ts_future",
			req:     tm{ts: now.Add(time.Hour), wt: now},
			res:     tm{ts: now.Add(time.Hour).Add(time.Minute)},
			wantReq: tm{wt: now},
			wantRes: tm{wt: now.Add(time.Minute)},
		},
		{
			name:    "wt_past",
			req:     tm{ts: now, wt: now.Add(-time.Hour)},
			res:     tm{ts: now.Add(time.Minute)},
			wantReq: tm{wt: now.Add(-time.Hour)},
			wantRes: tm{wt: now.Add(-time.Hour).Add(time.Minute)},
		},
		{
			name:    "wt_future",
			req:     tm{ts: now, wt: now.Add(time.Hour)},
			res:     tm{ts: now.Add(time.Minute)},
			wantReq: tm{wt: now.Add(time.Hour)},
			wantRes: tm{wt: now.Add(time.Hour).Add(time.Minute)},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			registry := k6metrics.NewRegistry()
			k6m := k6ext.RegisterCustomMetrics(registry)

			var (
				vu = k6test.NewVU(t)
				nm = &NetworkManager{ctx: vu.Context(), vu: vu, customMetrics: k6m, eventInterceptor: &EventInterceptorMock{}}
			)
			vu.ActivateVU()

			req, err := NewRequest(vu.Context(), log.NewNullLogger(), NewRequestParams{
				event: &network.EventRequestWillBeSent{
					Request:   &network.Request{},
					Timestamp: (*cdp.MonotonicTime)(&tt.req.ts),
					WallTime:  (*cdp.TimeSinceEpoch)(&tt.req.wt),
				},
			})
			require.NoError(t, err)
			nm.emitRequestMetrics(req)
			n := vu.AssertSamples(func(s k6metrics.Sample) {
				assert.Equalf(t, tt.wantReq.wt, s.Time, "timing skew in %s", s.Metric.Name)
			})
			assert.Equalf(t, 1, n, "should emit %d request metric", 1)
			res := NewHTTPResponse(vu.Context(), req,
				&network.Response{Timing: &network.ResourceTiming{}},
				(*cdp.MonotonicTime)(&tt.res.ts),
			)
			nm.emitResponseMetrics(res, req)
			n = vu.AssertSamples(func(s k6metrics.Sample) {
				assert.Equalf(t, tt.wantRes.wt, s.Time, "timing skew in %s", s.Metric.Name)
			})
			assert.Equalf(t, 9, n, "should emit 9 response metrics (8 timing+data + failed)")
		})
	}
}

func TestNetworkManagerMetricsRetainRequestTagsAndMetadata(t *testing.T) {
	t.Parallel()

	registry := k6metrics.NewRegistry()
	k6m := k6ext.RegisterCustomMetrics(registry)
	vu := k6test.NewVU(t)
	vu.ActivateVU()

	state := vu.State()
	state.Tags.Modify(func(tagsAndMeta *k6metrics.TagsAndMeta) {
		tagsAndMeta.SetTag(k6metrics.TagGroup.String(), "::request")
		tagsAndMeta.SetTag("phase", "request")
		tagsAndMeta.SetMetadata("trace", "request")
	})

	now := time.Now()
	req, err := NewRequest(vu.Context(), log.NewNullLogger(), NewRequestParams{
		event: &network.EventRequestWillBeSent{
			Request:   &network.Request{URL: "https://example.test/"},
			Timestamp: (*cdp.MonotonicTime)(&now),
			WallTime:  (*cdp.TimeSinceEpoch)(&now),
		},
		tagsAndMeta: state.Tags.GetCurrentValues(),
	})
	require.NoError(t, err)

	state.Tags.Modify(func(tagsAndMeta *k6metrics.TagsAndMeta) {
		tagsAndMeta.SetTag(k6metrics.TagGroup.String(), "")
		tagsAndMeta.SetTag("phase", "after")
		tagsAndMeta.SetMetadata("trace", "after")
	})

	nm := &NetworkManager{
		ctx:              vu.Context(),
		vu:               vu,
		customMetrics:    k6m,
		eventInterceptor: &EventInterceptorMock{},
	}
	nm.emitRequestMetrics(req)
	resp := NewHTTPResponse(vu.Context(), req, &network.Response{
		URL:    req.URL(),
		Timing: &network.ResourceTiming{},
	}, (*cdp.MonotonicTime)(&now))
	nm.emitResponseMetrics(resp, req)

	n := vu.AssertSamples(func(sample k6metrics.Sample) {
		group, ok := sample.Tags.Get(k6metrics.TagGroup.String())
		require.True(t, ok)
		assert.Equal(t, "::request", group)
		phase, ok := sample.Tags.Get("phase")
		require.True(t, ok)
		assert.Equal(t, "request", phase)
		assert.Equal(t, "request", sample.Metadata["trace"])
	})
	assert.Equal(t, 10, n)

	// The fork also emits samples when navigation fails before any response.
	// Those must use the request's context, not the VU's restored context.
	req.setErrorText("net::ERR_CONNECTION_REFUSED")
	nm.emitRequestFailedMetrics(req)
	n = vu.AssertSamples(func(sample k6metrics.Sample) {
		group, _ := sample.Tags.Get(k6metrics.TagGroup.String())
		assert.Equal(t, "::request", group)
		phase, _ := sample.Tags.Get("phase")
		assert.Equal(t, "request", phase)
		assert.Equal(t, "request", sample.Metadata["trace"])
		errorText, _ := sample.Tags.Get("error")
		assert.Equal(t, "net::ERR_CONNECTION_REFUSED", errorText)
	})
	assert.Equal(t, 9, n)
}

func TestNetworkManagerRedirectKeepsLoaderOperation(t *testing.T) {
	t.Parallel()

	nm, _ := newTestNetworkManager(t, k6lib.Options{})
	state := nm.vu.State()
	state.FeatureFlags = &features.Flags{AsyncMetricContext: true}
	page := &Page{}
	nm.frameManager.page = page

	state.Tags.Modify(func(tagsAndMeta *k6metrics.TagsAndMeta) {
		tagsAndMeta.SetTag("operation", "navigation")
	})
	navigationFinishAndRetain, _ := page.BeginNetworkOperation(state.Tags.GetCurrentValues())
	navigationTags, navigationOperation := nm.networkTagsAndMeta(state, &network.EventRequestWillBeSent{
		FrameID:  cdp.FrameID("frame"),
		LoaderID: cdp.LoaderID("initial-loader"),
		Type:     network.ResourceTypeDocument,
	}, nil)
	require.NotNil(t, navigationOperation)
	navigationFinishAndRetain()

	state.Tags.Modify(func(tagsAndMeta *k6metrics.TagsAndMeta) {
		tagsAndMeta.SetTag("operation", "unrelated")
	})
	unrelatedFinishAndRetain, _ := page.BeginNetworkOperation(state.Tags.GetCurrentValues())
	defer unrelatedFinishAndRetain()

	redirectTags, redirectOperation := nm.networkTagsAndMeta(state, &network.EventRequestWillBeSent{
		FrameID:  cdp.FrameID("frame"),
		LoaderID: cdp.LoaderID("redirect-loader"),
		Type:     network.ResourceTypeDocument,
	}, []*Request{{
		tagsAndMeta:      navigationTags,
		networkOperation: navigationOperation,
	}})
	assert.Same(t, navigationOperation, redirectOperation)
	operation, _ := redirectTags.Tags.Get("operation")
	assert.Equal(t, "navigation", operation)

	loaderTags, loaderOperation, ok := page.getNetworkTagsAndMetaForRequest(
		cdp.FrameID("frame"), cdp.LoaderID("redirect-loader"), true, false,
	)
	require.True(t, ok)
	assert.Same(t, navigationOperation, loaderOperation)
	operation, _ = loaderTags.Tags.Get("operation")
	assert.Equal(t, "navigation", operation)
}

func TestRequestLoaderContext(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		name      string
		typ       network.ResourceType
		initiator *network.Initiator
		prefer    bool
		bind      bool
	}{
		{name: "document", typ: network.ResourceTypeDocument, prefer: true, bind: true},
		{name: "parser", initiator: &network.Initiator{Type: network.InitiatorTypeParser}, prefer: true},
		{name: "preload", initiator: &network.Initiator{Type: network.InitiatorTypePreload}, prefer: true},
		{
			name:      "signed exchange",
			initiator: &network.Initiator{Type: network.InitiatorTypeSignedExchange},
			prefer:    true,
		},
		{name: "script", initiator: &network.Initiator{Type: network.InitiatorTypeScript}},
		{name: "preflight", initiator: &network.Initiator{Type: network.InitiatorTypePreflight}},
		{name: "unknown initiator"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			prefer, bind := requestLoaderContext(&network.EventRequestWillBeSent{
				Type:      testCase.typ,
				Initiator: testCase.initiator,
			})
			assert.Equal(t, testCase.prefer, prefer)
			assert.Equal(t, testCase.bind, bind)
		})
	}
}

func TestRequestForOnLoadingFinished(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		ID      network.RequestID
		request *Request
		parent  *Request
		want    *Request
	}{
		"non_nil_request": {
			ID: "1234",
			request: &Request{
				requestID:  "1234",
				documentID: "1234",
			},
			want: &Request{
				requestID:  "1234",
				documentID: "1234",
			},
		},
		"non_nil_request_with_parent": {
			ID: "1234",
			request: &Request{
				requestID:  "1234",
				documentID: "1234",
			},
			parent: &Request{
				documentID: "3421",
			},
			want: &Request{
				requestID:  "1234",
				documentID: "1234",
			},
		},
		"nil_request": {
			request: nil,
			want:    nil,
		},
		"nil_request_with_non_nil_parent_with_matching_document_id": {
			ID:      "1234",
			request: nil,
			parent: &Request{
				requestID:  "1234",
				documentID: "1234",
			},
			want: &Request{
				requestID:  "1234",
				documentID: "1234",
			},
		},
		"nil_request_with_non_nil_parent_with_non_matching_document_id": {
			ID:      "1234",
			request: nil,
			parent: &Request{
				requestID:  "1234",
				documentID: "4321",
			},
			want: nil,
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			nm, _ := newTestNetworkManager(t, k6lib.Options{})
			nm.parent, _ = newTestNetworkManager(t, k6lib.Options{})

			if tt.request != nil {
				tt.request.requestID = tt.ID
				nm.reqIDToRequest[tt.ID] = tt.request
			}
			if tt.parent != nil {
				nm.parent.reqIDToRequest[tt.parent.requestID] = tt.parent
			}

			r := nm.requestForOnLoadingFinished(tt.ID)
			if tt.want == nil {
				require.Nil(t, r)
				return
			}
			require.NotNil(t, r)
			assert.Equal(t, tt.want, r)
		})
	}
}

// TestNetworkManagerReceivingMetric verifies that the receiving metric is
// computed correctly using realistic CDP timing data. The primary approach
// uses MonotonicTimeEpoch + RequestTime to stay in ResourceTiming's time base.
func TestNetworkManagerReceivingMetric(t *testing.T) {
	t.Parallel()

	registry := k6metrics.NewRegistry()
	k6m := k6ext.RegisterCustomMetrics(registry)

	vu := k6test.NewVU(t)
	vu.ActivateVU()

	nm := &NetworkManager{
		ctx:              vu.Context(),
		vu:               vu,
		customMetrics:    k6m,
		logger:           log.NewNullLogger(),
		eventInterceptor: &EventInterceptorMock{},
	}

	// Simulate realistic timing:
	// MonotonicTimeEpoch is system boot time. RequestTime is monotonic seconds since boot.
	// ReceiveHeadersEnd is ms from RequestTime to when headers arrived.
	// responseEndWall is the MonotonicTime when LoadingFinished fired.
	bootTime := *cdp.MonotonicTimeEpoch

	// Request started 1000 seconds after boot
	requestTimeSec := 1000.0
	requestTimeGo := bootTime.Add(time.Duration(requestTimeSec * float64(time.Second)))

	// Headers arrived 200ms after RequestTime, content download took 150ms
	receiveHeadersEnd := 200.0
	responseEndWall := requestTimeGo.Add(350 * time.Millisecond) // 200ms + 150ms

	reqTimestamp := requestTimeGo
	reqWallTime := requestTimeGo // offset = 0 for simplicity

	req, err := NewRequest(vu.Context(), log.NewNullLogger(), NewRequestParams{
		event: &network.EventRequestWillBeSent{
			Request:   &network.Request{},
			Timestamp: (*cdp.MonotonicTime)(&reqTimestamp),
			WallTime:  (*cdp.TimeSinceEpoch)(&reqWallTime),
		},
	})
	require.NoError(t, err)
	req.responseEndWall = responseEndWall
	// Set headersEndWall too — the primary approach should still be used.
	req.headersEndWall = requestTimeGo.Add(200 * time.Millisecond)

	resp := NewHTTPResponse(vu.Context(), req,
		&network.Response{
			Timing: &network.ResourceTiming{
				RequestTime:       requestTimeSec,
				SendStart:         10,
				SendEnd:           20,
				ReceiveHeadersEnd: receiveHeadersEnd,
			},
		},
		(*cdp.MonotonicTime)(&reqTimestamp),
	)
	nm.emitResponseMetrics(resp, req)

	var receivingVal float64
	vu.AssertSamples(func(s k6metrics.Sample) {
		if s.Metric.Name == "browser_http_req_receiving" {
			receivingVal = s.Value
		}
	})

	// Expect receiving ≈ 150ms (350ms total - 200ms ReceiveHeadersEnd)
	assert.InDelta(t, 150.0, receivingVal, 1.0,
		"receiving should be ~150ms, got %.3f", receivingVal)
}

// TestNetworkManagerReceivingMetricNoHeadersEndWall verifies that receiving is
// correctly computed even when headersEndWall is zero (e.g. iframe cross-session
// requests where onResponseReceived can't find the request in its map).
func TestNetworkManagerReceivingMetricNoHeadersEndWall(t *testing.T) {
	t.Parallel()

	registry := k6metrics.NewRegistry()
	k6m := k6ext.RegisterCustomMetrics(registry)

	vu := k6test.NewVU(t)
	vu.ActivateVU()

	nm := &NetworkManager{
		ctx:              vu.Context(),
		vu:               vu,
		customMetrics:    k6m,
		logger:           log.NewNullLogger(),
		eventInterceptor: &EventInterceptorMock{},
	}

	bootTime := *cdp.MonotonicTimeEpoch
	requestTimeSec := 2000.0
	requestTimeGo := bootTime.Add(time.Duration(requestTimeSec * float64(time.Second)))

	receiveHeadersEnd := 100.0
	responseEndWall := requestTimeGo.Add(250 * time.Millisecond) // 100ms + 150ms receiving

	reqTimestamp := requestTimeGo
	reqWallTime := requestTimeGo

	req, err := NewRequest(vu.Context(), log.NewNullLogger(), NewRequestParams{
		event: &network.EventRequestWillBeSent{
			Request:   &network.Request{},
			Timestamp: (*cdp.MonotonicTime)(&reqTimestamp),
			WallTime:  (*cdp.TimeSinceEpoch)(&reqWallTime),
		},
	})
	require.NoError(t, err)
	req.responseEndWall = responseEndWall
	// headersEndWall intentionally left as zero — simulates cross-session iframe

	resp := NewHTTPResponse(vu.Context(), req,
		&network.Response{
			Timing: &network.ResourceTiming{
				RequestTime:       requestTimeSec,
				SendStart:         5,
				SendEnd:           10,
				ReceiveHeadersEnd: receiveHeadersEnd,
			},
		},
		(*cdp.MonotonicTime)(&reqTimestamp),
	)
	nm.emitResponseMetrics(resp, req)

	var receivingVal float64
	vu.AssertSamples(func(s k6metrics.Sample) {
		if s.Metric.Name == "browser_http_req_receiving" {
			receivingVal = s.Value
		}
	})

	// Expect receiving ≈ 150ms even without headersEndWall
	assert.InDelta(t, 150.0, receivingVal, 1.0,
		"receiving should be ~150ms even without headersEndWall, got %.3f", receivingVal)
}

// TestNetworkManagerReceivingMetricCachedResponse verifies that receiving falls
// back to the headersEndWall/responseEndWall approach when RequestTime is zero
// (as happens with cached responses).
func TestNetworkManagerReceivingMetricCachedResponse(t *testing.T) {
	t.Parallel()

	registry := k6metrics.NewRegistry()
	k6m := k6ext.RegisterCustomMetrics(registry)

	vu := k6test.NewVU(t)
	vu.ActivateVU()

	nm := &NetworkManager{
		ctx:              vu.Context(),
		vu:               vu,
		customMetrics:    k6m,
		logger:           log.NewNullLogger(),
		eventInterceptor: &EventInterceptorMock{},
	}

	now := time.Now()
	reqTimestamp := now
	reqWallTime := now

	req, err := NewRequest(vu.Context(), log.NewNullLogger(), NewRequestParams{
		event: &network.EventRequestWillBeSent{
			Request:   &network.Request{},
			Timestamp: (*cdp.MonotonicTime)(&reqTimestamp),
			WallTime:  (*cdp.TimeSinceEpoch)(&reqWallTime),
		},
	})
	require.NoError(t, err)

	// Simulate cached response: RequestTime=0, but headersEndWall/responseEndWall are set
	headersEnd := now.Add(50 * time.Millisecond)
	responseEnd := now.Add(120 * time.Millisecond)
	req.headersEndWall = headersEnd
	req.responseEndWall = responseEnd

	resp := NewHTTPResponse(vu.Context(), req,
		&network.Response{
			Timing: &network.ResourceTiming{
				RequestTime:       0, // cached — no real network request
				SendStart:         0,
				SendEnd:           0,
				ReceiveHeadersEnd: 0,
			},
		},
		(*cdp.MonotonicTime)(&reqTimestamp),
	)
	nm.emitResponseMetrics(resp, req)

	var receivingVal float64
	receivingFound := false
	vu.AssertSamples(func(s k6metrics.Sample) {
		if s.Metric.Name == "browser_http_req_receiving" {
			receivingVal = s.Value
			receivingFound = true
		}
	})

	require.True(t, receivingFound, "browser_http_req_receiving metric should be emitted")
	// Expect receiving ≈ 70ms (responseEnd - headersEnd)
	assert.False(t, math.IsNaN(receivingVal), "receiving should not be NaN")
	assert.InDelta(t, 70.0, receivingVal, 1.0,
		"receiving should fall back to wall-time approach for cached response, got %.3f", receivingVal)
}
