package k6ext

import (
	k6metrics "go.k6.io/k6/v2/metrics"
)

const (
	webVitalTTFB = "TTFB"
	webVitalLCP  = "LCP"
	webVitalCLS  = "CLS"
	webVitalINP  = "INP"
	webVitalFCP  = "FCP"

	ttfbName = "browser_web_vital_ttfb"
	lcpName  = "browser_web_vital_lcp"
	clsName  = "browser_web_vital_cls"
	inpName  = "browser_web_vital_inp"
	fcpName  = "browser_web_vital_fcp"

	browserDataSentName          = "browser_data_sent"
	browserDataReceivedName      = "browser_data_received"
	browserHTTPReqDurationName   = "browser_http_req_duration"
	browserHTTPReqBlockedName    = "browser_http_req_blocked"
	browserHTTPReqConnectingName = "browser_http_req_connecting"
	browserHTTPReqTLSHandshaking = "browser_http_req_tls_handshaking"
	browserHTTPReqSendingName    = "browser_http_req_sending"
	browserHTTPReqWaitingName    = "browser_http_req_waiting"
	browserHTTPReqReceivingName  = "browser_http_req_receiving"
	browserHTTPReqFailedName     = "browser_http_req_failed"
)

// CustomMetrics are the custom k6 metrics used by xk6-browser.
type CustomMetrics struct {
	WebVitals map[string]*k6metrics.Metric

	BrowserDataSent              *k6metrics.Metric
	BrowserDataReceived          *k6metrics.Metric
	BrowserHTTPReqDuration       *k6metrics.Metric
	BrowserHTTPReqBlocked        *k6metrics.Metric
	BrowserHTTPReqConnecting     *k6metrics.Metric
	BrowserHTTPReqTLSHandshaking *k6metrics.Metric
	BrowserHTTPReqSending        *k6metrics.Metric
	BrowserHTTPReqWaiting        *k6metrics.Metric
	BrowserHTTPReqReceiving      *k6metrics.Metric
	BrowserHTTPReqFailed         *k6metrics.Metric
}

// RegisterCustomMetrics creates and registers our custom metrics with the k6
// VU Registry and returns our internal struct pointer.
func RegisterCustomMetrics(registry *k6metrics.Registry) *CustomMetrics {
	wvs := map[string]string{
		webVitalTTFB: ttfbName, // time to first byte
		webVitalLCP:  lcpName,  // largest content paint
		webVitalCLS:  clsName,  // cumulative layout shift
		webVitalINP:  inpName,  // interaction to next paint
		webVitalFCP:  fcpName,  // first contentful paint
	}
	webVitals := make(map[string]*k6metrics.Metric)

	for k, v := range wvs {
		t := k6metrics.Time
		// CLS is not a time based measurement, it is a score,
		// so use the default metric type for CLS.
		if k == webVitalCLS {
			t = k6metrics.Default
		}

		webVitals[k] = registry.MustNewMetric(v, k6metrics.Trend, t)
	}

	return &CustomMetrics{
		WebVitals:                    webVitals,
		BrowserDataSent:              registry.MustNewMetric(browserDataSentName, k6metrics.Counter, k6metrics.Data),
		BrowserDataReceived:          registry.MustNewMetric(browserDataReceivedName, k6metrics.Counter, k6metrics.Data),
		BrowserHTTPReqDuration:       registry.MustNewMetric(browserHTTPReqDurationName, k6metrics.Trend, k6metrics.Time),
		BrowserHTTPReqBlocked:        registry.MustNewMetric(browserHTTPReqBlockedName, k6metrics.Trend, k6metrics.Time),
		BrowserHTTPReqConnecting:     registry.MustNewMetric(browserHTTPReqConnectingName, k6metrics.Trend, k6metrics.Time),
		BrowserHTTPReqTLSHandshaking: registry.MustNewMetric(browserHTTPReqTLSHandshaking, k6metrics.Trend, k6metrics.Time),
		BrowserHTTPReqSending:        registry.MustNewMetric(browserHTTPReqSendingName, k6metrics.Trend, k6metrics.Time),
		BrowserHTTPReqWaiting:        registry.MustNewMetric(browserHTTPReqWaitingName, k6metrics.Trend, k6metrics.Time),
		BrowserHTTPReqReceiving:      registry.MustNewMetric(browserHTTPReqReceivingName, k6metrics.Trend, k6metrics.Time),
		BrowserHTTPReqFailed:         registry.MustNewMetric(browserHTTPReqFailedName, k6metrics.Rate),
	}
}
