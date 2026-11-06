package breakingit

import (
	"fmt"
	"testing"
	"time"

	"github.com/gofrs/uuid/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFailedHTTPRows(t *testing.T) {
	t.Parallel()
	for _, synthetic := range []bool{false, true} {
		t.Run(fmt.Sprint(synthetic), func(t *testing.T) {
			t.Parallel()
			logger := newTransactionTestLogger(synthetic)
			at := time.Date(2026, 9, 24, 12, 0, 0, 123000000, time.UTC)
			logger.processHTTPGroup(&MetricGroup{
				Timestamp: at.UnixNano(),
				Tags: map[string]string{
					"group": "::Checkout", "path": "/pay", "name": "https://example.test/pay",
					"status": "500", "run_id": "run-123", "location": "Amsterdam",
					"node_name": "node-1", "scenario_name": "checkout", "scenario": "browser_checkout",
				},
				Values: map[string]float64{
					"http_req_failed": 1, "http_req_connecting": 2, "http_req_tls_handshaking": 3,
					"http_req_sending": 4, "http_req_waiting": 5, "http_req_duration": 20,
				},
				ErrorReqHeaders: "POST /pay", ErrorReqBody: "payload",
				ErrorResHeaders: "HTTP/1.1 500", ErrorResBody: "failure",
			})
			requests, err := readBinaryRows(logger.bufHTTP, map[bool]string{false: "tssssbiisiii", true: "tssssssbiisiiiu"}[logger.isSynthetic], false)
			require.NoError(t, err)
			require.Len(t, requests, 1)
			offset := 3
			if synthetic {
				offset = 5
			}
			assert.Equal(t, []string{"Checkout", "/pay", "false", "", "", "500", "5", "14", "25"},
				requests[0][offset:offset+9])
			errors, err := readBinaryRows(logger.bufErrors, map[bool]string{false: "tssssssiissssssu", true: "tsssssssiissssssuu"}[logger.isSynthetic], false)
			require.NoError(t, err)
			require.Len(t, errors, 1)
			offset = 4
			if synthetic {
				offset = 5
			}
			assert.Equal(t, []string{
				"Checkout", "/pay", "500", "25", "5", "https://example.test/pay", "",
				"HTTP 500", "POST /pay\npayload", "HTTP/1.1 500", "failure",
			}, errors[0][offset:offset+11])
			id, err := uuid.FromString(errors[0][offset+11])
			require.NoError(t, err)
			assert.EqualValues(t, 7, id.Version())
			stamp, err := uuid.TimestampFromV7(id)
			require.NoError(t, err)
			stampTime, err := stamp.Time()
			require.NoError(t, err)
			assert.Equal(t, at.UnixMilli(), stampTime.UnixMilli())
		})
	}
}
