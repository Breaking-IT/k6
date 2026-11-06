package common

import (
	"testing"
	"time"

	"github.com/chromedp/cdproto/network"
	"github.com/stretchr/testify/assert"

	"go.k6.io/k6/v2/internal/js/modules/k6/browser/log"
)

func TestForkResponseTimings(t *testing.T) {
	t.Parallel()
	now := time.Now()
	req := &Request{
		wallTime:        now.Add(-time.Second),
		headersEndWall:  now.Add(-100 * time.Millisecond),
		responseEndWall: now,
	}
	for _, tc := range []struct {
		name     string
		response *Response
		want     browserResponseTimings
	}{
		{
			name: "TLS phases exclude connection establishment from duration",
			response: &Response{timing: &network.ResourceTiming{
				ConnectStart: 10, SslStart: 30, ConnectEnd: 70,
				SendStart: 80, SendEnd: 90, ReceiveHeadersEnd: 140,
			}},
			want: browserResponseTimings{
				blocked: 80, connecting: 20, tlsHandshaking: 40,
				sending: 10, waiting: 50, receiving: 100, durationMs: 160,
			},
		},
		{
			name: "plain HTTP",
			response: &Response{timing: &network.ResourceTiming{
				ConnectStart: 10, SslStart: -1, ConnectEnd: 30,
				SendStart: 50, SendEnd: 60, ReceiveHeadersEnd: 80,
			}},
			want: browserResponseTimings{
				blocked: 50, connecting: 20,
				sending: 10, waiting: 20, receiving: 100, durationMs: 130,
			},
		},
		{
			name: "negative phases are clamped",
			response: &Response{timing: &network.ResourceTiming{
				ConnectStart: 10, SslStart: -1, ConnectEnd: 5,
				SendStart: -2, SendEnd: -3, ReceiveHeadersEnd: -4,
			}},
			want: browserResponseTimings{receiving: 100, durationMs: 100},
		},
		{name: "no response", want: browserResponseTimings{durationMs: 1000}},
		{
			name: "no timing", response: &Response{},
			want: browserResponseTimings{durationMs: 1000},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			nm := &NetworkManager{logger: log.NewNullLogger()}
			assert.Equal(t, tc.want, nm.responseTimings(req, tc.response, now))
		})
	}
}
