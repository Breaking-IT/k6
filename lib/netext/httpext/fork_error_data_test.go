package httpext

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"go.k6.io/k6/v2/metrics"
)

func TestHTTPErrorBodyRepresentations(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		body any
		want string
	}{
		{"string", "response", "response"},
		{"bytes", []byte("response"), "response"},
		{"empty string", "", ""},
		{"empty bytes", []byte{}, ""},
		{"nil", nil, ""},
		{"binary string", string([]byte{0xff}), "<binary>"},
		{"binary bytes", []byte{0xff}, "<binary>"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var sample metrics.Sample
			setHTTPErrorFields(&sample, nil, &Response{Body: tc.body})
			assert.Equal(t, tc.want, sample.HTTPErrorResBody)
		})
	}
}
