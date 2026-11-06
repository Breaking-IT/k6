package breakingit

import (
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.k6.io/k6/v2/output"
)

func TestConfiguredEnvironment(t *testing.T) {
	t.Parallel()
	exporter, err := New(output.Params{
		ConfigArgument: "https://proxy.example.test", StdOut: io.Discard,
		Environment: map[string]string{
			"PG_PROXY_TOKEN": "configured-token", "PG_PROXY_DB": "configured-db",
			"PG_PROXY_INSECURE_SKIP_TLS_VERIFY": "false", "syntheticMonitoring": "true",
			"runId": "run", "nodeName": "node", "scenarioName": "scenario", "location": "AMS",
			"CUSTOM_URL_PATTERNS": "[0-9]+=={id}",
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, exporter.Stop()) })
	logger := exporter.(*Logger)
	assert.Equal(t, "configured-token", logger.token)
	assert.Equal(t, "configured-db", logger.database)
	assert.True(t, logger.isSynthetic)
	assert.Equal(t, "run", logger.envTags["run_id"])
	assert.Equal(t, "node", logger.envTags["node_name"])
	assert.Equal(t, "scenario", logger.envTags["scenario_name"])
	assert.Equal(t, "AMS", logger.envTags["location"])
	assert.Equal(t, "{id}", logger.customPatterns["[0-9]+"])
	assert.False(t, logger.httpClient.Transport.(*http.Transport).TLSClientConfig.InsecureSkipVerify)
}

func TestInvalidProxyTLSConfiguration(t *testing.T) {
	t.Parallel()
	exporter, err := New(output.Params{
		ConfigArgument: "https://proxy.example.test", StdOut: io.Discard,
		Environment: map[string]string{
			"PG_PROXY_TOKEN": "test", "PG_PROXY_INSECURE_SKIP_TLS_VERIFY": "invalid",
		},
	})
	require.ErrorContains(t, err, "PG_PROXY_INSECURE_SKIP_TLS_VERIFY")
	assert.Nil(t, exporter)
}
