//go:build lean

package cmd

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.k6.io/k6/v2/internal/cmd/tests"
	"go.k6.io/k6/v2/output"
)

func TestLeanUnavailableOutputs(t *testing.T) {
	t.Parallel()
	constructors, err := getAllOutputConstructors()
	require.NoError(t, err)
	for _, name := range []string{"cloud", "influxdb", "experimental-prometheus-rw", "opentelemetry", "web-dashboard"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			constructor, ok := constructors[name]
			require.True(t, ok)
			out, err := constructor(output.Params{})
			require.ErrorContains(t, err, "not available in this lean build")
			assert.Nil(t, out)
		})
	}
	for _, name := range []string{"json", "csv", "timescaledb-breakingit"} {
		assert.Contains(t, constructors, name)
	}
}

func TestLeanCommandStubs(t *testing.T) {
	t.Parallel()
	ts := tests.NewGlobalTestState(t)
	cloud := getCmdCloud(ts.GlobalState)
	assert.Contains(t, cloud.Short, "not available in this lean build")
	assert.Empty(t, cloud.Commands())
	stats := getCmdStats(ts.GlobalState)
	assert.True(t, stats.Hidden)
	require.ErrorContains(t, stats.RunE(stats, nil), "not available in this lean build")
	assert.ErrorIs(t, getCmdCloudLogin(ts.GlobalState).RunE(nil, nil), errCloudNotAvailable)
}
