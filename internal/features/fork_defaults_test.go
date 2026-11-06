package features

import (
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestForkDefaultAsyncMetricContext(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		cli    Source
		json   Source
		env    map[string]string
		active []string
	}{
		{name: "default", active: []string{"async-metric-context"}},
		{name: "empty CLI opts out", cli: supplied(""), active: []string{}},
		{name: "empty env opts out", env: map[string]string{"K6_FEATURES": ""}, active: []string{}},
		{name: "empty JSON opts out", json: supplied(""), active: []string{}},
		{name: "explicit list replaces default", cli: supplied("merge-run-tags"), active: []string{"merge-run-tags"}},
		{
			name: "explicit list includes default", cli: supplied("merge-run-tags,async-metric-context"),
			active: []string{"async-metric-context", "merge-run-tags"},
		},
		{
			name: "env overrides JSON", json: supplied("async-metric-context"),
			env: map[string]string{"K6_FEATURES": ""}, active: []string{},
		},
		{
			name: "CLI overrides env", cli: supplied(""),
			env: map[string]string{"K6_FEATURES": "async-metric-context"}, active: []string{},
		},
		{
			name: "CLI re-enables", cli: supplied("async-metric-context"),
			env: map[string]string{"K6_FEATURES": ""}, active: []string{"async-metric-context"},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			flags, err := Init(logrus.New(), test.cli, test.json, test.env)
			require.NoError(t, err)
			assert.Equal(t, test.active, flags.Activated())
			wantAsync := false
			for _, name := range test.active {
				wantAsync = wantAsync || name == "async-metric-context"
			}
			assert.Equal(t, wantAsync, flags.AsyncMetricContext)
			assert.Equal(t, wantAsync, flags.Tags()["k6_feature_async_metric_context"] == "true")
		})
	}
	definitions, err := All()
	require.NoError(t, err)
	for _, definition := range definitions {
		if definition.Name == "async-metric-context" {
			assert.Equal(t, Experimental, definition.Lifecycle)
			return
		}
	}
	t.Fatal("async-metric-context definition missing")
}
