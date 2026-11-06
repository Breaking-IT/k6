//go:build !lean

package cmd

import (
	"errors"
	"fmt"

	"go.k6.io/k6/v2/cmd/state"
	"go.k6.io/k6/v2/ext"
	"go.k6.io/k6/v2/internal/dashboard"
	"go.k6.io/k6/v2/internal/output/breakingit"
	"go.k6.io/k6/v2/internal/output/cloud"
	"go.k6.io/k6/v2/internal/output/csv"
	"go.k6.io/k6/v2/internal/output/influxdb"
	"go.k6.io/k6/v2/internal/output/json"
	"go.k6.io/k6/v2/internal/output/opentelemetry"
	"go.k6.io/k6/v2/internal/output/prometheusrw/remotewrite"
	"go.k6.io/k6/v2/output"
)

// TODO: move this to an output sub-module after we get rid of the old collectors?
func getAllOutputConstructors() (map[string]output.Constructor, error) {
	// Start with the built-in outputs
	result := map[string]output.Constructor{
		builtinOutputJSON.String():     json.New,
		builtinOutputCloud.String():    cloud.New,
		builtinOutputCSV.String():      csv.New,
		builtinOutputInfluxdb.String(): influxdb.New,
		builtinOutputKafka.String(): func(_ output.Params) (output.Output, error) {
			return nil, errors.New("the kafka output was deprecated in k6 v0.32.0 and removed in k6 v0.34.0, " +
				"please use the new xk6 kafka output extension instead - https://github.com/k6io/xk6-output-kafka")
		},
		builtinOutputStatsd.String(): func(_ output.Params) (output.Output, error) {
			return nil, errors.New("the statsd output was deprecated in k6 v0.47.0 and removed in k6 v0.55.0, " +
				"please use the new xk6 statsd output extension instead. " +
				"It can be found at https://github.com/LeonAdato/xk6-output-statsd and " +
				"more info at https://github.com/grafana/k6/issues/2982")
		},
		builtinOutputDatadog.String(): func(_ output.Params) (output.Output, error) {
			return nil, errors.New("the datadog output was deprecated in k6 v0.32.0 and removed in k6 v0.34.0, " +
				"please use the statsd output extension https://github.com/LeonAdato/xk6-output-statsd with environment " +
				"variable K6_STATSD_ENABLE_TAGS=true or an experimental opentelemetry output instead",
			)
		},
		builtinOutputExperimentalPrometheusRW.String(): func(params output.Params) (output.Output, error) {
			return remotewrite.New(params)
		},
		"web-dashboard": dashboard.New,
		builtinOutputTimescaledbBreakingit.String(): breakingit.New,
		builtinOutputExperimentalOpentelemetry.String(): func(params output.Params) (output.Output, error) {
			params.Logger.Warnf("OpenTelemetry output has been graduated as a stable output."+
				"You can now use just %q instead of %q. The experimental version will be removed in future versions.",

				"opentelemetry", "experimental-opentelemetry")
			return opentelemetry.New(params)
		},
		builtinOutputOpentelemetry.String(): func(params output.Params) (output.Output, error) {
			return opentelemetry.New(params)
		},
	}

	exts := ext.Get(ext.OutputExtension)
	for _, e := range exts {
		if _, ok := result[e.Name]; ok {
			return nil, fmt.Errorf("invalid output extension %s, built-in output with the same type already exists", e.Name)
		}
		m, ok := e.Module.(output.Constructor)
		if !ok {
			return nil, fmt.Errorf("unexpected output extension type %T", e.Module)
		}
		result[e.Name] = m
	}

	return result, nil
}

// attachCloudLogDrainer gives the cloud output the local-execution log pusher
// so it flushes buffered logs before the run is notified complete.
func attachCloudLogDrainer(out output.Output, gs *state.GlobalState) {
	if co, ok := out.(*cloud.Output); ok && gs.CloudLogPusher != nil {
		co.SetLogDrainer(gs.CloudLogPusher)
	}
}
