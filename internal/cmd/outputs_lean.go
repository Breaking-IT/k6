//go:build lean

package cmd

import (
	"errors"
	"fmt"

	"go.k6.io/k6/v2/cmd/state"
	"go.k6.io/k6/v2/ext"
	"go.k6.io/k6/v2/internal/output/breakingit"
	"go.k6.io/k6/v2/internal/output/csv"
	"go.k6.io/k6/v2/internal/output/json"
	"go.k6.io/k6/v2/output"
)

// getAllOutputConstructors returns the lean set of output constructors,
// excluding cloud, OpenTelemetry, Prometheus remote write, InfluxDB,
// and the web dashboard outputs.
func getAllOutputConstructors() (map[string]output.Constructor, error) {
	result := map[string]output.Constructor{
		builtinOutputJSON.String(): json.New,
		builtinOutputCSV.String():  csv.New,
		builtinOutputInfluxdb.String(): func(_ output.Params) (output.Output, error) {
			return nil, errors.New("the influxdb output is not available in this lean build of k6")
		},
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
		builtinOutputCloud.String(): func(_ output.Params) (output.Output, error) {
			return nil, errors.New("the cloud output is not available in this lean build of k6")
		},
		builtinOutputExperimentalPrometheusRW.String(): func(_ output.Params) (output.Output, error) {
			return nil, errors.New("the prometheus remote write output is not available in this lean build of k6")
		},
		builtinOutputExperimentalOpentelemetry.String(): func(_ output.Params) (output.Output, error) {
			return nil, errors.New("the opentelemetry output is not available in this lean build of k6")
		},
		builtinOutputOpentelemetry.String(): func(_ output.Params) (output.Output, error) {
			return nil, errors.New("the opentelemetry output is not available in this lean build of k6")
		},
		"web-dashboard": func(_ output.Params) (output.Output, error) {
			return nil, errors.New("the web-dashboard output is not available in this lean build of k6")
		},
		builtinOutputTimescaledbBreakingit.String(): breakingit.New,
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

func attachCloudLogDrainer(_ output.Output, _ *state.GlobalState) {}
