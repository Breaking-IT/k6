package cmd

import (
	"fmt"
	"sort"
	"strings"

	"go.k6.io/k6/v2/cmd/state"
	"go.k6.io/k6/v2/lib"
	"go.k6.io/k6/v2/output"
)

// webDashboardOutputName matches dashboard.OutputName. Duplicated here so that
// shared code can reference it without importing internal/dashboard, which
// embeds the dashboard UI assets and is excluded from lean builds.
const webDashboardOutputName = "web-dashboard"

// builtinOutput marks the available builtin outputs.
//
// NOTE: that the enumer is the github.com/dmarkham/enumer
//
//go:generate go run github.com/dmarkham/enumer@v1.5.11 -type=builtinOutput -trimprefix builtinOutput -transform=kebab -output builtin_output_gen.go
type builtinOutput uint32

const (
	builtinOutputCloud builtinOutput = iota
	builtinOutputCSV
	builtinOutputDatadog
	builtinOutputExperimentalPrometheusRW
	builtinOutputInfluxdb
	builtinOutputJSON
	builtinOutputKafka
	builtinOutputStatsd
	builtinOutputExperimentalOpentelemetry
	builtinOutputOpentelemetry
	builtinOutputSummary
	builtinOutputTimescaledbBreakingit
)

func getPossibleIDList(constrs map[string]output.Constructor) string {
	res := make([]string, 0, len(constrs))
	for k := range constrs {
		if k == "kafka" || k == "datadog" {
			continue
		}
		res = append(res, k)
	}
	sort.Strings(res)
	return strings.Join(res, ", ")
}

func createOutputs(
	gs *state.GlobalState, test *loadedAndConfiguredTest, executionPlan []lib.ExecutionStep,
) ([]output.Output, error) {
	outputConstructors, err := getAllOutputConstructors()
	if err != nil {
		return nil, err
	}
	baseParams := output.Params{
		ScriptPath:     test.source.URL,
		Logger:         gs.Logger,
		Environment:    gs.Env,
		StdOut:         gs.Stdout,
		StdErr:         gs.Stderr,
		FS:             gs.FS,
		ScriptOptions:  test.derivedConfig.Options,
		RuntimeOptions: test.preInitState.RuntimeOptions,
		ExecutionPlan:  executionPlan,
		Usage:          test.preInitState.Usage,
		Features:       test.preInitState.FeatureFlags,
	}

	outputs := test.derivedConfig.Out
	if test.derivedConfig.WebDashboard.Bool {
		outputs = append(outputs, webDashboardOutputName)
	}

	result := make([]output.Output, 0, len(outputs))

	for _, outputFullArg := range outputs {
		outputType, outputArg, _ := strings.Cut(outputFullArg, "=")
		outputConstructor, ok := outputConstructors[outputType]
		if !ok {
			return nil, fmt.Errorf(
				"invalid output type '%s', available types are: %s",
				outputType, getPossibleIDList(outputConstructors),
			)
		}
		if _, builtinErr := builtinOutputString(outputType); builtinErr == nil {
			err := test.preInitState.Usage.Strings("outputs", outputType)
			if err != nil {
				gs.Logger.WithError(err).Warnf("Couldn't report usage for output %q", outputType)
			}
		}

		params := baseParams
		params.OutputType = outputType
		params.ConfigArgument = outputArg
		params.JSONConfig = test.derivedConfig.Collectors[outputType]

		out, err := outputConstructor(params)
		if err != nil {
			return nil, fmt.Errorf("could not create the '%s' output: %w", outputType, err)
		}

		if thresholdOut, ok := out.(output.WithThresholds); ok {
			thresholdOut.SetThresholds(test.derivedConfig.Thresholds)
		}

		if builtinMetricOut, ok := out.(output.WithBuiltinMetrics); ok {
			builtinMetricOut.SetBuiltinMetrics(test.preInitState.BuiltinMetrics)
		}

		// If the output is configured to support the archive, and supports it, we proceed
		// with building an archive and setting it on the output instance.
		if !test.derivedConfig.NoArchiveUpload.Bool {
			if archiveOut, ok := out.(output.WithArchive); ok {
				archiveOut.SetArchive(test.makeArchive())
			}
		}

		attachCloudLogDrainer(out, gs)

		result = append(result, out)
	}

	return result, nil
}
