package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"runtime"
	"strconv"
	"time"

	"go.k6.io/k6/v2/cmd/state"
	"go.k6.io/k6/v2/internal/build"
	"go.k6.io/k6/v2/internal/execution"
	"go.k6.io/k6/v2/internal/usage"
)

// envLookup adapts an environment map to the lookup-function shape the report
// helpers share with the run path's LookupEnv.
func envLookup(env map[string]string) func(string) (string, bool) {
	return func(key string) (string, bool) {
		val, ok := env[key]
		return val, ok
	}
}

// addEnvironmentInfo stamps the fields identifying this k6 binary and where it runs.
func addEnvironmentInfo(m map[string]any, lookupEnv func(string) (string, bool)) {
	m["k6_version"] = build.Version
	m["goos"] = runtime.GOOS
	m["goarch"] = runtime.GOARCH
	m["is_ci"] = isCI(lookupEnv)
}

// createReport assembles the anonymous usage report for a run from its scheduler
// and recorded usage. Extension identities are intentionally not collected.
func createReport(u *usage.Usage, execScheduler *execution.Scheduler) map[string]any {
	execState := execScheduler.GetState()
	m := u.Map()
	// The fork deliberately excludes detailed extension-usage reporting.
	delete(m, "extensions")

	addEnvironmentInfo(m, execState.Test.LookupEnv)

	m["duration"] = execState.GetCurrentTestRunDuration().String()
	m["vus_max"] = uint64(execState.GetInitializedVUsCount()) //nolint:gosec
	m["iterations"] = execState.GetFullIterationCount()
	executors := make(map[string]int)
	for _, ec := range execScheduler.GetExecutorConfigs() {
		executors[ec.GetType()]++
	}
	m["executors"] = executors

	return m
}

// reportUsage sends the report built by create, logging the attempt and outcome
// at debug, all bounded by a timeout.
func reportUsage(ctx context.Context, gs *state.GlobalState, create func(ctx context.Context) map[string]any) {
	reportCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	gs.Logger.Debug("Sending usage report...")
	if err := postUsageReport(reportCtx, envLookup(gs.Env), create(reportCtx)); err != nil {
		gs.Logger.WithError(err).Debug("Error sending usage report")
	} else {
		gs.Logger.Debug("Usage report sent successfully")
	}
}

// defaultUsageReportURL is the production endpoint the anonymous usage report
// is sent to when K6_USAGE_REPORT_URL is not set.
const defaultUsageReportURL = "https://stats.grafana.org/k6-usage-report"

func postUsageReport(ctx context.Context, lookupEnv func(string) (string, bool), m map[string]any) error {
	body, err := json.Marshal(m)
	if err != nil {
		return err
	}

	usageStatsURL := defaultUsageReportURL
	if url, ok := lookupEnv(state.UsageReportURL); ok && url != "" {
		usageStatsURL = url
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, usageStatsURL, bytes.NewBuffer(body))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	res, err := http.DefaultClient.Do(req)
	if err == nil {
		_ = res.Body.Close()
	}

	return err
}

// isCI is a helper that follows a naive approach to determine if k6 is being
// executed within a CI system. This naive approach consists of checking if
// the "CI" environment variable (among others) is set.
//
// We treat the "CI" environment variable carefully, because it's the one
// used more often, and because we know sometimes it's explicitly set to
// "false" to signal that k6 is not running in a CI environment.
//
// It is not a foolproof method, but it should work for most cases.
func isCI(lookupEnv func(key string) (val string, ok bool)) bool {
	if ci, ok := lookupEnv("CI"); ok {
		ciBool, err := strconv.ParseBool(ci)
		if err == nil {
			return ciBool
		}
		// If we can't parse the "CI" value as a bool, we assume return true
		// because we know that at least it's not set to any variant of "false",
		// which is the most common use case, and the reasoning we apply below.
		return true
	}

	// List of common environment variables used by different CI systems.
	ciEnvVars := []string{
		"BUILD_ID",               // Jenkins, Cloudbees
		"BUILD_NUMBER",           // Jenkins, TeamCity
		"CI",                     // Travis CI, CircleCI, Cirrus CI, Gitlab CI, Appveyor, CodeShip, dsari
		"CI_APP_ID",              // Appflow
		"CI_BUILD_ID",            // Appflow
		"CI_BUILD_NUMBER",        // Appflow
		"CI_NAME",                // Codeship and others
		"CONTINUOUS_INTEGRATION", // Travis CI, Cirrus CI
		"RUN_ID",                 // TaskCluster, dsari
	}

	// Check if any of these variables are set
	for _, key := range ciEnvVars {
		if _, ok := lookupEnv(key); ok {
			return true
		}
	}

	return false
}
