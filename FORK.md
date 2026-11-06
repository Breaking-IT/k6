# Breaking-IT k6 fork

This repository is a modified version of [Grafana k6](https://github.com/grafana/k6).
Breaking-IT maintains it for use with BreakTest's load-generation integrations.

## License

The complete work, including the Breaking-IT modifications, is licensed under the
[GNU Affero General Public License, version 3](LICENSE.md). The original copyright and
license notices are retained.

## Source releases

The `master` branch contains the current fork source. The historical `breakingit` branch is
retained for reference. Immutable source-release tags are
published for distributed builds. The initial public release is
[`breakingit-v2.1.0-26`](https://github.com/Breaking-IT/k6/tree/breakingit-v2.1.0-26).

Each BreakTest release that contains this component should identify the corresponding immutable
tag or commit in this repository. Build and deployment tooling must not add proprietary source
code to the k6 executable.

## Fork-specific changes

- Adds the `timescaledb-breakingit` output for writing load-test data through a PG-proxy.
- Adds optional detailed request and response data for failed HTTP requests.
- Adds the optional `httpRequestUrl` option to keep a named request's real URL in sample metadata.
- Extends browser network and error metrics, including optional error screenshots.
- Adds the `lean` build tag used for smaller cloud-free release binaries.
- Adds Breaking-IT build helpers and operational configuration.
- Excludes extension-usage, installation-ID, and build-origin reporting in all builds.
  Basic anonymous run statistics retain the existing `K6_NO_USAGE_REPORT` opt-out.
- Preserves custom browser timings and error payloads with upstream async metric context.
- Enables async metric context by default, with explicit feature lists retaining opt-out control.
- Exports native group durations and failure outcomes as BreakTest transactions alongside legacy
  `pages` metrics. See [transaction configuration](internal/output/breakingit/README.md).
- Restores metric context correctly when native promise settlement re-enters the runtime, preventing
  browser group paths from accumulating across iterations.
- Excludes upstream's Grafana-specific Renovate approval automation.
- Runs lean release builds and fork regression tests on standard Linux GitHub Actions runners.

## Development and builds in Breaking-IT/k6

`master` is the default development and build branch, based on upstream k6 v2.3.0. It includes
the consolidated `breakingit` customizations, native group transactions and browser group-context
fix. Browser admission control remains reverted. Create feature branches from `origin/master`
and merge tested pull requests into `master` in Breaking-IT/k6.

Track stable upstream releases: fetch tags from `upstream`, create an integration branch from
`origin/master`, and merge the new release tag into that branch. Preserve the fork changes,
validate normal and lean builds, and open a pull request against Breaking-IT/k6 `master`. Do not
replace the fork branch with upstream history. The `upstream` remote is fetch-only; keep its
push URL set to `DISABLED` and push only to `origin` (Breaking-IT/k6).

```sh
git switch master
git pull --ff-only origin master
./build-linux-binaries.sh
```

The build script reads the source in the current checkout and writes Linux and macOS binaries for
amd64 and arm64 into `dist/`. It does not install or deploy them. Its displayed `VERSION` value is
informational; use the binary's `version` command and SHA-256 checksum to identify an artifact.
Filenames include the build-start timestamp in UTC after `-breakingit`, for example
`k6-linux-amd64-breakingit-20261013T143000Z`. All four binaries from one invocation share the
same timestamp.

For the complete change history, compare the `master` branch with the upstream
[grafana/k6](https://github.com/grafana/k6) repository.

## Validation before merging

Run lint and the full race-enabled suite with bounded concurrency. Browser tests launch real
Chromium processes; on a workstation, the upstream 210-second package timeout and default
parallelism can overload the browser suite.

```sh
make lint
go test -race -p 2 -parallel 2 -timeout 15m ./...
go test -race -p 2 -parallel 2 -tags lean -timeout 210s \
  ./internal/cmd ./internal/cmd/tests ./internal/api ./internal/js \
  ./internal/features ./internal/output/breakingit
go build -o dist/k6 .
go build -tags lean -o dist/k6-lean .
```

Cloud, REST API, and excluded-output integration tests require the full build. Lean-specific
checks verify their stubs and output availability; shared runtime and fork regression tests
run in both configurations.
