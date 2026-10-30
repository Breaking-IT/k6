# Breaking-IT k6 fork

This repository is a modified version of [Grafana k6](https://github.com/grafana/k6).
Breaking-IT maintains it for use with BreakTest's load-generation integrations.

## License

The complete work, including the Breaking-IT modifications, is licensed under the
[GNU Affero General Public License, version 3](LICENSE.md). The original copyright and
license notices are retained.

## Source releases

The `breakingit` branch contains the current fork source. Immutable source-release tags are
published for distributed builds. The initial public release is
[`breakingit-v2.1.0-26`](https://github.com/Breaking-IT/k6/tree/breakingit-v2.1.0-26).

Each BreakTest release that contains this component should identify the corresponding immutable
tag or commit in this repository. Build and deployment tooling must not add proprietary source
code to the k6 executable.

## Fork-specific changes

- Adds the `timescaledb-breakingit` output for writing load-test data through a PG-proxy.
- Adds the opt-in `custom-db-breakingit` direct output alongside TimescaleDB; see
  [configuration and current limits](internal/output/breakingit/CUSTOM_DB.md).
- Adds optional detailed request and response data for failed HTTP requests.
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

## Development and builds in jgaalen/k6

`breakingit` is the custom build branch, based on upstream k6 v2.3.0. Build from `breakingit` to
include the fork customizations, native group transactions and the browser group-context fix together.
Upstream releases are integrated through `master` and then merged into `breakingit`; push custom
changes only to the jgaalen/k6 fork.

```sh
git switch breakingit
git pull --ff-only origin breakingit
./build-linux-binaries.sh
```

The build script reads the source in the current checkout and writes Linux and macOS binaries for
amd64 and arm64 into `dist/`. It does not install or deploy them. Its displayed `VERSION` value is
informational; use the binary's `version` command and SHA-256 checksum to identify an artifact.

For the complete change history, compare the `breakingit` branch with the upstream
[grafana/k6](https://github.com/grafana/k6) repository.
