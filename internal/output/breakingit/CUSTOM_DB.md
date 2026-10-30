# Direct Custom DB output

The fork has two independent built-in outputs, in both normal and `lean` builds:

- `timescaledb-breakingit`: the existing CSV/PG-proxy writer.
- `custom-db-breakingit`: direct zstd-compressed PostgreSQL binary COPY batches
  to the BreakTest Custom DB service. It does not contact PostgreSQL or PG-proxy.

The implementation lives in this fork, alongside the active built-in TimescaleDB
writer. It does not use the older xk6 extension in the BreakTest repository.

## Invocation and application contract

The application must first provision a Custom DB run and one to four producer tokens per
load generator. Pass the run ID and that generator's token when invoking k6:

```sh
export runId='<provisioned-run-id>'
export CUSTOM_STORAGE_TOKEN='<lane-1-token>,<lane-2-token>'
export location='Amsterdam'
export nodeName='generator-1'
k6 run --out custom-db-breakingit=https://storage.example.test script.js
```

The URL is the service base URL (an optional routing prefix is retained). The
writer appends `/runs/<runId>/hello`, `/ingest/batch`, `/watermark` and `/finish` to it. TLS
certificates are verified, connections reused, and redirects disabled so tokens
cannot be redirected to another origin. Never pass the service admin token.

`BREAKINGIT_TRANSACTION_SOURCE` and `CUSTOM_URL_PATTERNS` retain their existing
mapping behavior. Native groups, legacy `pages`, requests, WebSocket/custom trend
request rows, failed checks, browser errors and VUs use the existing mapping code.
Unknown CSV fields remain NULL in the binary encoding, and negative/unavailable
connect times become NULL. Error UUIDs are generated independently per output.

The application selects the output from the frozen test backend, provisions all
lane tokens, and finalizes only after every lane finishes. BreakTest's integration
requires generator capability `k6-custom-v2`, verified against the installed binary
checksum; this prevents routing multi-lane jobs to the older single-source writer.
Generator machine statistics remain the responsibility of the existing agent.

## Delivery and limits

- Flush every 200 ms or at a 2 MiB encoded-row budget. Four queued batches per lane bound
  outgoing buffering; a full queue applies backpressure rather than dropping rows.
  The building batch, one waiting enqueue, sender/compression buffers, and k6's own
  sample containers consume additional memory; 8 MiB per lane is not a total RSS limit.
- One to four ordered compression/sender workers per generator, with independent
  sequences and shared keep-alive connections. Batches are distributed round-robin.
  Watermark-only messages reach idle lanes too. No high-load capacity claim yet.
- Incomplete HTTP joins are limited to 4,096 groups / an estimated 2 MiB payload
  budget. Oversized rows or exhausted join budgets stop the run explicitly.
- HTTP/browser joins include timestamp and tag-set identity. All required request
  metrics must arrive before a row is emitted; delayed browser failure payloads
  are not treated as success. Ambiguous duplicate metrics or incomplete groups at
  shutdown fail the output instead of publishing an apparently complete run.
- Retry transient failures and throttling with the exact same compressed bytes,
  boundary, and sequence, for at most 30 seconds per operation. Validate ACKs.
  Rejection, invalid ACK, or exhausted retries stops k6 and forbids `/finish`.
- Drain accepted sample containers and all queued requests before `/finish`.
  The producer does not seal the run itself. A final barrier proves delivery of
  this producer's data; it is not a durable-on-disk/fsync acknowledgement.
- A fresh producer source is required at startup. Tracked native and browser runs recover after a
  server incarnation change using the anchored source clock and checkpoint sequence:
  skip a held batch proven checkpointed; otherwise renumber and replay only that
  unacknowledged batch. Previously acknowledged, lost rows are not replayed; the
  service exposes its relaxed-durability coverage gaps. Same-incarnation conflicts
  remain fatal.

## Watermarks and unsupported data

Native and browser k6 runs send live request watermarks, using the same sequence-backed,
all-source barrier as the Java writer. The output manager installs a per-run
tracker before producers start. HTTP completion assigns its timestamp and pin
atomically; WebSocket connecting/ping and custom metrics are similarly tracked.
Pins survive the engine sample channel and pending metric joins until rows are
serialized. A sharded tracker computes the oldest unresolved timestamp. It uses
an anchored monotonic clock, so a wall-clock step cannot invalidate a sent barrier.

After detaching all preceding rows, the writer queues the same whole-second
barrier on every lane. A lane sends it only after acknowledging its earlier
batches. The service closes the common prefix into immediate 1s aggregates; open
seconds remain queryable through the pending/raw path. Event-time completeness
still does not imply fsync durability. Transactions and monitoring are separate
streams, just as in the Java request-watermark contract.

Browser requests acquire a completion pin when the generator dispatches the
terminal CDP event (including redirects and loading failures), before deferred
header/body retrieval. They emit one complete timing/failure bundle. The pin
survives deferred work and the output queue; skipped cached requests release it.
Mixed browser/native scenarios use the same tracker and lane barriers.

**Timestamp contract:** browser sampler timestamps now mean generator-observed
terminal CDP completion, for both Custom DB and TimescaleDB outputs. Previously,
successful rows used the response-header timestamp. Chromium phase/duration
measurements remain in their original time base. CDP transport delay precedes
this observation; this is not a claim that Chromium's original event clock is
watermarked. Nothing is retimestamped at output delivery to hide writer backlog.
Uninstrumented request metric producers fail closed.

Synthetic monitoring and browser screenshots are explicitly unsupported and
rejected. Ordinary browser request timings, failure response data, native groups
and browser error messages are supported. An unexpected screenshot-bearing error
also fails the output, rather than storing a dangling screenshot reference.

## Validation

```sh
go test -race ./metrics ./output ./lib/netext/httpext ./internal/output/breakingit
go test -tags lean ./internal/cmd -run 'TestBuiltinOutputString|TestCustomDBAndTimescaleConstructors'
go test ./internal/cmd -run 'TestBuiltinOutputString|TestCustomDBAndTimescaleConstructors'
go build -tags lean -o /tmp/k6-custom-db-writer .
python3 ai-tests/custom-db/smoke.py --k6 /tmp/k6-custom-db-writer \
  --image breaktest-storage-lab:local --lanes 4
python3 ai-tests/custom-db/smoke.py --k6 /tmp/k6-custom-db-writer --lanes 4 --restart-live
```

The smoke test creates its own disposable Docker service and temporary directory,
then runs both writers on the **same** metric stream. The TimescaleDB writer goes
to an isolated CSV-capture server; it does not write an actual TimescaleDB database.
The test compares every stored column against that CSV oracle, except independently
generated error UUIDs. It checks request, transaction, error and VU counts, complete
sealing and reads after storage-service restart. It never connects to existing
customer databases. This establishes protocol/mapping compatibility, not a
TimescaleDB performance benchmark or browser end-to-end acceptance.

The live smoke test also requires unfinished sources with advancing watermarks and
complete aggregate buckets before finalization. Crash mode kills only its own
throwaway service, verifies retained rows against the oracle without duplicates,
and requires explicit incomplete coverage whenever rows were lost.

Browser validation also runs real Chromium with redirects, iframe requests,
connection failure, caching and a delayed 503 response, comparing both writers
on the same stream. Use `--browser --container-image IMAGE` with a Linux binary
and an image containing Chromium. This mapping oracle is distinct from the
actual-database high-load benchmark in the BreakTest integration repository.
