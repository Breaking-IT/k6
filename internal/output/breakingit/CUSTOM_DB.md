# Direct Custom DB output

The fork has two independent built-in outputs, in both normal and `lean` builds:

- `timescaledb-breakingit`: the existing CSV/PG-proxy writer.
- `custom-db-breakingit`: direct zstd-compressed PostgreSQL binary COPY batches
  to the BreakTest Custom DB service. It does not contact PostgreSQL or PG-proxy.

The implementation lives in this fork, alongside the active built-in TimescaleDB
writer. It does not use the older xk6 extension in the BreakTest repository.

## Invocation and application contract

The application must first provision a Custom DB run and one producer token per
load generator. Pass the run ID and that generator's token when invoking k6:

```sh
export runId='<provisioned-run-id>'
export CUSTOM_STORAGE_TOKEN='<run-scoped-producer-token>'
export location='Amsterdam'
export nodeName='generator-1'
k6 run --out custom-db-breakingit=https://storage.example.test script.js
```

The URL is the service base URL (an optional routing prefix is retained). The
writer appends `/runs/<runId>/hello`, `/ingest/batch` and `/finish` to it. TLS
certificates are verified, connections reused, and redirects disabled so tokens
cannot be redirected to another origin. Never pass the service admin token.

`BREAKINGIT_TRANSACTION_SOURCE` and `CUSTOM_URL_PATTERNS` retain their existing
mapping behavior. Native groups, legacy `pages`, requests, WebSocket/custom trend
request rows, failed checks, browser errors and VUs use the existing mapping code.
Unknown CSV fields remain NULL in the binary encoding, and negative/unavailable
connect times become NULL. Error UUIDs are generated independently per output.

The application must select the output from the frozen test backend, provide the
producer credentials, and finalize/publish only after all producers finish.
This fork change does **not** yet update the BreakTest launch configuration,
remove its k6 preview restriction, rebuild its deployed generator image, or change
any customer's storage selection. Those changes must accompany deployment.
Generator machine statistics remain the responsibility of the existing agent.

## Delivery and limits

- Flush every 200 ms or at a 2 MiB encoded-row budget. Four queued batches bound
  outgoing buffering; a full queue applies backpressure rather than dropping rows.
  The building batch, one waiting enqueue, sender/compression buffers, and k6's own
  sample containers consume additional memory; 8 MiB is not a total RSS limit.
- One ordered sender per generator. Multiple generators have independent sources.
  This first version does not claim the throughput of the parallel Java writer.
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
- A fresh producer source is required. A server incarnation change is fatal in
  this first implementation. Automatic relaxed-durability reconnect requires a
  source clock contract that k6 does not yet supply.

## Watermarks and unsupported data

This writer **does not send live watermarks**. k6 output callbacks can be delayed
and out of event-time order, particularly for browser failure bodies. Neither
wall-clock time nor the latest observed sample proves that older samples cannot
arrive. Live reads therefore use the service's pending/raw and grace-period paths;
the final producer barrier permits complete aggregation after drain. Proven live
source barriers are a separate k6 core integration, not a timer setting.

Synthetic monitoring and browser screenshots are explicitly unsupported and
rejected. Ordinary browser request timings, failure response data, native groups
and browser error messages are supported. An unexpected screenshot-bearing error
also fails the output, rather than storing a dangling screenshot reference.

## Validation

```sh
go test -race ./internal/output/breakingit
go test -tags lean ./internal/cmd -run 'TestBuiltinOutputString|TestCustomDBAndTimescaleConstructors'
go test ./internal/cmd -run 'TestBuiltinOutputString|TestCustomDBAndTimescaleConstructors'
go build -tags lean -o /tmp/k6-custom-db-writer .
python3 ai-tests/custom-db/smoke.py --k6 /tmp/k6-custom-db-writer \
  --image breaktest-storage-lab:local
```

The smoke test creates its own disposable Docker service and temporary directory,
then runs both writers on the **same** metric stream. The TimescaleDB writer goes
to an isolated CSV-capture server; it does not write an actual TimescaleDB database.
The test compares every stored column against that CSV oracle, except independently
generated error UUIDs. It checks request, transaction, error and VU counts, complete
sealing and reads after storage-service restart. It never connects to existing
customer databases. This establishes protocol/mapping compatibility, not a
TimescaleDB performance benchmark or browser end-to-end acceptance.
