# BreakTest transactions

The `breakingit` exporter accepts both native k6 `group_duration` samples and the
legacy custom `pages` Trend. They use the existing `transactions` table (or
`transactions_sm` for synthetic monitoring); no database migration is required.

## Native groups (preferred for new scripts)

This fork enables k6 2.3's experimental async metric context by default, in both
normal and lean builds. No feature flag is needed for browser groups:

```sh
k6 run --out breakingit=https://your-pg-proxy script.js
```

Keep the existing `PG_PROXY_TOKEN`, run/location, and synthetic-monitoring
configuration; set `BASE_URL` for the example below.

To restore upstream's opt-in behavior for a run, use `k6 run --features='' script.js`
or `K6_FEATURES='' k6 run script.js`. An explicit feature list **replaces** the
default, following the existing CLI > environment > JSON configuration precedence.
If selecting other features, include `async-metric-context` in that list to keep it
enabled. An explicit JSON `"features": []` also disables the default unless a
higher-priority source overrides it. This remains an experimental feature, not GA.

Compared with the disabled behavior, group-local changes to VU metric tags and
metadata are restored on exit rather than leaking into subsequent steps.

```javascript
import { browser } from 'k6/browser';
import { check, group } from 'k6';
import { setTimeout } from 'k6/timers';

export const options = {
  scenarios: {
    checkout: {
      executor: 'shared-iterations',
      vus: 1,
      iterations: 1,
      options: { browser: { type: 'chromium' } },
    },
  },
};

export default async function () {
  const page = await browser.newPage();
  try {
    // Keep think time outside the group so it is not part of response_time.
    await new Promise(resolve => setTimeout(resolve, 1000));
    await group('01_OpenHomepage', async () => {
      const response = await page.goto(__ENV.BASE_URL);
      if (!check(response, { 'homepage is 200': r => r && r.status() === 200 })) {
        throw new Error('Homepage did not return 200');
      }
      await page.locator('.hero-product img').waitFor({ state: 'visible' });
    });
  } finally {
    await page.close();
  }
}
```

Each completed group invocation becomes one transaction:

- `transaction_name`: the group path without its leading `::`, for example
  `Checkout::Payment`. Nested groups produce separate parent and child rows;
  the parent duration includes its children, so these durations are not additive.
- `response_time`: the whole callback/returned-promise lifetime in integer
  milliseconds, including waits inside the group, on success **and** failure.
- `success`: `false` if the callback throws or its returned promise rejects;
  otherwise `true`. The fork carries this outcome on the duration sample, without
  adding tags or matching unrelated samples by name/timestamp.
- Browser requests and browser errors retain the group context and are associated
  with the same transaction name. A group is not limited to HTTP requests: waits,
  interactions, and other awaited work count toward its duration.

A failed `check()` still writes an error record, but does not by itself reject the
group. Throw on a failed check when it should fail the transaction, as above.
Likewise, HTTP error status codes do not automatically reject `page.goto()`.
Errors caught inside a group do not fail that group. Catching a rejection outside
the group does not change its failed outcome. Always await/return the work being
measured. Work aborted before group completion may have no duration sample.

Keep the `group` system tag enabled (the default) so names and associations are
available. Without it, transaction names fall back to `name` or `/`.

## Legacy support and migration

Existing `new Trend('pages', true)` scripts keep working unchanged: the `page` tag
is preferred as the transaction name and emitted samples remain successful rows.
If a legacy helper emits `pages` only on success, its failures still have no
transaction-duration row. This legacy behavior is deliberately preserved.

`BREAKINGIT_TRANSACTION_SOURCE` selects which duration metrics become transactions:

- `both` (default): export `pages` and `group_duration`. Use this for a mix of
  legacy and native steps that each emit only one of the two duration metrics.
- `groups`: export only `group_duration`. Use this while wrapping old helpers
  in native groups, to avoid recording the same step twice.
- `pages`: export only legacy `pages` durations.

This selector affects only transaction rows, not requests, checks, browser errors,
or k6's own metric aggregation. Invalid values stop exporter initialization.
There is no automatic deduplication: names/timestamps cannot reliably distinguish
an intentional nested transaction from two measurements of the same step.

For a full migration, remove `pages.add()` and use `await group(...)` for timing
and context, while keeping any business checks and throwing on failures.

### Proxy TLS configuration

`PG_PROXY_INSECURE_SKIP_TLS_VERIFY=false` enables certificate verification for the
PG-proxy connection. The default remains `true` for compatibility with existing
private proxy deployments. This setting applies only to output delivery, not to
HTTP requests generated by the test script.
