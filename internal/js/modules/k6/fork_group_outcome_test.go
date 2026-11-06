package k6

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.k6.io/k6/v2/metrics"
)

func TestGroupTransactionOutcome(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		callback string
		async    bool
		failed   bool
	}{
		{"sync success", `() => 42`, false, false},
		{"sync throw", `() => { throw new Error('failed'); }`, false, true},
		{"sync with feature", `() => 42`, true, false},
		{"sync throw with feature", `() => { throw new Error('failed'); }`, true, true},
		{"async success", `async () => { await Promise.resolve(); return 42; }`, true, false},
		{"async rejection", `async () => { await Promise.resolve(); throw new Error('failed'); }`, true, true},
		{"primitive rejection", `() => Promise.reject(undefined)`, true, true},
		{"caught rejection", `async () => { try { await Promise.reject('failed'); } catch (_) {} }`, true, false},
		{"failed check", `async () => { k6.check(false, {bad: value => value}); }`, true, false},
		{"check then throw", `async () => { if (!k6.check(false, {bad: v => v})) throw 'bad'; }`, true, true},
		{"throwing getter", `() => ({get then() { throw 'failed'; }})`, true, true},
		{"thenable resolve", `() => ({then(resolve) { resolve(42); }})`, true, false},
		{"thenable reject", `() => ({then(_, reject) { reject('failed'); }})`, true, true},
		{"thenable throw", `() => ({then() { throw 'failed'; }})`, true, true},
		{"first settlement wins", `() => ({then(resolve, reject) { resolve(42); reject('late'); }})`, true, false},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			tc := testCaseRuntimeWithAsyncMetricContext(t, test.async)
			_, err := tc.testRuntime.RunOnEventLoop(fmt.Sprintf(`
				(async () => {
					let failed = false;
					try { await k6.group('step', %s); } catch (_) { failed = true; }
					if (failed !== %t) throw new Error('callback outcome changed');
				})()
			`, test.callback, test.failed))
			require.NoError(t, err)
			var durations []metrics.Sample
			for _, container := range metrics.GetBufferedSamples(tc.samples) {
				for _, sample := range container.GetSamples() {
					if sample.Metric.Name == metrics.GroupDurationName {
						durations = append(durations, sample)
					}
				}
			}
			require.Len(t, durations, 1)
			assert.Equal(t, test.failed, durations[0].GroupFailed)
			assert.Equal(t, map[string]string{"group": "::step"}, durations[0].Tags.Map())
			assert.GreaterOrEqual(t, durations[0].Value, float64(0))
		})
	}
}

func TestGroupTransactionOutcomesStayIsolated(t *testing.T) {
	t.Parallel()
	tc := testCaseRuntime(t)
	_, err := tc.testRuntime.RunOnEventLoop(`
		(async () => {
			await Promise.allSettled([
				k6.group('same', async () => { await Promise.resolve(); throw 'failed'; }),
				k6.group('same', async () => { await Promise.resolve(); }),
			]);
			await k6.group('parent', async () => {
				try { await k6.group('child', async () => { throw 'failed'; }); } catch (_) {}
			});
			try {
				await k6.group('uncaught', async () => {
					await k6.group('child', async () => { throw 'failed'; });
				});
			} catch (_) {}
		})()
	`)
	require.NoError(t, err)
	outcomes := map[string][]bool{}
	for _, container := range metrics.GetBufferedSamples(tc.samples) {
		for _, sample := range container.GetSamples() {
			if sample.Metric.Name == metrics.GroupDurationName {
				group, _ := sample.Tags.Get("group")
				outcomes[group] = append(outcomes[group], sample.GroupFailed)
			}
		}
	}
	assert.Equal(t, map[string][]bool{
		"::same": {true, false}, "::parent": {false}, "::parent::child": {true},
		"::uncaught": {true}, "::uncaught::child": {true},
	}, outcomes)
}
