package tests

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.k6.io/k6/v2/internal/js/modules/k6/browser/k6ext/k6test"
	"go.k6.io/k6/v2/metrics"
)

func TestAsyncGroupBrowserIterationContext(t *testing.T) {
	t.Parallel()

	for _, nested := range []bool{false, true} {
		name := "sequential"
		if nested {
			name = "nested_rejection"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel() // Each browser VU must restore its own context independently.
			tb := newTestBrowser(t)
			prepareAsyncGroupBrowser(t, tb)
			for iteration := range 3 {
				if iteration > 0 {
					tb.vu.State().Iteration = int64(iteration)
					tb.vu.StartIteration(t, k6test.WithIteration(iteration))
				}
				_, err := tb.vu.RunAsync(t, `
     const context = await browser.newContext();
     try {
      const page = await context.newPage();
      const runGroups = async () => {
       await k6.group('first', async () => {
        await page.goto('data:text/html,<h1>First</h1>');
        k6.check(null, {first: true});
       });
       if (%t) {
        const reason = {message: 'original rejection'};
        try {
         await k6.group('rejected', async () => {
          await page.goto('data:text/html,<h1>Rejected</h1>');
          throw reason;
         });
         throw new Error('expected rejection');
        } catch (error) {
         if (error !== reason) throw error;
        }
       }
       await k6.group('last', async () => {
        await page.goto('data:text/html,<h1>Last</h1>');
        k6.check(null, {last: true});
       });
      };
      if (%t) { await k6.group('parent', runGroups); }
      else { await runGroups(); }
      k6.check(null, {outside: true});
     } finally {
      await context.close();
      k6.check(null, {cleanup: true});
     }
    `, nested, nested)
				require.NoError(t, err)
				// Inspect ambient state after the event loop drains, not only JS await continuations.
				group, _ := tb.vu.State().Tags.GetCurrentValues().Tags.Get("group")
				assert.Empty(t, group, "context leaked after iteration %d", iteration)
				tb.vu.EndIteration(t, k6test.WithIteration(iteration))
			}

			prefix := ""
			expected := map[string]int{"::first": 3, "::last": 3}
			if nested {
				prefix = "::parent"
				expected = map[string]int{
					"::parent": 3, "::parent::first": 3, "::parent::last": 3, "::parent::rejected": 3,
				}
			}
			samples := collectBrowserSamples(tb)
			counts := map[string]int{}
			for _, sample := range samples {
				if sample.Metric.Name == metrics.GroupDurationName {
					group, _ := sample.Tags.Get("group")
					counts[group]++
					assert.Positive(t, sample.Value)
				}
			}
			assert.Equal(t, expected, counts)
			assertBrowserCheckGroups(t, samples, map[string]string{
				"first": prefix + "::first", "last": prefix + "::last", "outside": "", "cleanup": "",
			})
		})
	}
}
