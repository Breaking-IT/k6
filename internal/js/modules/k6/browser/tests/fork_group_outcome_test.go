package tests

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.k6.io/k6/v2/metrics"
)

func TestBrowserGroupTransactionOutcomes(t *testing.T) {
	t.Parallel()

	tb := newTestBrowser(t)
	prepareAsyncGroupBrowser(t, tb)
	_, err := tb.vu.RunAsync(t, `
		const page = await browser.newPage();
		try {
			await k6.group('success', async () => {
				await page.setContent('<h1>Checkout</h1>');
				await page.locator('h1').waitFor({state: 'visible'});
			});
			try {
				await k6.group('failure', async () => {
					await page.goto('http://127.0.0.1:1');
				});
				throw new Error('expected an unsafe-port navigation failure');
			} catch (error) {
				if (!String(error).includes('ERR_UNSAFE_PORT')) throw error;
			}
			await k6.group('caught', async () => {
				try { await page.goto('http://127.0.0.1:1'); } catch (_) {}
			});
		} finally {
			await page.close();
		}
	`)
	require.NoError(t, err)

	outcomes := map[string][]bool{}
	errors := map[string]int{}
	for _, sample := range collectBrowserSamples(tb) {
		group, _ := sample.Tags.Get("group")
		switch sample.Metric.Name {
		case metrics.GroupDurationName:
			outcomes[group] = append(outcomes[group], sample.GroupFailed)
			assert.Greater(t, sample.Value, float64(0))
		case "browser_errors":
			errors[group]++
		}
	}
	assert.Equal(t, map[string][]bool{
		"::success": {false}, "::failure": {true}, "::caught": {false},
	}, outcomes)
	assert.Equal(t, map[string]int{"::failure": 1, "::caught": 1}, errors)
}
