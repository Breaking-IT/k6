package tests

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The fork's error samples must retain the initiating operation's context,
// even after an async group has restored the root context.
func TestAsyncGroupBrowserErrorsKeepContext(t *testing.T) {
	t.Parallel()

	tb := newTestBrowser(t)
	prepareAsyncGroupBrowser(t, tb)
	_, err := tb.vu.RunAsync(t, `
		const page = await browser.newPage();
		try {
			for (const name of ['checkout', 'payment']) {
				await k6.group(name, async () => {
					exec.vu.metrics.tags.owner = name;
					exec.vu.metrics.metadata.trace = name;
					try {
						await page.goto('http://127.0.0.1:1');
						throw new Error('expected an unsafe-port navigation failure');
					} catch (error) {
						if (!String(error).includes('ERR_UNSAFE_PORT')) throw error;
					}
				});
			}
		} finally {
			await page.close();
		}
	`)
	require.NoError(t, err)

	seen := map[string]int{}
	for _, sample := range collectBrowserSamples(tb) {
		if sample.Metric.Name != "browser_errors" {
			continue
		}
		owner, _ := sample.Tags.Get("owner")
		group, _ := sample.Tags.Get("group")
		assert.Contains(t, []string{"checkout", "payment"}, owner)
		assert.Equal(t, "::"+owner, group)
		assert.Equal(t, owner, sample.Metadata["trace"])
		assert.Equal(t, float64(1), sample.Value)
		seen[owner]++
	}
	assert.Equal(t, map[string]int{"checkout": 1, "payment": 1}, seen)
}
