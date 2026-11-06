package tests

import (
	"maps"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.k6.io/k6/v2/internal/cmd"
	"go.k6.io/k6/v2/lib/fsext"
)

func TestForkDefaultGroupAndOptOut(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name    string
		args    []string
		env     map[string]string
		enabled bool
	}{
		{name: "default", enabled: true},
		{name: "CLI opt-out", args: []string{"--features="}},
		{name: "environment opt-out", env: map[string]string{"K6_FEATURES": ""}},
		{name: "explicit list replaces default", args: []string{"--features=merge-run-tags"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			args := append([]string{"--out=json=results.json", "--summary-mode=disabled"}, test.args...)
			ts := getSingleFileTestState(t, `
				import { check, group } from 'k6';
				export default async function () {
					let supported = false;
					try {
						await group('step', async () => {
							await Promise.resolve();
							check(true, { inside: value => value });
						});
						supported = true;
					} catch (error) {
						if (!String(error).includes('group() does not support async')) throw error;
					}
					check(supported, { supported: value => value });
				}
			`, args, 0)
			maps.Copy(ts.Env, test.env)
			cmd.ExecuteWithGlobalState(ts.GlobalState)
			results, err := fsext.ReadFile(ts.FS, "results.json")
			require.NoError(t, err)
			want := float64(0)
			if test.enabled {
				want = 1
				assert.Equal(t, []float64{1}, getSampleValues(t, results, "checks", map[string]string{
					"check": "inside", "group": "::step",
				}))
				assert.Len(t, getSampleValues(t, results, "group_duration", map[string]string{"group": "::step"}), 1)
			} else {
				assert.Empty(t, getSampleValues(t, results, "group_duration", nil))
			}
			assert.Equal(t, []float64{want}, getSampleValues(t, results, "checks", map[string]string{
				"check": "supported", "group": "",
			}))
		})
	}
}
