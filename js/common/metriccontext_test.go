package common

import (
	"strconv"
	"testing"

	"github.com/grafana/sobek"
	"github.com/stretchr/testify/require"

	"go.k6.io/k6/v2/internal/features"
	"go.k6.io/k6/v2/lib"
	"go.k6.io/k6/v2/metrics"
)

func newMetricContextState(enabled bool) *lib.State {
	registry := metrics.NewRegistry()
	return &lib.State{
		FeatureFlags: &features.Flags{AsyncMetricContext: enabled},
		Tags: lib.NewVUStateTags(
			registry.RootTagSet().WithTagsFromMap(map[string]string{"group": lib.RootGroupPath}),
		),
	}
}

func setMetricContext(state *lib.State, group, phase, trace string) {
	state.Tags.Modify(func(tagsAndMeta *metrics.TagsAndMeta) {
		tagsAndMeta.SetTag("group", group)
		tagsAndMeta.SetTag("phase", phase)
		tagsAndMeta.SetMetadata("trace", trace)
	})
}

func requireMetricContext(t *testing.T, state *lib.State, group, phase, trace string) {
	t.Helper()
	current := state.Tags.GetCurrentValues()
	actualGroup, _ := current.Tags.Get("group")
	actualPhase, _ := current.Tags.Get("phase")
	require.Equal(t, group, actualGroup)
	require.Equal(t, phase, actualPhase)
	require.Equal(t, trace, current.Metadata["trace"])
}

func TestCapturedMetricContext(t *testing.T) {
	t.Parallel()

	state := newMetricContextState(true)
	setMetricContext(state, "::registered", "registered", "registered")
	captured := CaptureMetricContext(state)
	setMetricContext(state, "::active", "active", "active")

	restore := captured.Enter()
	requireMetricContext(t, state, "::registered", "registered", "registered")
	setMetricContext(state, "::callback", "callback", "callback")
	restore()
	requireMetricContext(t, state, "::active", "active", "active")

	// A reusable snapshot must not retain mutations from its previous invocation.
	restore = captured.Enter()
	requireMetricContext(t, state, "::registered", "registered", "registered")
	restore()
}

func TestCapturedMetricContextDisabled(t *testing.T) {
	t.Parallel()

	state := newMetricContextState(false)
	setMetricContext(state, "::registered", "registered", "registered")
	captured := CaptureMetricContext(state)
	setMetricContext(state, "::active", "active", "active")

	restore := captured.Enter()
	requireMetricContext(t, state, "::active", "active", "active")
	setMetricContext(state, "::callback", "callback", "callback")
	restore()
	requireMetricContext(t, state, "::callback", "callback", "callback")
}

func TestRunWithMetricContext(t *testing.T) {
	t.Parallel()

	state := newMetricContextState(true)
	setMetricContext(state, "::registered", "registered", "registered")
	captured := CaptureMetricContext(state)
	setMetricContext(state, "::active", "active", "active")

	value, err := RunWithMetricContext(captured, func() (string, error) {
		requireMetricContext(t, state, "::registered", "registered", "registered")
		setMetricContext(state, "::callback", "callback", "callback")
		return "result", nil
	})
	require.NoError(t, err)
	require.Equal(t, "result", value)
	requireMetricContext(t, state, "::active", "active", "active")
}

func TestMetricContextTracker(t *testing.T) {
	t.Parallel()

	state := newMetricContextState(true)
	tracker := NewMetricContextTracker(func() *lib.State { return state })
	setMetricContext(state, "::registered", "registered", "registered")
	captured := tracker.Grab()
	setMetricContext(state, "::active", "active", "active")

	tracker.Resumed(captured)
	requireMetricContext(t, state, "::registered", "registered", "registered")
	setMetricContext(state, "::callback", "callback", "callback")
	tracker.Exited()
	requireMetricContext(t, state, "::active", "active", "active")
}

func BenchmarkCapturedMetricContext(b *testing.B) {
	for _, metadataEntries := range []int{0, 4} {
		b.Run(strconv.Itoa(metadataEntries)+"_metadata", func(b *testing.B) {
			state := newMetricContextState(true)
			state.Tags.Modify(func(tagsAndMeta *metrics.TagsAndMeta) {
				tagsAndMeta.SetTag("phase", "registered")
				for i := range metadataEntries {
					tagsAndMeta.SetMetadata(string(rune('a'+i)), "value")
				}
			})
			captured := CaptureMetricContext(state)

			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				restore := captured.Enter()
				restore()
			}
		})
	}
}

// A native promise handler can settle another promise while Sobek drains jobs. The
// nested Go-to-JS call drains its reactions before the outer handler exits.
func TestMetricContextTrackerNativePromiseSettlement(t *testing.T) {
	t.Parallel()
	for _, rejectInner := range []bool{false, true} {
		t.Run(strconv.FormatBool(rejectInner), func(t *testing.T) {
			t.Parallel()
			state := newMetricContextState(true)
			rt := sobek.New()
			rt.SetAsyncContextTracker(NewMetricContextTracker(func() *lib.State { return state }))
			outer, resolveOuter, _ := rt.NewPromise()
			inner, resolveInner, reject := rt.NewPromise()
			require.NoError(t, rt.Set("outer", outer))
			require.NoError(t, rt.Set("inner", inner))
			setMetricContext(state, "::outer", "outer", "outer")
			require.NoError(t, rt.Set("settle", func(sobek.FunctionCall) sobek.Value {
				requireMetricContext(t, state, "::outer", "outer", "outer")
				if rejectInner {
					require.NoError(t, reject("reason"))
				} else {
					require.NoError(t, resolveInner("value"))
				}
				requireMetricContext(t, state, "::outer", "outer", "outer")
				return sobek.Undefined()
			}))
			_, err := rt.RunString(`outer.then(settle)`)
			require.NoError(t, err)
			setMetricContext(state, "::inner", "inner", "inner")
			require.NoError(t, rt.Set("checkInner", func() {
				requireMetricContext(t, state, "::inner", "inner", "inner")
			}))
			_, err = rt.RunString(`inner.then(checkInner, checkInner)`)
			require.NoError(t, err)
			setMetricContext(state, "", "root", "root")
			require.NoError(t, resolveOuter(nil))
			requireMetricContext(t, state, "", "root", "root")
		})
	}
}

func TestMetricContextTrackerNestedUntrackedReaction(t *testing.T) {
	t.Parallel()
	state := newMetricContextState(true)
	tracker := NewMetricContextTracker(func() *lib.State { return state })
	setMetricContext(state, "::registered", "registered", "registered")
	captured := tracker.Grab()
	setMetricContext(state, "", "root", "root")
	tracker.Resumed(captured)
	// Init-context reactions have no captured tags but must still balance their exit.
	tracker.Resumed(nil)
	tracker.Exited()
	requireMetricContext(t, state, "::registered", "registered", "registered")
	tracker.Exited()
	requireMetricContext(t, state, "", "root", "root")
}

func TestMetricContextTrackerVUIsolation(t *testing.T) {
	t.Parallel()
	states := []*lib.State{newMetricContextState(true), newMetricContextState(true)}
	trackers := make([]sobek.AsyncContextTracker, len(states))
	for i, state := range states {
		trackers[i] = NewMetricContextTracker(func() *lib.State { return state })
		group := "::vu" + strconv.Itoa(i)
		setMetricContext(state, group, group, group)
		captured := trackers[i].Grab()
		setMetricContext(state, "", "root", "root")
		trackers[i].Resumed(captured)
	}
	// These are independent VUs, so their exits need not follow a shared stack order.
	trackers[0].Exited()
	requireMetricContext(t, states[0], "", "root", "root")
	requireMetricContext(t, states[1], "::vu1", "::vu1", "::vu1")
	trackers[1].Exited()
	requireMetricContext(t, states[1], "", "root", "root")
}
