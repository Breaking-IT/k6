package metrics

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRequestTrackerPinsBeforeTimestampAssignment(t *testing.T) {
	ch := make(chan SampleContainer, 1)
	tracker, unbind := BindRequestTracker(ch)
	defer unbind()
	ticket := BeginRequest(ch)
	require.NotNil(t, ticket)
	require.False(t, tracker.Before().After(ticket.Time))
	ticket.Release()
	ticket.Release()
	for i := range tracker.shards {
		require.Empty(t, tracker.shards[i].active)
	}
	unbind()
	require.Nil(t, BeginRequest(ch))
}

func TestRequestTrackerConcurrentProducers(t *testing.T) {
	ch := make(chan SampleContainer)
	tracker, unbind := BindRequestTracker(ch)
	defer unbind()
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() {
			for range 1000 {
				ticket := BeginRequest(ch)
				require.False(t, tracker.Before().After(ticket.Time))
				ticket.Release()
			}
		})
	}
	wg.Wait()
	for i := range tracker.shards {
		require.Empty(t, tracker.shards[i].active)
	}
}
