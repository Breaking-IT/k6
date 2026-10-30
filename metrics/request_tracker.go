package metrics

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// RequestTracker pins request-end timestamps until the output has serialized the
// entire request. Registration happens before assigning the timestamp, not when
// the metrics eventually arrive at the output. Only opted-in outputs pay this cost.
type requestShard struct {
	mu     sync.Mutex
	active map[*RequestTicket]time.Time
}
type RequestTracker struct {
	origin time.Time
	next   atomic.Uint64
	shards [64]requestShard
}

type RequestTicket struct {
	tracker *RequestTracker
	shard   uint64
	Time    time.Time
}

var requestTrackers sync.Map // chan<- SampleContainer -> *RequestTracker

func BindRequestTracker(ch chan<- SampleContainer) (*RequestTracker, func()) {
	t := &RequestTracker{origin: time.Now()}
	for i := range t.shards {
		t.shards[i].active = make(map[*RequestTicket]time.Time)
	}
	requestTrackers.Store(ch, t)
	return t, func() { requestTrackers.CompareAndDelete(ch, t) }
}

// BeginRequest assigns an end timestamp using an anchored monotonic clock.
// The pin and the timestamp are atomic with respect to Before().
func BeginRequest(ch chan<- SampleContainer) *RequestTicket {
	v, ok := requestTrackers.Load(ch)
	if !ok {
		return nil
	}
	return v.(*RequestTracker).Begin()
}
func (t *RequestTracker) Begin() *RequestTicket {
	index := t.next.Add(1) % uint64(len(t.shards))
	shard := &t.shards[index]
	shard.mu.Lock()
	defer shard.mu.Unlock()
	stamp := t.Clock()
	ticket := &RequestTicket{tracker: t, shard: index, Time: stamp}
	shard.active[ticket] = stamp
	return ticket
}
func (t *RequestTicket) Release() {
	if t == nil {
		return
	}
	shard := &t.tracker.shards[t.shard]
	shard.mu.Lock()
	delete(shard.active, t)
	shard.mu.Unlock()
}
func (t *RequestTracker) Before() time.Time {
	// New pins created during the scan have timestamps >= this captured clock.
	before := t.Clock()
	for i := range t.shards {
		shard := &t.shards[i]
		shard.mu.Lock()
		for _, stamp := range shard.active {
			if stamp.Before(before) {
				before = stamp
			}
		}
		shard.mu.Unlock()
	}
	return before.Truncate(time.Second)
}
func (t *RequestTracker) Clock() time.Time { return t.origin.Add(time.Since(t.origin)) }

// RequestSamples preserves the normal payload while identifying its lifecycle pin.
type RequestSamples struct {
	SampleContainer
	Ticket *RequestTicket
}

func (s RequestSamples) RequestTicket() *RequestTicket { return s.Ticket }

// PushTrackedRequest keeps the lifecycle pin across both the engine queue and
// the output's mapping queue. A cancelled producer releases a never-enqueued pin.
func PushTrackedRequest(ctx context.Context, ch chan<- SampleContainer, ticket *RequestTicket, sample SampleContainer) bool {
	if ticket != nil {
		sample = RequestSamples{SampleContainer: sample, Ticket: ticket}
	}
	if PushIfNotDone(ctx, ch, sample) {
		return true
	}
	ticket.Release()
	return false
}
func RequestTime(ticket *RequestTicket, fallback time.Time) time.Time {
	if ticket != nil {
		return ticket.Time
	}
	return fallback
}
