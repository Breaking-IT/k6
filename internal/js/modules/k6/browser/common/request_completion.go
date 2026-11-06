package common

import (
	"time"

	k6metrics "go.k6.io/k6/v2/metrics"
)

// Browser completion is observed at terminal CDP-event dispatch, before async
// header/body retrieval. Chromium phase durations remain in their CDP time base;
// the result's timestamp is the generator-observed sampler completion, for ALL
// outputs. Retimestamping at output delivery would hide backlog and is forbidden.
type browserCompletion struct {
	at     time.Time
	ticket *k6metrics.RequestTicket
}

func (m *NetworkManager) beginBrowserCompletion() browserCompletion {
	var ticket *k6metrics.RequestTicket
	if state := m.vu.State(); state != nil {
		ticket = k6metrics.BeginRequest(state.Samples)
	}
	return browserCompletion{at: k6metrics.RequestTime(ticket, time.Now()), ticket: ticket}
}
