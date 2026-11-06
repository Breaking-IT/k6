package breakingit

import (
	"errors"
)

func (c *BreakTestDBOutput) hello() error {
	hello, err := c.register()
	if err != nil {
		return err
	}
	if hello.Sealed || hello.Incarnation == "" || hello.Accepted == nil || *hello.Accepted != 0 {
		return errors.New("BreakTest DB requires a fresh unfinished producer source")
	}
	c.incarnation = hello.Incarnation
	return nil
}

// Called under addMu, after detaching every serialized row. Tickets in the engine
// queue and incomplete joins still pin Before(). Each FIFO carries its own barrier:
// the database takes the minimum across all lanes and generators.
func (c *BreakTestDBOutput) publishWatermark() {
	if c.tracker == nil || c.err() != nil {
		return
	}
	before := c.tracker.Before().UnixMicro() - pgEpochSeconds*1_000_000
	if before <= c.queuedWatermark {
		return
	}
	for _, lane := range c.lanes {
		select {
		case <-c.failed:
			return
		case lane.queue <- dbBatch{beforeUS: before}:
		}
	}
	c.queuedWatermark = before
}
