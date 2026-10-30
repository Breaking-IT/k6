package breakingit

import (
	"encoding/json"
	"errors"
	"fmt"
)

type customRejection int

func (s customRejection) Error() string {
	return fmt.Sprintf("Custom DB rejected request (HTTP %d)", int(s))
}

type sourceHello struct {
	Incarnation string  `json:"incarnation"`
	Accepted    *uint64 `json:"accepted_sequence"`
	Checkpoint  *uint64 `json:"checkpoint_sequence"`
	Sealed      bool    `json:"sealed"`
}

func (c *CustomOutput) register() (sourceHello, error) {
	body := []byte(`{}`)
	if c.tracker != nil {
		body, _ = json.Marshal(map[string]any{"source_clock_us": c.tracker.Clock().UnixMicro() - pgEpochSeconds*1_000_000, "clock_contract": "sample-end-monotonic-v1"})
	}
	var hello sourceHello
	err := c.request("/hello", body, "application/json", 0, &hello)
	return hello, err
}

// Acknowledged, uncheckpointed rows may be lost under relaxed durability. The
// server exposes that loss as coverage gaps. Replay only the held unacknowledged
// batch, never already acknowledged history. Each ordered lane recovers itself.
func (c *CustomOutput) recoverSource(err error) (uint64, error) {
	var status customRejection
	if c.tracker == nil || !errors.As(err, &status) || status != 409 {
		return 0, err
	}
	hello, e := c.register()
	if e != nil {
		return 0, e
	}
	if hello.Sealed || hello.Incarnation == "" || hello.Incarnation == c.incarnation || hello.Accepted == nil || hello.Checkpoint == nil || *hello.Accepted != *hello.Checkpoint {
		return 0, err
	}
	c.incarnation = hello.Incarnation
	c.sequence = *hello.Accepted
	return *hello.Checkpoint, nil
}

func (c *CustomOutput) sendBatch(body []byte, content string) error {
	for attempt := 0; attempt < 4; attempt++ {
		next := c.sequence + 1
		var ack struct {
			Accepted *uint64 `json:"accepted"`
		}
		err := c.request("/ingest/batch", body, content, next, &ack)
		if err == nil {
			if ack.Accepted == nil || *ack.Accepted != next {
				return errors.New("invalid Custom DB batch acknowledgement")
			}
			c.sequence = next
			return nil
		}
		checkpoint, e := c.recoverSource(err)
		if e != nil {
			return e
		}
		if checkpoint > next {
			return errors.New("checkpoint exceeds outstanding batch sequence")
		}
		if next <= checkpoint {
			return nil
		} // Lost ACK, but checkpoint proves this batch persisted.
	}
	return errors.New("Custom DB restarted repeatedly during batch delivery")
}

func (c *CustomOutput) sendControl(path string, before int64) error {
	body := []byte(nil)
	if path == "/watermark" {
		body, _ = json.Marshal(map[string]int64{"before_us": before})
	}
	for attempt := 0; attempt < 4; attempt++ {
		var ack struct {
			Accepted  *uint64 `json:"accepted_sequence"`
			Watermark int64   `json:"watermark_us"`
			Finished  bool    `json:"finished"`
		}
		previous := c.sequence
		err := c.request(path, body, "application/json", c.sequence, &ack)
		if err == nil {
			if ack.Accepted == nil || *ack.Accepted != c.sequence || (path == "/watermark" && ack.Watermark != before) || (path == "/finish" && !ack.Finished) {
				return errors.New("invalid Custom DB control acknowledgement")
			}
			return nil
		}
		if checkpoint, e := c.recoverSource(err); e != nil {
			return e
		} else if checkpoint > previous {
			return errors.New("checkpoint exceeds acknowledged sequence")
		}
	}
	return errors.New("Custom DB restarted repeatedly during control delivery")
}
