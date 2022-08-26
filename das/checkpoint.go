package das

import (
	"bytes"
	"fmt"
)

type checkpoint struct {
	SampledBefore uint64             `json:"sampled_before"`
	MaxKnown      uint64             `json:"max_known"`
	Failed        map[uint64]int     `json:"failed,omitempty"` // failed will be put in priority on restart
	Workers       []workerCheckpoint `json:"workers,omitempty"`
}

// workerCheckpoint will be used to resume worker on restart
type workerCheckpoint struct {
	From uint64 `json:"from"`
	To   uint64 `json:"to"`
}

func newCheckpoint(stats SamplingStats) checkpoint {
	workers := make([]workerCheckpoint, 0, len(stats.Workers))
	for _, w := range stats.Workers {
		workers = append(workers, workerCheckpoint{
			From: w.Curr,
			To:   w.To,
		})
	}
	return checkpoint{
		SampledBefore: stats.SampledBefore,
		MaxKnown:      stats.MaxKnown,
		Failed:        stats.Failed,
		Workers:       workers,
	}
}

func (c checkpoint) String() string {
	buf := bytes.Buffer{}
	buf.WriteString(fmt.Sprintf(
		"SampledBefore: %v, MaxKnown: %v",
		c.SampledBefore, c.MaxKnown))

	if len(c.Failed) > 0 {
		buf.WriteString(fmt.Sprintf("\nFailed: %v", c.Failed))
	}

	if len(c.Workers) > 0 {
		buf.WriteString("\nWorkers: ")
	}
	for _, w := range c.Workers {
		buf.WriteString(fmt.Sprintf("\n from: %v, to: %v", w.From, w.To))
	}
	return buf.String()
}
