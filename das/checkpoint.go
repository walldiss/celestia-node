package das

import (
	"fmt"
	"strings"
)

type checkpoint struct {
	SampleFrom  uint64 `json:"sample_from"`
	NetworkHead uint64 `json:"network_head"`
	// Failed will be prioritized on restart
	Failed map[uint64]int `json:"failed,omitempty"`
	// Workers will resume on restart from corresponding state
	Workers []workerCheckpoint `json:"workers,omitempty"`
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
		SampleFrom:  stats.HeadOfCatchup + 1,
		NetworkHead: stats.NetworkHead,
		Failed:      stats.Failed,
		Workers:     workers,
	}
}

func (c checkpoint) String() string {
	buf := strings.Builder{}
	buf.WriteString(fmt.Sprintf("SampleFrom: %v, NetworkHead: %v",
		c.SampleFrom, c.NetworkHead))

	if len(c.Workers) > 0 {
		buf.WriteString(fmt.Sprintf(", Workers: %v", len(c.Workers)))
	}

	if len(c.Failed) > 0 {
		buf.WriteString(fmt.Sprintf("\nFailed: %v", c.Failed))
	}

	return buf.String()
}
