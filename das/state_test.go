package das

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/multierr"
)

func Test_coordinatorStats(t *testing.T) {
	tests := []struct {
		name  string
		state *coordinatorState
		want  SamplingStats
	}{
		{
			"basic",
			&coordinatorState{
				inProgress: map[int]func() workerState{
					1: func() workerState {
						return workerState{
							Curr:   25,
							From:   21,
							To:     30,
							failed: []uint64{22},
							Err:    errors.New("22: failed"),
						}
					},
					2: func() workerState {
						return workerState{
							Curr:   15,
							From:   11,
							To:     20,
							failed: []uint64{12, 13},
							Err:    multierr.Append(errors.New("12: failed"), errors.New("13: failed")),
						}
					},
				},
				failed:    map[uint64]int{22: 1, 23: 1, 24: 2},
				nextJobID: 0,
				next:      31,
				maxKnown:  100,
			},
			SamplingStats{
				MinSampled: 11,
				MaxKnown:   100,
				Failed:     map[uint64]int{22: 2, 23: 1, 24: 2, 12: 1, 13: 1},
				Workers: []WorkerStats{
					{
						Curr:   25,
						From:   21,
						To:     30,
						ErrMsg: "22: failed",
					},
					{
						Curr:   15,
						From:   11,
						To:     20,
						ErrMsg: "12: failed; 13: failed",
					},
				},
				Concurrency: 2,
				CatchUpDone: false,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stats := tt.state.stats()
			assert.Equalf(t, tt.want, stats, "stats()")
		})
	}
}
