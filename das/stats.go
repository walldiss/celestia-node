package das

import (
	"time"
)

// TODO(@walldiss): discuss new stats

// state collects information about the DASer process. Currently, there are
// only two sampling routines: the main sampling routine which performs sampling
// over current network headers, and the `catchUp` routine which performs sampling
// over past headers from the last sampled checkpoint.
type stats struct {
	State checkpoint `json:"state"`
	// tracks whether routine is running
	IsRunning bool `json:"is_running"`
}

// RoutineState contains important information about the state of a
// current sampling routine.
type RoutineState struct {
	// reports if an error has occurred during the routine's
	// sampling process
	Error error `json:"error"`
	// tracks the latest successfully sampled height of the routine
	LatestSampledHeight uint64 `json:"latest_sampled_height"`
	// tracks the square width of the latest successfully sampled
	// height of the routine
	LatestSampledWidth uint64 `json:"latest_sampled_width"`
	// tracks amount of busy parallel workers
	Concurrency uint64 `json:"concurrency"`
	// tracks whether all known headers are sampled
	CatchUpDone bool `json:"catch_up_done"`
	// tracks whether routine is running
	IsRunning bool `json:"is_running"`
}

// JobInfo contains information about a catchUp job.
type JobInfo struct {
	Start time.Time `json:"start"`
	End   time.Time `json:"end"`
	Error error     `json:"error"`

	ID     uint64 `json:"id"`
	Height uint64 `json:"height"`
	From   uint64 `json:"from"`
	To     uint64 `json:"to"`
}

func (ji JobInfo) Finished() bool {
	return ji.To == ji.Height
}

func (ji JobInfo) Duration() time.Duration {
	return ji.End.Sub(ji.Start)
}
