package das

// SamplingStats collects information about the DASer process. Currently, there are
// only two sampling routines: the main sampling routine which performs sampling
// over current network headers, and the `catchUp` routine which performs sampling
// over past headers from the last sampled checkpoint.
type SamplingStats struct {
	// all headers before SampledBefore were successfully sampled
	SampledBefore uint64 `json:"sampled_before_height"`
	// MaxKnown is the height of the newest known header
	MaxKnown uint64 `json:"max_known_height"`
	// Failed contains all skipped header's heights with corresponding try count
	Failed map[uint64]int `json:"failed,omitempty"`
	// Workers has information about each currently running worker stats
	Workers []WorkerStats `json:"workers,omitempty"`
	// Concurrency currently running parallel workers
	Concurrency int `json:"concurrency"`
	// CatchUpDone indicates whether all known headers are sampled
	CatchUpDone bool `json:"catch_up_done"`
}

type WorkerStats struct {
	Curr uint64 `json:"curr"`
	From uint64 `json:"from"`
	To   uint64 `json:"to"`

	ErrMsg string `json:"error,omitempty"`
}
