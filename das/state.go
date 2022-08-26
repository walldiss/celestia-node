package das

import (
	"context"
)

// represents current state of sampling
type coordinatorState struct {
	rangeSize uint64

	priority   []job                      // list of headers heights that will be sampled with higher priority
	inProgress map[int]func() workerState // keeps track of running workers
	failed     map[uint64]int             // stores heights of failed headers with amount of attempt as value

	nextJobID int
	next      uint64 // all headers before next were sent to workers
	maxKnown  uint64 // max known header height

	catchUpDone   bool          // indicates if all headers are sampled
	catchUpDoneCh chan struct{} // blocks until all headers are sampled
}

func initSamplingState(samplingRangeSize uint64, c checkpoint) coordinatorState {
	st := coordinatorState{
		rangeSize:     samplingRangeSize,
		inProgress:    make(map[int]func() workerState),
		failed:        c.Failed,
		next:          c.MinSampled,
		maxKnown:      c.MaxKnown,
		catchUpDoneCh: make(chan struct{}),
	}

	if st.failed == nil {
		st.failed = make(map[uint64]int)
	}

	st.priority = make([]job, 0, len(c.Failed)+len(c.Workers))
	// put failed into priority to retry them on restart
	for h := range c.Failed {
		st.priority = append(st.priority, st.newJob(h, h, true))
	}

	// put last inProgress into priority
	for _, wk := range c.Workers {
		st.priority = append(st.priority, st.newJob(wk.From, wk.To, true))
	}

	if c.MinSampled == 0 {
		c.MinSampled = 1
	}
	return st
}

func (s *coordinatorState) handleResult(res result) {
	delete(s.inProgress, res.id)

	failedFromWorker := make(map[uint64]bool)
	for _, h := range res.failed {
		failedFromWorker[h] = true
	}

	// update coordinator failed counter
	for h := res.from; h <= res.to; h++ {
		if failedFromWorker[h] {
			// failed in worker, increase failed counter
			s.failed[h]++
			continue
		}
		delete(s.failed, h)
	}
	s.checkDone()
}

func (s *coordinatorState) updateMaxKnown(last uint64) {
	// seen this header before
	if last <= s.maxKnown {
		return
	}

	if s.maxKnown == 1 {
		log.Infow("found first header, starting sampling")
	}

	// add most recent headers into priority queue
	for from := s.maxKnown + 1; from <= last; from += s.rangeSize {
		s.priority = append(s.priority, s.newJob(from, last, true))
	}

	log.Debug("added recent headers to DASer priority queue ", "from_height", s.maxKnown, "to_height", last)
	s.maxKnown = last
	s.checkDone()
	return
}

// nextJob will return header height to be processed and done flog if there is none
func (s *coordinatorState) nextJob() (next job, found bool) {
	// all headers were sent to workers.
	if s.next > s.maxKnown {
		return job{}, false
	}

	// try to take from priority first
	if next, found := s.nextFromPriority(); found {
		return next, found
	}

	j := s.newJob(s.next, s.maxKnown, false)

	s.next += s.rangeSize
	if s.next > s.maxKnown {
		s.next = s.maxKnown + 1
	}

	return j, true
}

func (s *coordinatorState) nextFromPriority() (job, bool) {
	for len(s.priority) > 0 {
		next := s.priority[len(s.priority)-1]

		// skip job if already processed
		if next.to <= s.next {
			s.priority = s.priority[:len(s.priority)-1]
			continue
		}

		// cut job iа partly processed
		if next.from <= s.next {
			next.from = s.next
		}

		s.priority = s.priority[:len(s.priority)-1]
		return next, true
	}
	return job{}, false
}

func (s *coordinatorState) putInProgress(jobID int, getState func() workerState) {
	s.inProgress[jobID] = getState
}

func (s *coordinatorState) newJob(from, max uint64, fromPriority bool) job {
	s.nextJobID++
	to := from + s.rangeSize - 1
	if to > max {
		to = max
	}
	return job{
		id:         s.nextJobID,
		from:       from,
		to:         to,
		isPriority: fromPriority,
	}
}

func (s *coordinatorState) stats() SamplingStats {
	workers := make([]WorkerStats, 0, len(s.inProgress))
	minSampled := s.next - 1
	failed := make(map[uint64]int)

	// gather worker SamplingStats
	for _, getStats := range s.inProgress {
		wstats := getStats()
		var errMsg string
		if wstats.Err != nil {
			errMsg = wstats.Err.Error()
		}
		workers = append(workers, WorkerStats{
			Curr:   wstats.Curr,
			From:   wstats.From,
			To:     wstats.To,
			ErrMsg: errMsg,
		})

		for _, h := range wstats.failed {
			failed[h]++
			if h < minSampled {
				minSampled = h - 1
			}
		}

		if wstats.Curr < minSampled {
			minSampled = wstats.Curr - 1
		}
	}

	// set minSampled to minimum failed - 1
	for h, count := range s.failed {
		failed[h] += count
		if h < minSampled {
			minSampled = h - 1
		}
	}

	return SamplingStats{
		MinSampled:  minSampled,
		MaxKnown:    s.maxKnown,
		Failed:      failed,
		Workers:     workers,
		Concurrency: len(workers),
		CatchUpDone: s.catchUpDone,
	}
}

func (s *coordinatorState) checkDone() {
	if len(s.inProgress) == 0 && s.next > s.maxKnown {
		if !s.catchUpDone {
			close(s.catchUpDoneCh)
			s.catchUpDone = true
		}
		return
	}

	if s.catchUpDone {
		s.catchUpDoneCh = make(chan struct{})
		s.catchUpDone = false
	}
}

// waitCatchUp waits for sampling process to finish catchup
func (s *coordinatorState) waitCatchUp(ctx context.Context) error {
	select {
	case <-s.catchUpDoneCh:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
