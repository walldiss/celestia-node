package das

import (
	"context"
	"sync/atomic"
)

// represents current state of sampling
type state struct {
	rangeSize uint64

	priority   []job                      // list of headers heights that will be sampled with higher priority
	inProgress map[int]func() workerState // keeps track of inProgress items
	failed     map[uint64]int             // stores heights of failed headers with amount of attempt as value

	nextJobID int
	next      uint64 // all headers before next were sent to workers
	maxKnown  uint64 // max known header height

	catchUpDone   int32         // indicates if all headers are sampled
	catchUpDoneCh chan struct{} // blocks until all headers are sampled
}

func initSamplingState(samplingRangeSize uint64, c checkpoint) state {
	st := state{
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

	st.priority = make([]job, 0, len(st.failed)+len(c.Workers))
	// put failed into priority
	for h := range st.failed {
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

func (s *state) handleResult(res result) {
	delete(s.inProgress, res.id)

	failedFromWorker := make(map[uint64]bool)
	for _, h := range res.failed {
		failedFromWorker[h] = true
	}

	// update failed state
	for h := res.from; h <= res.to; h++ {
		if failedFromWorker[h] {
			// increase failed counter
			s.failed[h]++
			continue
		}
		// success, remove from failed map
		delete(s.failed, h)
	}

	s.checkDone()
}

func (s *state) updateMaxKnown(last uint64) bool {
	// seen this header before
	if last <= s.maxKnown {
		return false
	}

	if s.maxKnown == 1 {
		log.Infow("found first header, starting sampling")
	}

	// add most recent headers into priority queue splitting jobs by rangeSize

	for from := s.maxKnown + 1; from <= last; from += s.rangeSize {
		s.priority = append(s.priority, s.newJob(from, last, true))
	}

	log.Debug("added recent headers to DASer priority queue ", "from_height", s.maxKnown, "to_height", last)
	s.maxKnown = last
	return true
}

// nextJob will return header height to be processed and done flog if there is none
func (s *state) nextJob() (next job, done bool) {
	// all headers were sent to workers.
	if s.next > s.maxKnown {
		return job{}, true
	}

	// try to take from priority first
	if next, found := s.nextFromPriority(); found {
		return next, false
	}

	j := s.newJob(s.next, s.maxKnown, false)

	s.next += s.rangeSize
	if s.next > s.maxKnown {
		s.next = s.maxKnown + 1
	}

	return j, false
}

func (s *state) nextFromPriority() (job, bool) {
	for len(s.priority) > 0 {
		next := s.priority[len(s.priority)-1]

		// skip job if already processed
		if next.to <= s.next {
			s.priority = s.priority[:len(s.priority)-1]
			continue
		}

		// cut job is partly processed
		if next.from <= s.next {
			next.from = s.next
		}

		s.priority = s.priority[:len(s.priority)-1]
		return next, true
	}
	return job{}, false
}

func (s *state) putInProgress(jobID int, w *worker) {
	s.inProgress[jobID] = w.getState
}

func (s *state) toCheckPoint() checkpoint {
	workers := make([]workerState, 0, len(s.inProgress))
	minSampled := s.next - 1

	// gather worker stats
	for _, getStats := range s.inProgress {
		wstats := getStats()
		workers = append(workers, wstats)

		if len(wstats.Failed) > 0 {
			// set minSampled to minimum failed - 1
			if min := wstats.Failed[0] - 1; min < minSampled {
				minSampled = min
			}
			continue
		}

		if wstats.Curr < minSampled {
			minSampled = wstats.Curr - 1
		}
	}

	// check previous jobs results. set minSampled to minimum failed - 1
	for failed := range s.failed {
		if min := failed - 1; min < minSampled {
			minSampled = min
		}
	}

	return checkpoint{
		MinSampled: minSampled,
		MaxKnown:   s.maxKnown,
		Failed:     s.failed,
		Workers:    workers,
	}
}

func (s *state) checkDone() {
	if len(s.inProgress) == 0 && s.next > s.maxKnown {
		if atomic.CompareAndSwapInt32(&s.catchUpDone, 0, 1) {
			close(s.catchUpDoneCh)
		}
		return
	}

	if atomic.CompareAndSwapInt32(&s.catchUpDone, 1, 0) {
		s.catchUpDoneCh = make(chan struct{})
	}
}

// waitCatchUp waits for sampling process to finish catchup
func (s *state) waitCatchUp(ctx context.Context) error {
	select {
	case <-s.catchUpDoneCh:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
