package das

import (
	"context"
)

// coordinatorState represents current state of sampling
type coordinatorState struct {
	rangeSize uint64

	priority   []job                      // list of headers heights that will be sampled with higher priority
	inProgress map[int]func() workerState // keeps track of running workers
	failed     map[uint64]int             // stores heights of failed headers with amount of attempt as value

	nextJobID   int
	next        uint64 // all headers before next were sent to workers
	networkHead uint64

	catchUpDone   bool          // indicates if all headers are sampled
	catchUpDoneCh chan struct{} // blocks until all headers are sampled
}

// initState initiates state for samplingCoordinator
func initState(samplingRangeSize uint64) coordinatorState {
	return coordinatorState{
		rangeSize:     samplingRangeSize,
		priority:      make([]job, 0),
		inProgress:    make(map[int]func() workerState),
		failed:        make(map[uint64]int),
		nextJobID:     0,
		next:          genesisHeight,
		networkHead:   genesisHeight,
		catchUpDone:   false,
		catchUpDoneCh: make(chan struct{}),
	}
}

func (s *coordinatorState) resumeFromCheckpoint(c checkpoint) {
	s.next = c.SampleFrom
	s.networkHead = c.NetworkHead
	// put failed into priority to retry them on restart
	for h, count := range c.Failed {
		s.failed[h] = count
		s.priority = append(s.priority, s.newJob(h, h))
	}
}

func (s *coordinatorState) handleResult(res result) {
	delete(s.inProgress, res.id)

	failedFromWorker := make(map[uint64]bool)
	for _, h := range res.failed {
		failedFromWorker[h] = true
	}

	// update coordinator failed counter
	for h := res.From; h <= res.To; h++ {
		if failedFromWorker[h] {
			// failed in worker, increase failed counter
			s.failed[h]++
			continue
		}
		delete(s.failed, h)
	}
	s.checkDone()
}

func (s *coordinatorState) updateHead(last uint64) bool {
	// seen this header before
	if last <= s.networkHead {
		log.Warnf("received head height: %v, which is lower or the same as previously known: %v", last, s.networkHead)
		return false
	}

	if s.networkHead == genesisHeight {
		s.networkHead = last
		log.Infow("found first header, starting sampling")
		return true
	}

	// add most recent headers into priority queue
	from := s.networkHead + 1
	for from <= last && len(s.priority) < priorityCap {
		s.priority = append(s.priority, s.newJob(from, last))
		from += s.rangeSize
	}

	log.Debugw("added recent headers to DASer priority queue", "from_height", s.networkHead, "to_height", last)
	s.networkHead = last
	s.checkDone()
	return true
}

// nextJob will return header height to be processed and done flag if there is none
func (s *coordinatorState) nextJob() (next job, found bool) {
	// all headers were sent to workers.
	if s.next > s.networkHead {
		return job{}, false
	}

	// try to take from priority first
	if next, found := s.nextFromPriority(); found {
		return next, found
	}

	j := s.newJob(s.next, s.networkHead)

	s.next += s.rangeSize
	if s.next > s.networkHead {
		s.next = s.networkHead + 1
	}

	return j, true
}

func (s *coordinatorState) nextFromPriority() (job, bool) {
	for len(s.priority) > 0 {
		next := s.priority[len(s.priority)-1]
		s.priority = s.priority[:len(s.priority)-1]

		// skip the job, if it will be processed next without a priority
		if next.From == s.next {
			continue
		}

		return next, true
	}
	return job{}, false
}

func (s *coordinatorState) putInProgress(jobID int, getState func() workerState) {
	s.inProgress[jobID] = getState
}

func (s *coordinatorState) newJob(from, max uint64) job {
	s.nextJobID++
	to := from + s.rangeSize - 1
	if to > max {
		to = max
	}
	return job{
		id:   s.nextJobID,
		From: from,
		To:   to,
	}
}

// unsafeStats collects coordinator stats without thread-safety
func (s *coordinatorState) unsafeStats() SamplingStats {
	workers := make([]WorkerStats, 0, len(s.inProgress))
	lowestFailedOrInProgress := s.next
	failed := make(map[uint64]int)

	// gather worker stats
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
			if h < lowestFailedOrInProgress {
				lowestFailedOrInProgress = h
			}
		}

		if wstats.Curr < lowestFailedOrInProgress {
			lowestFailedOrInProgress = wstats.Curr
		}
	}

	// set lowestFailedOrInProgress to minimum failed - 1
	for h, count := range s.failed {
		failed[h] += count
		if h < lowestFailedOrInProgress {
			lowestFailedOrInProgress = h
		}
	}

	return SamplingStats{
		HeadOfSampledChain: lowestFailedOrInProgress - 1,
		HeadOfCatchup:      s.next - 1,
		NetworkHead:        s.networkHead,
		Failed:             failed,
		Workers:            workers,
		Concurrency:        len(workers),
		CatchUpDone:        s.catchUpDone,
		IsRunning:          len(workers) > 0 || s.catchUpDone,
	}
}

func (s *coordinatorState) checkDone() {
	if len(s.inProgress) == 0 && len(s.priority) == 0 && s.next > s.networkHead {
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

// waitCatchUp waits for sampling process to indicateDone catchup
func (s *coordinatorState) waitCatchUp(ctx context.Context) error {
	select {
	case <-s.catchUpDoneCh:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
