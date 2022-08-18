package das

// represents current state of sampling
type state struct {
	priority   []uint64        // list of headers heights that will be sampled with higher priority
	inProgress map[uint64]bool // keeps track of inProgress item with priority flag stored as value
	failed     map[uint64]int  // stores heights of failed headers with amount of attempt

	priorityBusy bool // semaphore to allow only one priority item to be processed at time

	maxKnown   uint64 // max known header height
	next       uint64 // all header before next were sampled, except ones that tracked in failed
	minSampled uint64 // tracks min sampled height
}

func (s *state) handleResult(res result) {
	if s.inProgress[res.height] {
		s.priorityBusy = false
	}

	//TODO: no handling, no retry. just store retry stats for now
	if res.err != nil {
		s.failed[res.height]++
		return
	}

	delete(s.inProgress, res.height)
	delete(s.failed, res.height)

	// set minSampled based on min non-failed done header
	for s.minSampled < s.next-1 {
		if _, failed := s.failed[s.minSampled]; failed {
			break
		}
		if _, busy := s.inProgress[s.minSampled]; busy {
			break
		}
		s.minSampled++
	}
}

func (s *state) updateMaxKnown(last uint64) bool {
	// seen this header before
	if last == s.maxKnown {
		return false
	}

	if s.maxKnown == 1 {
		log.Infow("found first header, starting sampling")
	}

	// add most recent headers that fit into priority queue without overflowing it
	from := s.maxKnown
	spaceInQueue := priorityBufferSize - uint64(len(s.priority))
	if last-from > spaceInQueue {
		from = last - spaceInQueue
	}
	// put recent heights with the highest priority
	for h := from; h <= last; h++ {
		s.priority = append(s.priority, h)
	}

	log.Infow("added recent headers from DASer priority queue ", "from_height", from, "to_height", last)
	s.maxKnown = last
	return true
}

// nextHeight will return header height to be processed and done flog if there is none
func (s *state) nextHeight() (next uint64, done bool) {
	if !s.priorityBusy {
		// select next height for priority worker
		for len(s.priority) > 0 {
			next = s.priority[len(s.priority)-1]

			// skip all items lower than s.next to avoid double sampling,
			//  since they were already processed by parallel workers
			if next <= s.next {
				s.priority = s.priority[:len(s.priority)-1]
				continue
			}

			return next, false
		}
	}

	if s.next <= s.maxKnown {
		return s.next, false
	}

	return 0, true
}

func (s *state) setBusy(next uint64) {
	var fromPriority bool
	if len(s.priority) > 0 {
		if next == s.priority[len(s.priority)-1] {
			s.priority = s.priority[:len(s.priority)-1]
			s.priorityBusy = true
			fromPriority = true
		}
	}

	if s.next == next {
		s.next++
	}

	s.inProgress[next] = fromPriority
}

func (s *state) checkPoint() checkpoint {
	return checkpoint{
		MinSampled: s.minSampled,
		MaxKnown:   s.maxKnown,
		Skipped:    s.failed,
	}
}

func (c checkpoint) samplingState() *state {
	failed := c.Skipped
	if failed == nil {
		failed = make(map[uint64]int)
	}

	// start from 1
	if c.MinSampled == 0 {
		c.MinSampled = 1
	}
	return &state{
		inProgress: make(map[uint64]bool),
		maxKnown:   c.MaxKnown,
		next:       c.MinSampled,
		minSampled: c.MinSampled,
		failed:     failed,
	}
}
