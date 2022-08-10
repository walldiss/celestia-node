package das

// represents current state of sampling
type state struct {
	priority   []uint64        // list of headers heights that will be sampled with higher priority
	inProgress map[uint64]bool // keeps track of inProgress item with priority flag stored as value
	failed     map[uint64]int

	priorityBusy bool // semaphore to limit only one priority to be processed at time

	maxKnown   uint64 // max known header height
	sampleFrom uint64 // all header before sampleFrom were sampled, except ones that tracked in failed
	minSampled uint64 // tracks min sampled height before which all
}

func (s state) handleResult(res result) {
	if s.inProgress[res.height] {
		s.priorityBusy = false
	}

	//TODO: no handling, no retry. just store retry stats for now
	if res.failed {
		s.failed[res.height]++
		return
	}

	// set minSampled based on min non-failed done header
	for s.minSampled < s.sampleFrom {
		if _, failed := s.failed[s.minSampled]; failed {
			break
		}
		if _, busy := s.inProgress[s.minSampled]; busy {
			break
		}
		s.minSampled++
	}
	delete(s.inProgress, res.height)
	delete(s.failed, res.height)
}

func (s *state) updateMaxKnown(last uint64) bool {
	// seen this header before
	if last == s.maxKnown {
		return false
	}

	if s.maxKnown == 0 {
		log.Infow("discovered first header, starting sampling")
	}

	// add most recent headers that fit into priority queue without overflowing it
	from := s.maxKnown
	spaceInQueue := priorityLimit - uint64(len(s.priority))
	if last-from > spaceInQueue {
		from = last - spaceInQueue
	}
	// put newly discovered heights with the highest priority
	for h := from; h <= last; h++ {
		s.priority = append(s.priority, h)
	}

	log.Infow("added recent headers to DASer priority queue ", "from_height", from, "to_height", last)
	s.maxKnown = last
	return true
}

// nextHeight will return header height to be processed and done flog if there is none
func (s *state) nextHeight() (next uint64, done bool) {
	if len(s.priority) > 0 && !s.priorityBusy {
		next = s.priority[len(s.priority)-1]
		s.priority = s.priority[:len(s.priority)-1]

		s.priorityBusy = true
		s.inProgress[next] = true
		return next, false
	}

	if s.sampleFrom < s.maxKnown {
		next = s.sampleFrom
		s.inProgress[next] = false
		s.sampleFrom++
		return next, false
	}

	return 0, true
}

func (s *state) checkPoint() checkPoint {
	return checkPoint{
		MinSampledHeight: s.minSampled,
		MaxKnownHeight:   s.maxKnown,
		Skipped:          s.failed,
	}
}

func (c checkPoint) samplingState() *state {
	return &state{
		inProgress: make(map[uint64]bool),
		maxKnown:   c.MaxKnownHeight,
		sampleFrom: c.MinSampledHeight,
		minSampled: c.MinSampledHeight,
		failed:     c.Skipped,
	}
}
