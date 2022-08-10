package das

import "sort"

// represents current state of sampling
type state struct {
	priority   []uint64        // list of headers heights that will be sampled with higher priority
	inProgress map[uint64]bool // keeps track of inProgress item with priority flag stored as value

	priorityBusy bool // semaphore to limit only one priority to be processed at time

	maxKnown   uint64 // max known header height
	sampleFrom uint64 // all header before sampleFrom were sampled, except ones that tracked in failed
	sampleTo   uint64 // height where parallel sampling will stop and will proceed only from priority

	failed map[uint64]int
}

func (s state) handleResult(res result) {
	if s.inProgress[res.height] {
		s.priorityBusy = false
	}

	delete(s.inProgress, res.height)
	//TODO: no handling, just handleResult for now
	// put item back to priority for handleResult
	if res.failed {
		s.failed[res.height]++
		return
	}
	delete(s.failed, res.height)
}

func (s *state) updateMaxKnown(last uint64) bool {
	// seen this header before
	if last == s.maxKnown {
		return false
	}

	if s.maxKnown == 0 {
		s.sampleTo = last
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

	if s.sampleFrom < s.sampleTo {
		next = s.sampleFrom
		s.inProgress[next] = false
		s.sampleFrom++
		return next, false
	}

	return 0, true
}

func (s *state) checkPoint() checkPoint {

	// queue = append(queue, s.priority...) no need to persist priority over restart because we set sampleTo

	// put all in progress items to priority to resume from current state
	for h := range s.inProgress {
		// reset sampleFrom to min inProgress item that wasn't failed
		if _, ok := s.failed[h]; !ok && h < s.sampleFrom {
			s.sampleFrom = h
		}
	}

	return checkPoint{
		MinSampledHeight: s.sampleFrom,
		MaxKnownHeight:   s.maxKnown,
		Skipped:          s.failed,
	}
}

func (c checkPoint) samplingState() *state {

	// put failed to priority to retry after restart
	priority := make([]uint64, 0, len(c.Skipped))
	for h := range c.Skipped {
		priority = append(priority, h)
	}
	//sort in Desc order
	sort.Slice(priority, func(i, j int) bool { return priority[i] < priority[j] })
	return &state{
		priority:   priority,
		inProgress: make(map[uint64]bool),
		maxKnown:   c.MaxKnownHeight,
		sampleFrom: c.MinSampledHeight,
		sampleTo:   c.MaxKnownHeight,
		failed:     c.Skipped,
	}
}
