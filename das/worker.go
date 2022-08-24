package das

import (
	"context"
	"fmt"
	"sync"

	"github.com/hashicorp/go-multierror"
)

type job struct {
	id           int
	from, to     uint64
	fromPriority bool
}

func (s *state) newJob(from, max uint64, fromPriority bool) job {
	s.nextJobID++
	to := from + s.rangeSize - 1
	if to > max {
		to = max
	}
	return job{
		id:           s.nextJobID,
		from:         from,
		to:           to,
		fromPriority: fromPriority,
	}
}

type worker struct {
	lock     sync.Mutex
	state    workerState
	resultCh chan<- result
	fetch    func(ctx2 context.Context, uint642 uint64) error
}

type workerState struct {
	From   uint64            `json:"from"`
	To     uint64            `json:"to"`
	Curr   uint64            `json:"curr"`
	Failed []uint64          `json:"failed"`
	Err    *multierror.Error `json:"Err"`
}

func newWorker(
	resultCh chan<- result,
	fetch func(context.Context, uint64) error) *worker {
	return &worker{
		resultCh: resultCh,
		fetch:    fetch,
	}
}

func (w *worker) run(ctx context.Context, j job) {
	w.setStateFromJob(j)

	for curr := j.from; curr <= j.to; curr++ {
		w.setResult(curr, w.fetch(ctx, curr))

		select {
		case <-ctx.Done():
			return
		default:
		}
	}

	select {
	case w.resultCh <- result{
		job:    j,
		failed: w.state.Failed,
		err:    w.state.Err,
	}:
	case <-ctx.Done():
	}
}

func (w *worker) setStateFromJob(j job) {
	w.lock.Lock()
	defer w.lock.Unlock()
	w.state = workerState{
		From:   j.from,
		To:     j.to,
		Curr:   j.from,
		Failed: make([]uint64, 0),
	}
}

func (w *worker) setResult(curr uint64, err error) {
	w.lock.Lock()
	defer w.lock.Unlock()
	if err != nil {
		w.state.Failed = append(w.state.Failed, curr)
		w.state.Err = multierror.Append(w.state.Err, fmt.Errorf("height: %v, Err: %w", curr, err))
	}
	w.state.Curr = curr
}

func (w *worker) getState() workerState {
	w.lock.Lock()
	defer w.lock.Unlock()
	return w.state
}
