package das

import (
	"context"
	"fmt"
	"sync"

	"go.uber.org/multierr"
)

type worker struct {
	lock  sync.Mutex
	state workerState
}

// workerState contains important information about the state of a
// current sampling routine.
type workerState struct {
	Curr uint64 `json:"curr"`
	From uint64 `json:"from"`
	To   uint64 `json:"to"`

	failed []uint64
	Err    error `json:"error,omitempty"`
}

type job struct {
	id         int
	from, to   uint64
	isPriority bool
}

func (w *worker) run(
	ctx context.Context,
	j job,
	sample func(context.Context, uint64) error,
	resultCh chan<- result) {
	w.setStateFromJob(j)

	for curr := j.from; curr <= j.to; curr++ {
		err := sample(ctx, curr)
		w.setResult(curr, err)

		select {
		case <-ctx.Done():
			return
		default:
		}
	}

	select {
	case resultCh <- result{
		job:    j,
		failed: w.state.failed,
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
		failed: make([]uint64, 0),
	}
}

func (w *worker) setResult(curr uint64, err error) {
	w.lock.Lock()
	defer w.lock.Unlock()
	if err != nil {
		w.state.failed = append(w.state.failed, curr)
		w.state.Err = multierr.Append(w.state.Err, fmt.Errorf("height: %v, Err: %w", curr, err))
	}
	w.state.Curr = curr
}

func (w *worker) getState() workerState {
	w.lock.Lock()
	defer w.lock.Unlock()
	return w.state
}
