package das

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/celestiaorg/celestia-node/header"

	"go.uber.org/multierr"
)

type worker struct {
	lock  sync.Mutex
	state workerState
}

// workerState contains important information about the state of a
// current sampling routine.
type workerState struct {
	job

	Curr   uint64 `json:"current"`
	Err    error  `json:"error,omitempty"`
	failed []uint64
}

type job struct {
	id   int
	From uint64 `json:"from"`
	To   uint64 `json:"to"`
}

func (w *worker) run(
	ctx context.Context,
	getter header.Getter,
	sample sampleFn,
	resultCh chan<- result) {
	jobStart := time.Now()
	log.Infow("start sampling worker", "from", w.state.From, "to", w.state.To)

	for curr := w.state.From; curr <= w.state.To; curr++ {
		// TODO: get headers in batches
		h, err := getter.GetByHeight(ctx, curr)
		if err == nil {
			err = sample(ctx, h)
		}

		if errors.Is(err, context.Canceled) {
			// sampling worker will resume upon restart
			break
		}
		w.setResult(curr, err)
	}

	if w.state.Curr > w.state.From {
		jobTime := time.Since(jobStart)
		log.Infow("sampled headers", "from", w.state.From, "to", w.state.Curr,
			"finished (s)", jobTime.Seconds())
	}

	select {
	case resultCh <- result{
		job:    w.state.job,
		failed: w.state.failed,
		err:    w.state.Err}:
	case <-ctx.Done():
	}
}

func newWorker(j job) worker {
	return worker{
		state: workerState{
			job:    j,
			Curr:   j.From,
			failed: make([]uint64, 0),
		},
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
