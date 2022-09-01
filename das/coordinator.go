package das

import (
	"context"
	"sync"
)

// samplingCoordinator runs and coordinates sampling workers and updates current sampling state
type samplingCoordinator struct {
	concurrencyLimit int
	sampleFn         sampleFn

	state coordinatorState

	resultCh chan result // fan-in sampling results from worker to coordinator
	updMaxCh chan uint64 // signals to update max known header height
	waitCh   chan func() // blocks coordinator for external access to state

	workersWg sync.WaitGroup
	done
}

type result struct {
	job
	failed []uint64
	err    error
}

func newSamplingCoordinator(
	concurrency int,
	sample sampleFn) *samplingCoordinator {
	return &samplingCoordinator{
		concurrencyLimit: concurrency,
		sampleFn:         sample,
		resultCh:         make(chan result),
		updMaxCh:         make(chan uint64),
		waitCh:           make(chan func()),
		done:             newDone("sampling coordinator"),
	}
}

func (sm *samplingCoordinator) run(ctx context.Context) {
	for {
		for !sm.concurrencyLimitReached() {
			next, found := sm.state.nextJob()
			if !found {
				break
			}
			sm.runWorker(ctx, next)
		}

		select {
		case max := <-sm.updMaxCh:
			sm.state.updateHead(max)
		case res := <-sm.resultCh:
			sm.state.handleResult(res)
		case wait := <-sm.waitCh:
			wait()
		case <-ctx.Done():
			sm.workersWg.Wait()
			sm.indicateDone()
			return
		}
	}
}

// runWorker runs job in separate worker go-routine
func (sm *samplingCoordinator) runWorker(ctx context.Context, j job) {
	w := worker{}
	sm.state.putInProgress(j.id, w.getState)

	// launch worker go-routine
	sm.workersWg.Add(1)
	go func() {
		defer sm.workersWg.Done()
		w.run(ctx, j, sm.sampleFn, sm.resultCh)
	}()
}

// listen notifies coordinator about new header height has been found
func (sm *samplingCoordinator) listen(ctx context.Context, height uint64) {
	select {
	case sm.updMaxCh <- height:
	case <-ctx.Done():
	}
}

// stats pause coordinator to get stats in concurrently safe manner
func (sm *samplingCoordinator) stats(ctx context.Context) (SamplingStats, error) {
	var wg sync.WaitGroup
	wg.Add(1)
	defer wg.Done()

	select {
	case sm.waitCh <- wg.Wait:
	case <-ctx.Done():
		return SamplingStats{}, ctx.Err()
	}

	return sm.state.unsafeStats(), nil
}

func (sm *samplingCoordinator) getCheckpoint(ctx context.Context) (checkpoint, error) {
	stats, err := sm.stats(ctx)
	if err != nil {
		return checkpoint{}, err
	}
	return newCheckpoint(stats), nil
}

func (sm *samplingCoordinator) concurrencyLimitReached() bool {
	return len(sm.state.inProgress) >= sm.concurrencyLimit
}
