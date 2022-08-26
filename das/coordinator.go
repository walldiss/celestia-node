package das

import (
	"context"
	"sync"
)

// samplingCoordinator runs and coordinates sampling workers and updates current sampling state
type samplingCoordinator struct {
	concurrencyLimit int
	fetchFn          fetchFn
	store            checkpointStore

	state coordinatorState

	resultCh chan result // fan-in sampling results from worker to coordinator
	updMaxCh chan uint64 // signals to update max known header height
	waitCh   chan func() // blocks coordinator for external access to state

	workersWg       sync.WaitGroup
	workersDone     done
	coordinatorDone done
}

type result struct {
	job
	failed []uint64
	err    error
}

func newSamplingCoordinator(
	concurrency int,
	fetch fetchFn,
	store checkpointStore) *samplingCoordinator {
	return &samplingCoordinator{
		concurrencyLimit: concurrency,
		fetchFn:          fetch,
		store:            store,
		resultCh:         make(chan result),
		updMaxCh:         make(chan uint64),
		waitCh:           make(chan func()),
		workersDone:      newDone("workers"),
		coordinatorDone:  newDone("sampling coordinator"),
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
			sm.state.updateMaxKnown(max)
		case res := <-sm.resultCh:
			sm.state.handleResult(res)
		case wait := <-sm.waitCh:
			wait()
		case <-ctx.Done():
			sm.coordinatorDone.Done()
			sm.workersWg.Wait()
			sm.workersDone.Done()
			return
		}
	}
}

// runWorker runs job in separate worker go-routine
func (sm *samplingCoordinator) runWorker(ctx context.Context, j job) {
	w := newWorker(sm.resultCh, sm.fetchFn)
	sm.state.putInProgress(j.id, w.getState)

	// launch worker go-routine
	sm.workersWg.Add(1)
	go func() {
		defer sm.workersWg.Done()
		w.run(ctx, j)
	}()
}

// finished stores last sampling state to internal storage and waits for all routines to finish
func (sm *samplingCoordinator) finished(ctx context.Context) error {
	// wait for coordinator to exit and store state
	if err := sm.coordinatorDone.wait(ctx); err != nil {
		return err
	}
	sm.store.store(ctx, newCheckpoint(sm.state.stats()))

	// wait for all worker routines to finish
	if err := sm.workersDone.wait(ctx); err != nil {
		return err
	}
	sm.store.store(ctx, newCheckpoint(sm.state.stats()))
	return nil
}

// listen notifies coordinator about new header height has been found
func (sm *samplingCoordinator) listen(ctx context.Context, height uint64) {
	select {
	case sm.updMaxCh <- height:
	case <-ctx.Done():
	}
}

// listens for new headers to keep maxHeader up-to-date
func (sm *samplingCoordinator) getStats(ctx context.Context) (SamplingStats, error) {
	var wg sync.WaitGroup
	wg.Add(1)
	defer wg.Done()

	select {
	case sm.waitCh <- wg.Wait:
	case <-ctx.Done():
		return SamplingStats{}, ctx.Err()
	}

	return sm.state.stats(), nil
}

func (sm *samplingCoordinator) getCheckpoint(ctx context.Context) (checkpoint, error) {
	stats, err := sm.getStats(ctx)
	if err != nil {
		return checkpoint{}, err
	}
	return newCheckpoint(stats), nil
}

func (sm *samplingCoordinator) concurrencyLimitReached() bool {
	return len(sm.state.inProgress) >= sm.concurrencyLimit
}
