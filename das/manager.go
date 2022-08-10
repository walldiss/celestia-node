package das

import (
	"context"
	"sync"
)

type samplingManager struct {
	concurrency int
	fetchFn     func(context.Context, uint64) error
	state       *state

	wg              sync.WaitGroup
	jobsCh          chan uint64
	resultCh        chan result
	discoveryCh     chan uint64
	coordinatorDone chan struct{}
}

func newSamplingManager(concurrency int, fetch func(context.Context, uint64) error) *samplingManager {
	return &samplingManager{
		concurrency:     concurrency,
		fetchFn:         fetch,
		jobsCh:          make(chan uint64, 1),
		discoveryCh:     make(chan uint64),
		resultCh:        make(chan result),
		coordinatorDone: make(chan struct{}),
	}
}

func (sm *samplingManager) run(ctx context.Context, point checkPoint) {
	sm.state = point.samplingState()

	go sm.runCoordinator(ctx)

	for i := 0; i < sm.concurrency; i++ {
		go func(num int) {
			sm.wg.Add(1)
			defer sm.wg.Done()
			runWorker(ctx, sm.jobsCh, sm.resultCh, sm.fetchFn, num)
		}(i)
	}
}

func (sm *samplingManager) runCoordinator(ctx context.Context) {
	jobsCh := sm.jobsCh
	noop := make(chan uint64)
	next, done := sm.state.nextHeight()

	for {
		// sm.updateSampleStats() TODO:implement me

		// if nothing to sample, don't send job to workers
		if done {
			jobsCh = noop
		}

		select {
		case jobsCh <- next:
			next, done = sm.state.nextHeight()
		case last := <-sm.discoveryCh:
			// if jobsCh was locked and discovery found new headers to sample unblock it
			if sm.state.updateMaxKnown(last) && done {
				jobsCh = sm.jobsCh
				next, done = sm.state.nextHeight()
			}
		case res := <-sm.resultCh:
			sm.state.handleResult(res)
		case <-ctx.Done():
			close(sm.jobsCh)
			close(sm.coordinatorDone)
			return
		}
	}
}

func (sm *samplingManager) stop(ctx context.Context, storeSate func(context.Context, *state)) error {
	// wait coordinator to exit and store state
	select {
	case <-sm.coordinatorDone:
		storeSate(ctx, sm.state)
	case <-ctx.Done():
		return ctx.Err()
	}

	// wait for all worker routines to finish
	done := make(chan struct{})
	go func() {
		sm.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (sm *samplingManager) listen(ctx context.Context, height uint64) {
	select {
	case sm.discoveryCh <- height:
	case <-ctx.Done():
	}
}
