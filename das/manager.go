package das

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

type samplingManager struct {
	concurrency int
	fetchFn     fetchFn
	storeState  storeFn
	state       *state

	catchUpDone   int32         // indicates if all headers are sampled
	catchUpDoneCh chan struct{} // blocks until all headers are sampled

	workersWg     sync.WaitGroup
	jobsCh        chan uint64 // fan-out jobs to workers
	resultCh      chan result // fan-in sampling results from worker to coordinator
	discoveryCh   chan uint64 // receives all info about new headers discovery
	storeCh       chan state  // communicates with backgroundStore routine
	storeInterval time.Duration

	coordinatorDone chan struct{}
	cancel          context.CancelFunc
}

type fetchFn func(context.Context, uint64) error
type storeFn func(context.Context, state)

func newSamplingManager(concurrency int,
	bufferSize int,
	storeInterval time.Duration,
	fetch fetchFn,
	storeFn storeFn) *samplingManager {
	return &samplingManager{
		concurrency:     concurrency,
		fetchFn:         fetch,
		storeState:      storeFn,
		jobsCh:          make(chan uint64, bufferSize),
		resultCh:        make(chan result),
		discoveryCh:     make(chan uint64),
		storeCh:         make(chan state),
		storeInterval:   storeInterval,
		coordinatorDone: make(chan struct{}),
		catchUpDoneCh:   make(chan struct{}),
	}
}

func (sm *samplingManager) run(ctx context.Context, ch checkPoint) {
	ctx, sm.cancel = context.WithCancel(ctx)

	sm.state = ch.samplingState()

	go sm.runCoordinator(ctx)
	if sm.storeInterval > 0 {
		// run store routine only when storeInterval is specified
		go sm.runBackgroundStore(ctx, sm.storeInterval)
	}

	for i := 0; i < sm.concurrency; i++ {
		sm.workersWg.Add(1)
		go func(num int) {
			defer sm.workersWg.Done()
			runWorker(ctx, sm.jobsCh, sm.resultCh, sm.fetchFn, num)
		}(i)
	}
}

func (sm *samplingManager) runCoordinator(ctx context.Context) {
	jobsCh := sm.jobsCh
	noop := make(chan uint64)

	var next uint64
	var done bool
	for {
		sm.updateStats()

		if next, done = sm.state.nextHeight(); done {
			// if nothing to sample, don't send job to workers
			jobsCh = noop
		}

		select {
		case jobsCh <- next:
			sm.state.setBusy(next)
		case last := <-sm.discoveryCh:
			// if jobsCh was locked and discovery found new headers to sample unblock it
			if sm.state.updateMaxKnown(last) && done {
				jobsCh = sm.jobsCh
			}
		case res := <-sm.resultCh:
			sm.state.handleResult(res)
		case sm.storeCh <- *sm.state:
		case <-ctx.Done():
			close(sm.jobsCh)
			close(sm.coordinatorDone)
			return
		}
	}
}

func (sm *samplingManager) stop(ctx context.Context) error {
	sm.cancel()
	// wait for coordinator to exit and store state
	select {
	case <-sm.coordinatorDone:
		sm.storeState(ctx, *sm.state)
	case <-ctx.Done():
		return ctx.Err()
	}

	// wait for all worker routines to finish
	done := make(chan struct{})
	go func() {
		sm.workersWg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// BackgroundStore periodically stores state to keep stored version up-to-date in case force quit happens
func (sm *samplingManager) runBackgroundStore(ctx context.Context, interval time.Duration) {
	storeTicker := time.NewTicker(interval)

	for {
		// blocked by ticker to perform store only once in period of time
		select {
		case <-storeTicker.C:
		case <-ctx.Done():
			return
		}

		select {
		case s := <-sm.storeCh:
			sm.storeState(ctx, s)
		case <-ctx.Done():
			return
		}
	}
}

func (sm *samplingManager) updateStats() {
	if sm.state.minSampled == sm.state.maxKnown {
		if atomic.CompareAndSwapInt32(&sm.catchUpDone, 0, 1) {
			close(sm.catchUpDoneCh)
			return
		}
	}

	if atomic.CompareAndSwapInt32(&sm.catchUpDone, 1, 0) {
		sm.catchUpDoneCh = make(chan struct{})
	}
}

func (sm *samplingManager) waitCatchUp(ctx context.Context) error {
	select {
	case <-sm.catchUpDoneCh:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

func (sm *samplingManager) listen(ctx context.Context, height uint64) {
	select {
	case sm.discoveryCh <- height:
	case <-ctx.Done():
	}
}
