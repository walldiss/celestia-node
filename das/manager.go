package das

import (
	"context"
	"fmt"
	"sync"

	"github.com/hashicorp/go-multierror"
)

// samplingManager coordinates sampling workers and stores current state
type samplingManager struct {
	concurrency int
	fetchFn     fetchFn
	storeState  storeFn

	state state

	resultCh chan result // fan-in sampling results from worker to coordinator
	updMaxCh chan uint64 // signals to update max known header height
	stateCh  chan state  // communicates with backgroundStore routine

	workersWg           sync.WaitGroup
	workersDone         chan struct{}
	coordinatorDone     chan struct{}
	backgroundStoreDone chan struct{}
}

type fetchFn func(context.Context, uint64) error
type storeFn func(context.Context, checkpoint)

type result struct {
	job
	failed []uint64
	err    *multierror.Error
}

func newSamplingManager(
	concurrency int,
	fetch fetchFn,
	storeFn storeFn) *samplingManager {
	return &samplingManager{
		concurrency:         concurrency,
		fetchFn:             fetch,
		storeState:          storeFn,
		state:               state{},
		resultCh:            make(chan result),
		updMaxCh:            make(chan uint64),
		stateCh:             make(chan state),
		workersDone:         make(chan struct{}),
		coordinatorDone:     make(chan struct{}),
		backgroundStoreDone: make(chan struct{}),
	}
}

func (sm *samplingManager) runCoordinator(ctx context.Context) {
	var done bool

	for {
		// launch workers if there are jobs available and concurrency limit is not reached
		for !done && len(sm.state.inProgress) < sm.concurrency {
			done = sm.runWorker(ctx)
		}

		select {
		case max := <-sm.updMaxCh:
			if sm.state.updateMaxKnown(max) && done {
				// unlock workers for newly received headers
				done = false
				sm.state.checkDone()
			}
		case res := <-sm.resultCh:
			sm.state.handleResult(res)
			sm.state.checkDone()
		case sm.stateCh <- sm.state:
		case <-ctx.Done():
			close(sm.coordinatorDone)
			sm.workersWg.Wait()
			close(sm.workersDone)
			return
		}
	}
}

func (sm *samplingManager) runWorker(ctx context.Context) (done bool) {
	next, done := sm.state.nextJob()
	if done {
		// if nothing to sample, no need to start worker
		return
	}

	w := newWorker(sm.resultCh, sm.fetchFn)
	sm.state.putInProgress(next.id, w)

	sm.workersWg.Add(1)
	go func() {
		defer sm.workersWg.Done()
		w.run(ctx, next)
	}()
	return false
}

func (sm *samplingManager) finished(ctx context.Context) error {
	// wait for coordinator to exit and store state
	select {
	case <-sm.coordinatorDone:
		sm.storeState(ctx, sm.state.toCheckPoint())
	case <-ctx.Done():
		return fmt.Errorf("coordinator stuck: %w", ctx.Err())
	}

	// wait for all worker routines to finish
	select {
	case <-sm.workersDone:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("workers stuck: %w", ctx.Err())
	}
}

// listens for new headers to keep maxHeader up-to-date
func (sm *samplingManager) listen(ctx context.Context, height uint64) {
	select {
	case sm.updMaxCh <- height:
	case <-ctx.Done():
	}
}

// listens for new headers to keep maxHeader up-to-date
func (sm *samplingManager) getStats(ctx context.Context) (stats, error) {
	select {
	case st := <-sm.stateCh:
		cp := st.toCheckPoint()
		return stats{cp, st.catchUpDone > 0}, nil
	case <-ctx.Done():
		return stats{}, ctx.Err()
	}
}

// TODO: rework backgroundStore
//type backgroundStore struct {
//	storeFn
//	storeInterval time.Duration
//	getStats      func(ctx context.Context) stats
//	done          chan struct{}
//}

//
//func (sm *backgroundStore) finished(ctx context.Context) error {
//	// wait for background store to exit
//	select {
//	case <-sm.done:
//	case <-ctx.Done():
//		return fmt.Errorf("background store stuck: %w", ctx.Err())
//	}
//}
//
//// BackgroundStore periodically stores state to keep stored version up-to-date in case force quit happens
//func (sm *backgroundStore) runBackgroundStore(ctx context.Context) {
//	defer close(sm.done)
//
//	if sm.storeInterval == 0 {
//		// run store routine only when storeInterval is specified
//		return
//	}
//	storeTicker := time.NewTicker(sm.storeInterval)
//
//	var prevMinSampled uint64
//	for {
//		// blocked by ticker to perform store only once in period of time
//		select {
//		case <-storeTicker.C:
//		case <-ctx.Done():
//			return
//		}
//
//		st := sm.toCheckPoint(ctx)
//		st.toCheckPoint
//		select {
//		case s := <-sm.toCheckPoint:
//			// store only if minSampled is updated
//			if s.minSampled != prevMinSampled {
//				sm.storeState(ctx, s)
//				prevMinSampled = s.minSampled
//			}
//		case <-ctx.Done():
//			return
//		}
//	}
//}
