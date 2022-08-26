package das

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestManager(t *testing.T) {
	concurrency := 10
	samplingRange := uint64(10)
	maxKnown := uint64(500)
	sampleBefore := uint64(1)
	timeoutDelay := 5 * time.Second

	t.Run("test run", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), timeoutDelay)

		fetcher := newMockFetcher(sampleBefore, maxKnown)
		store := newExpectStore(t, fetcher.finalState)

		manager := newSamplingCoordinator(concurrency, fetcher.fetch, store)
		manager.state = initSamplingState(samplingRange, fetcher.checkpoint)
		go manager.run(ctx)

		// wait for manager to finish catchup
		assert.NoError(t, manager.state.waitCatchUp(ctx))
		assert.Emptyf(t, manager.state.failed, "failde list should be empty")

		// check if all jobs were fetched successfully
		select {
		case <-ctx.Done():
			assert.NoError(t, ctx.Err())
		case <-fetcher.finishedCh:
		}

		cancel()
		stopCtx, cancel := context.WithTimeout(context.Background(), timeoutDelay)
		defer cancel()
		assert.NoError(t, manager.finished(stopCtx))
	})

	t.Run("discovered new headers", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), timeoutDelay)

		fetcher := newMockFetcher(sampleBefore, maxKnown)
		store := newExpectStore(t, fetcher.finalState)

		manager := newSamplingCoordinator(concurrency, fetcher.fetch, store)
		manager.state = initSamplingState(samplingRange, fetcher.checkpoint)
		go manager.run(ctx)

		time.Sleep(time.Second)
		// discover new height
		for i := 0; i < 200; i++ {
			// mess the order by running in go-routine
			fetcher.discover(ctx, maxKnown+uint64(i), manager.listen)
		}

		// wait for manager to finish catchup
		assert.NoError(t, manager.state.waitCatchUp(ctx))
		assert.Emptyf(t, manager.state.failed, "failde list should be empty")

		// check if all jobs were fetched successfully
		select {
		case <-ctx.Done():
			assert.NoError(t, ctx.Err())
		case <-fetcher.finishedCh:
		}

		cancel()
		stopCtx, cancel := context.WithTimeout(context.Background(), timeoutDelay)
		defer cancel()
		assert.NoError(t, manager.finished(stopCtx))
	})

	t.Run("prioritize newly discovered over known", func(t *testing.T) {
		sampleFrom := uint64(1)
		maxKnown := uint64(10)
		toBeDiscovered := uint64(21)
		samplingRange := uint64(4)
		concurrency := 1

		fetcher := newMockFetcher(sampleFrom, maxKnown)
		store := newExpectStore(t, fetcher.finalState)

		ctx, cancel := context.WithTimeout(context.Background(), timeoutDelay)

		// lock worker before start, to not let it finish before discover
		lk := newLock(sampleFrom, sampleFrom)

		// expect worker to prioritize newly discovered  (20 -> 10) and then old (0 -> 10)
		order := newCheckOrder().addInterval(sampleFrom, samplingRange) // worker will pick up first job before discovery
		order.addStack(maxKnown+1, toBeDiscovered, samplingRange)
		order.addInterval(samplingRange+1, toBeDiscovered)

		// start manager
		manager := newSamplingCoordinator(concurrency,
			lk.middleWare(
				order.middleWare(
					fetcher.fetch)),
			store)
		manager.state = initSamplingState(samplingRange, fetcher.checkpoint)
		go manager.run(ctx)

		// wait for worker to pick up first job
		time.Sleep(time.Second)

		// discover new height
		fetcher.discover(ctx, toBeDiscovered, manager.listen)

		// check if no header were sampled yet
		assert.Equal(t, 0, fetcher.fetchedAmount())

		// unblock worker
		lk.release(sampleFrom)

		// wait for manager to finish catchup
		assert.NoError(t, manager.state.waitCatchUp(ctx))
		assert.Emptyf(t, manager.state.failed, "failde list should be empty")

		// check if all jobs were fetched successfully
		select {
		case <-ctx.Done():
			assert.NoError(t, ctx.Err())
		case <-fetcher.finishedCh:
		}

		cancel()
		stopCtx, cancel := context.WithTimeout(context.Background(), timeoutDelay)
		defer cancel()
		assert.NoError(t, manager.finished(stopCtx))
	})

	t.Run("priority routine should not lock other workers", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), timeoutDelay)

		fetcher := newMockFetcher(sampleBefore, maxKnown)
		store := newExpectStore(t, fetcher.finalState)

		lk := newLock(sampleBefore, maxKnown) // lock all workers before start
		manager := newSamplingCoordinator(concurrency,
			lk.middleWare(fetcher.fetch), store)
		manager.state = initSamplingState(samplingRange, fetcher.checkpoint)
		go manager.run(ctx)

		time.Sleep(time.Second)
		// discover new height and lock it
		discovered := maxKnown + 1000
		lk.add(discovered)
		fetcher.discover(ctx, discovered, manager.listen)

		// check if no header were sampled yet
		assert.Equal(t, 0, fetcher.fetchedAmount())

		// unblock workers to resume sampling
		lk.releaseAll(discovered)

		// wait for manager to finish catchup
		time.Sleep(time.Second)

		// check that only last header is pending
		assert.EqualValues(t, int(discovered-sampleBefore), fetcher.doneAmount())
		assert.False(t, fetcher.heightIsDone(discovered))

		lk.releaseAll()

		// wait for manager to finish catchup
		assert.NoError(t, manager.state.waitCatchUp(ctx))
		assert.Emptyf(t, manager.state.failed, "failde list is not empty")

		// check if all jobs were fetched successfully
		select {
		case <-ctx.Done():
			assert.NoError(t, ctx.Err())
		case <-fetcher.finishedCh:
		}

		cancel()
		stopCtx, cancel := context.WithTimeout(context.Background(), timeoutDelay)
		defer cancel()
		assert.NoError(t, manager.finished(stopCtx))
	})

	t.Run("failed should be stored", func(t *testing.T) {
		sampleFrom := uint64(1)
		ctx, cancel := context.WithTimeout(context.Background(), timeoutDelay)

		bornToFail := []uint64{4, 8, 15, 16, 23, 42}
		fetcher := newMockFetcher(sampleFrom, maxKnown, bornToFail...)

		//
		expectedState := fetcher.finalState()
		expectedState.SampledBefore = bornToFail[0]
		for _, h := range bornToFail {
			expectedState.Failed[h] = 1
		}

		store := newExpectStore(t, func() checkpoint { return expectedState })

		manager := newSamplingCoordinator(concurrency, fetcher.fetch, store)
		manager.state = initSamplingState(samplingRange, fetcher.checkpoint)
		go manager.run(ctx)

		// check if all jobs were fetched successfully
		select {
		case <-ctx.Done():
			assert.NoError(t, ctx.Err())
		case <-fetcher.finishedCh:
		}

		// wait for manager to finish catchup
		assert.NoError(t, manager.state.waitCatchUp(ctx))

		cancel()
		stopCtx, cancel := context.WithTimeout(context.Background(), timeoutDelay)
		defer cancel()
		assert.NoError(t, manager.finished(stopCtx))
	})

	t.Run("failed should retry on restart", func(t *testing.T) {
		sampleFrom := uint64(1)
		ctx, cancel := context.WithTimeout(context.Background(), timeoutDelay)

		failedLastRun := map[uint64]int{4: 1, 8: 2, 15: 1, 16: 1, 23: 1, 42: 1}
		failedAgain := []uint64{16}

		fetcher := newMockFetcher(sampleFrom, maxKnown, failedAgain...)
		fetcher.checkpoint.Failed = failedLastRun

		expectedState := fetcher.checkpoint
		expectedState.SampledBefore = failedAgain[0]
		expectedState.Failed = map[uint64]int{16: 3}

		store := newExpectStore(t, func() checkpoint { return expectedState })

		manager := newSamplingCoordinator(concurrency, fetcher.fetch, store)
		manager.state = initSamplingState(samplingRange, fetcher.checkpoint)
		go manager.run(ctx)

		// check if all jobs were fetched successfully
		select {
		case <-ctx.Done():
			assert.NoError(t, ctx.Err())
		case <-fetcher.finishedCh:
		}

		// wait for manager to finish catchup
		assert.NoError(t, manager.state.waitCatchUp(ctx))

		cancel()
		stopCtx, cancel := context.WithTimeout(context.Background(), timeoutDelay)
		defer cancel()
		assert.NoError(t, manager.finished(stopCtx))
	})
}

func BenchmarkManager(b *testing.B) {
	concurrency := 100
	samplingRange := uint64(10)
	timeoutDelay := 5 * time.Second

	b.Run("bench run", func(b *testing.B) {
		ctx, cancel := context.WithTimeout(context.Background(), timeoutDelay)
		manager := newSamplingCoordinator(concurrency,
			func(ctx context.Context, u uint64) error { return nil },
			dummyStore{})
		manager.state = initSamplingState(samplingRange, checkpoint{
			SampledBefore: 1,
			MaxKnown:      uint64(b.N),
		})
		go manager.run(ctx)

		// wait for manager to finish catchup
		if err := manager.state.waitCatchUp(ctx); err != nil {
			b.Error(err)
		}
		cancel()
	})
}

// ensures all headers are sampled in range except once that are born to fail
type mockFetcher struct {
	lock sync.Mutex

	checkpoint
	bornToFail map[uint64]bool
	done       map[uint64]int

	finished   bool
	finishedCh chan struct{}
}

func newMockFetcher(SampledBefore, sampleTo uint64, bornToFail ...uint64) mockFetcher {
	failMap := make(map[uint64]bool)
	for _, h := range bornToFail {
		failMap[h] = true
	}
	return mockFetcher{
		checkpoint: checkpoint{
			SampledBefore: SampledBefore,
			MaxKnown:      sampleTo,
			Failed:        make(map[uint64]int),
			Workers:       make([]workerCheckpoint, 0),
		},
		bornToFail: failMap,
		done:       make(map[uint64]int),
		finishedCh: make(chan struct{}),
	}
}

func (m *mockFetcher) fetch(ctx context.Context, h uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	m.lock.Lock()
	defer m.lock.Unlock()

	if h > m.MaxKnown || h < m.SampledBefore {
		return fmt.Errorf("header: %v out of range: %v-%v", h, m.SampledBefore, m.MaxKnown)
	}
	m.done[h]++

	if len(m.done) > int(m.MaxKnown-m.SampledBefore) && !m.finished {
		m.finished = true
		close(m.finishedCh)
	}

	if m.bornToFail[h] {
		return errors.New("born to fail, sad life")
	}
	return nil
}

func (m *mockFetcher) heightIsDone(h uint64) bool {
	return m.done[h] != 0
}

func (m *mockFetcher) doneAmount() int {
	return len(m.done)
}

func (m *mockFetcher) finalState() checkpoint {
	m.lock.Lock()
	defer m.lock.Unlock()

	finalState := m.checkpoint
	finalState.SampledBefore = finalState.MaxKnown + 1
	return finalState
}

func (m *mockFetcher) discover(ctx context.Context, newHeight uint64, emit func(ctx context.Context, h uint64)) {
	m.lock.Lock()
	defer m.lock.Unlock()

	if newHeight > m.checkpoint.MaxKnown {
		m.checkpoint.MaxKnown = newHeight
		if m.finished {
			m.finishedCh = make(chan struct{})
			m.finished = false
		}
	}
	emit(ctx, newHeight)
}

func (m *mockFetcher) fetchedAmount() int {
	m.lock.Lock()
	defer m.lock.Unlock()
	return len(m.done)
}

// ensures correct order of operations
type checkOrder struct {
	lock  sync.Mutex
	queue []uint64
}

func newCheckOrder() *checkOrder {
	return &checkOrder{}
}

func (o *checkOrder) addInterval(start, end uint64) *checkOrder {
	o.lock.Lock()
	defer o.lock.Unlock()

	if end > start {
		for end >= start {
			o.queue = append(o.queue, start)
			start++
		}
		return o
	}

	for start >= end {
		o.queue = append(o.queue, start)
		if start == 0 {
			return o
		}
		start--

	}
	return o
}

func (o *checkOrder) addStack(start, end, size uint64) uint64 {
	if start+size-1 < end {
		end = o.addStack(start+size, end, size)
	}
	if start > end {
		start = end
	}
	o.addInterval(start, end)
	return start - 1
}

func TestOrder(t *testing.T) {
	o := newCheckOrder().addInterval(0, 3).addInterval(3, 0)
	assert.Equal(t, []uint64{0, 1, 2, 3, 3, 2, 1, 0}, o.queue)
}

func TestStack(t *testing.T) {
	o := newCheckOrder().addInterval(0, 3)
	o.addStack(10, 20, 3)
	assert.Equal(t, []uint64{0, 1, 2, 3, 19, 20, 16, 17, 18, 13, 14, 15, 10, 11, 12}, o.queue)
}

func (o *checkOrder) middleWare(out fetchFn) fetchFn {
	return func(ctx context.Context, h uint64) error {
		o.lock.Lock()

		if len(o.queue) > 0 {
			// check last item in queue to be same as input
			if o.queue[0] != h {
				o.lock.Unlock()
				return fmt.Errorf("expected height: %v,got: %v", o.queue[0], h)
			}
			o.queue = o.queue[1:]
		}

		o.lock.Unlock()
		return out(ctx, h)
	}
}

// blocks operations if item is in lock list
type lock struct {
	m         sync.Mutex
	blockList map[uint64]chan struct{}
}

func newLock(from, to uint64) *lock {
	list := make(map[uint64]chan struct{})
	for from <= to {
		list[from] = make(chan struct{})
		from++
	}
	return &lock{
		blockList: list,
	}
}

func (l *lock) add(hs ...uint64) {
	l.m.Lock()
	defer l.m.Unlock()
	for _, h := range hs {
		l.blockList[h] = make(chan struct{})
	}
}

func (l *lock) release(hs ...uint64) {
	l.m.Lock()
	defer l.m.Unlock()

	for _, h := range hs {
		if ch, ok := l.blockList[h]; ok {
			close(ch)
			delete(l.blockList, h)
		}
	}
}

func (l *lock) releaseAll(except ...uint64) {
	m := make(map[uint64]bool)
	for _, h := range except {
		m[h] = true
	}

	l.m.Lock()
	defer l.m.Unlock()

	for h, ch := range l.blockList {
		if m[h] {
			continue
		}
		close(ch)
		delete(l.blockList, h)
	}
}

func (l *lock) middleWare(out fetchFn) fetchFn {
	return func(ctx context.Context, h uint64) error {
		l.m.Lock()
		ch, blocked := l.blockList[h]
		l.m.Unlock()
		if !blocked {
			return out(ctx, h)
		}

		select {
		case <-ch:
			return out(ctx, h)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

type dummyStore struct {
}

func (m dummyStore) load(ctx context.Context) (checkpoint, error) {
	return checkpoint{}, nil
}

func (m dummyStore) store(ctx context.Context, cp checkpoint) {
}

// checks the stored checkpoint is correct
type expectStore struct {
	*testing.T
	dummyStore
	expect func() checkpoint
}

func newExpectStore(t *testing.T, expect func() checkpoint) checkpointStore {
	return &expectStore{
		T:      t,
		expect: expect,
	}
}

func (m *expectStore) store(_ context.Context, cp checkpoint) {
	expected := m.expect()
	if len(expected.Failed) == 0 {
		expected.Failed = make(map[uint64]int)
	}
	assert.Equal(m, expected, cp)
}
