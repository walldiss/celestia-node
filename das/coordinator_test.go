package das

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ipfs/go-datastore"
	ds_sync "github.com/ipfs/go-datastore/sync"

	"github.com/stretchr/testify/assert"
)

func TestCoordinator(t *testing.T) {
	concurrency := 10
	samplingRange := uint64(10)
	maxKnown := uint64(500)
	sampleBefore := uint64(1)
	timeoutDelay := 5 * time.Second

	t.Run("test run", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), timeoutDelay)

		sampler := newMockSampler(sampleBefore, maxKnown)
		store := wrapCheckpointStore(ds_sync.MutexWrap(datastore.NewMapDatastore()))

		coordinator := newSamplingCoordinator(concurrency, sampler.sample, store)
		coordinator.state = initCoordinatorState(samplingRange, sampler.checkpoint)
		go coordinator.run(ctx)

		// wait for coordinator to finish catchup
		assert.NoError(t, coordinator.state.waitCatchUp(ctx))
		assert.Emptyf(t, coordinator.state.failed, "failed list should be empty")

		// check if all jobs were sampled successfully
		select {
		case <-ctx.Done():
			assert.NoError(t, ctx.Err())
		case <-sampler.finishedCh:
		}
		cancel()

		stopCtx, cancel := context.WithTimeout(context.Background(), timeoutDelay)
		defer cancel()
		assert.NoError(t, coordinator.finished(stopCtx))
		equalCheckpoint(ctx, t, sampler.finalState(), store)
	})

	t.Run("discovered new headers", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), timeoutDelay)

		sampler := newMockSampler(sampleBefore, maxKnown)
		store := wrapCheckpointStore(ds_sync.MutexWrap(datastore.NewMapDatastore()))

		coordinator := newSamplingCoordinator(concurrency, sampler.sample, store)
		coordinator.state = initCoordinatorState(samplingRange, sampler.checkpoint)
		go coordinator.run(ctx)

		time.Sleep(50 * time.Millisecond)
		// discover new height
		for i := 0; i < 200; i++ {
			// mess the order by running in go-routine
			sampler.discover(ctx, maxKnown+uint64(i), coordinator.listen)
		}

		// wait for coordinator to finish catchup
		assert.NoError(t, coordinator.state.waitCatchUp(ctx))
		assert.Emptyf(t, coordinator.state.failed, "failed list should be empty")

		// check if all jobs were sampled successfully
		select {
		case <-ctx.Done():
			assert.NoError(t, ctx.Err())
		case <-sampler.finishedCh:
		}
		cancel()

		stopCtx, cancel := context.WithTimeout(context.Background(), timeoutDelay)
		defer cancel()
		assert.NoError(t, coordinator.finished(stopCtx))
		equalCheckpoint(ctx, t, sampler.finalState(), store)
	})

	t.Run("prioritize newly discovered over known", func(t *testing.T) {
		sampleFrom := uint64(1)
		maxKnown := uint64(10)
		toBeDiscovered := uint64(20)
		samplingRange := uint64(4)
		concurrency := 1

		sampler := newMockSampler(sampleFrom, maxKnown)
		store := wrapCheckpointStore(ds_sync.MutexWrap(datastore.NewMapDatastore()))

		ctx, cancel := context.WithTimeout(context.Background(), timeoutDelay)

		// lock worker before start, to not let it finish before discover
		lk := newLock(sampleFrom, sampleFrom)

		// expect worker to prioritize newly discovered  (20 -> 10) and then old (0 -> 10)
		order := newCheckOrder().addInterval(sampleFrom, samplingRange) // worker will pick up first job before discovery
		order.addStack(maxKnown+1, toBeDiscovered, samplingRange)
		order.addInterval(samplingRange+1, toBeDiscovered)

		// start coordinator
		coordinator := newSamplingCoordinator(concurrency,
			lk.middleWare(
				order.middleWare(
					sampler.sample)),
			store)
		coordinator.state = initCoordinatorState(samplingRange, sampler.checkpoint)
		go coordinator.run(ctx)

		// wait for worker to pick up first job
		time.Sleep(50 * time.Millisecond)

		// discover new height
		sampler.discover(ctx, toBeDiscovered, coordinator.listen)

		// check if no header were sampled yet
		assert.Equal(t, 0, sampler.sampledAmount())

		// unblock worker
		lk.release(sampleFrom)

		// wait for coordinator to finish catchup
		assert.NoError(t, coordinator.state.waitCatchUp(ctx))
		assert.Emptyf(t, coordinator.state.failed, "failed list should be empty")

		// check if all jobs were sampled successfully
		select {
		case <-ctx.Done():
			assert.NoError(t, ctx.Err())
		case <-sampler.finishedCh:
		}
		cancel()

		stopCtx, cancel := context.WithTimeout(context.Background(), timeoutDelay)
		defer cancel()
		assert.NoError(t, coordinator.finished(stopCtx))
		equalCheckpoint(ctx, t, sampler.finalState(), store)
	})

	t.Run("priority routine should not lock other workers", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), timeoutDelay)

		sampler := newMockSampler(sampleBefore, maxKnown)
		store := wrapCheckpointStore(ds_sync.MutexWrap(datastore.NewMapDatastore()))

		lk := newLock(sampleBefore, maxKnown) // lock all workers before start
		coordinator := newSamplingCoordinator(concurrency,
			lk.middleWare(sampler.sample), store)
		coordinator.state = initCoordinatorState(samplingRange, sampler.checkpoint)
		go coordinator.run(ctx)

		time.Sleep(50 * time.Millisecond)
		// discover new height and lock it
		discovered := maxKnown + 1000
		lk.add(discovered)
		sampler.discover(ctx, discovered, coordinator.listen)

		// check if no header were sampled yet
		assert.Equal(t, 0, sampler.sampledAmount())

		// unblock workers to resume sampling
		lk.releaseAll(discovered)

		// wait for coordinator to finish catchup
		time.Sleep(50 * time.Millisecond)

		// check that only last header is pending
		assert.EqualValues(t, int(discovered-sampleBefore), sampler.doneAmount())
		assert.False(t, sampler.heightIsDone(discovered))

		lk.releaseAll()

		// wait for coordinator to finish catchup
		assert.NoError(t, coordinator.state.waitCatchUp(ctx))
		assert.Emptyf(t, coordinator.state.failed, "failed list is not empty")

		// check if all jobs were sampled successfully
		select {
		case <-ctx.Done():
			assert.NoError(t, ctx.Err())
		case <-sampler.finishedCh:
		}
		cancel()

		stopCtx, cancel := context.WithTimeout(context.Background(), timeoutDelay)
		defer cancel()
		assert.NoError(t, coordinator.finished(stopCtx))
		equalCheckpoint(ctx, t, sampler.finalState(), store)
	})

	t.Run("failed should be stored", func(t *testing.T) {
		sampleFrom := uint64(1)
		ctx, cancel := context.WithTimeout(context.Background(), timeoutDelay)

		bornToFail := []uint64{4, 8, 15, 16, 23, 42}
		sampler := newMockSampler(sampleFrom, maxKnown, bornToFail...)
		store := wrapCheckpointStore(ds_sync.MutexWrap(datastore.NewMapDatastore()))

		// set failed items in expectedState
		expectedState := sampler.finalState()
		expectedState.SampledBefore = bornToFail[0]
		expectedState.Failed = make(map[uint64]int)
		for _, h := range bornToFail {
			expectedState.Failed[h] = 1
		}

		coordinator := newSamplingCoordinator(concurrency, sampler.sample, store)
		coordinator.state = initCoordinatorState(samplingRange, sampler.checkpoint)
		go coordinator.run(ctx)

		// wait for coordinator to finish catchup
		assert.NoError(t, coordinator.state.waitCatchUp(ctx))

		// check if all jobs were sampled successfully
		select {
		case <-ctx.Done():
			assert.NoError(t, ctx.Err())
		case <-sampler.finishedCh:
		}
		cancel()

		stopCtx, cancel := context.WithTimeout(context.Background(), timeoutDelay)
		defer cancel()
		assert.NoError(t, coordinator.finished(stopCtx))
		equalCheckpoint(ctx, t, expectedState, store)
	})

	t.Run("failed should retry on restart", func(t *testing.T) {
		sampleFrom := uint64(1)
		ctx, cancel := context.WithTimeout(context.Background(), timeoutDelay)

		failedLastRun := map[uint64]int{4: 1, 8: 2, 15: 1, 16: 1, 23: 1, 42: 1}
		failedAgain := []uint64{16}

		sampler := newMockSampler(sampleFrom, maxKnown, failedAgain...)
		sampler.checkpoint.Failed = failedLastRun

		expectedState := sampler.checkpoint
		expectedState.SampledBefore = failedAgain[0]
		expectedState.Failed = map[uint64]int{16: 3}

		store := wrapCheckpointStore(ds_sync.MutexWrap(datastore.NewMapDatastore()))

		coordinator := newSamplingCoordinator(concurrency, sampler.sample, store)
		coordinator.state = initCoordinatorState(samplingRange, sampler.checkpoint)
		go coordinator.run(ctx)

		// wait for coordinator to finish catchup
		assert.NoError(t, coordinator.state.waitCatchUp(ctx))

		// check if all jobs were sampled successfully
		select {
		case <-ctx.Done():
			assert.NoError(t, ctx.Err())
		case <-sampler.finishedCh:
		}
		cancel()

		stopCtx, cancel := context.WithTimeout(context.Background(), timeoutDelay)
		defer cancel()
		assert.NoError(t, coordinator.finished(stopCtx))
		equalCheckpoint(ctx, t, expectedState, store)
	})
}

func BenchmarkCoordinator(b *testing.B) {
	concurrency := 100
	samplingRange := uint64(10)
	timeoutDelay := 5 * time.Second

	b.Run("bench run", func(b *testing.B) {
		ctx, cancel := context.WithTimeout(context.Background(), timeoutDelay)
		coordinator := newSamplingCoordinator(concurrency,
			func(ctx context.Context, u uint64) error { return nil },
			wrapCheckpointStore(datastore.NewNullDatastore()))
		coordinator.state = initCoordinatorState(samplingRange, checkpoint{
			SampledBefore: 1,
			MaxKnown:      uint64(b.N),
		})
		go coordinator.run(ctx)

		// wait for coordinator to finish catchup
		if err := coordinator.state.waitCatchUp(ctx); err != nil {
			b.Error(err)
		}
		cancel()
	})
}

// ensures all headers are sampled in range except ones that are born to fail
type mockSampler struct {
	lock sync.Mutex

	checkpoint
	bornToFail map[uint64]bool
	done       map[uint64]int

	finished   bool
	finishedCh chan struct{}
}

func newMockSampler(sampledBefore, sampleTo uint64, bornToFail ...uint64) mockSampler {
	failMap := make(map[uint64]bool)
	for _, h := range bornToFail {
		failMap[h] = true
	}
	return mockSampler{
		checkpoint: checkpoint{
			SampledBefore: sampledBefore,
			MaxKnown:      sampleTo,
			Failed:        nil,
			Workers:       nil,
		},
		bornToFail: failMap,
		done:       make(map[uint64]int),
		finishedCh: make(chan struct{}),
	}
}

func (m *mockSampler) sample(ctx context.Context, h uint64) error {
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

func (m *mockSampler) heightIsDone(h uint64) bool {
	m.lock.Lock()
	defer m.lock.Unlock()
	return m.done[h] != 0
}

func (m *mockSampler) doneAmount() int {
	m.lock.Lock()
	defer m.lock.Unlock()
	return len(m.done)
}

func (m *mockSampler) finalState() checkpoint {
	m.lock.Lock()
	defer m.lock.Unlock()

	finalState := m.checkpoint
	finalState.SampledBefore = finalState.MaxKnown + 1
	return finalState
}

func (m *mockSampler) discover(ctx context.Context, newHeight uint64, emit func(ctx context.Context, h uint64)) {
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

func (m *mockSampler) sampledAmount() int {
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

func (o *checkOrder) middleWare(out sampleFn) sampleFn {
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

func (l *lock) middleWare(out sampleFn) sampleFn {
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

func equalCheckpoint(ctx context.Context, t *testing.T, expected checkpoint, store *store) {
	cp, err := store.load(ctx)
	assert.NoError(t, err, err)
	assert.Equal(t, expected, cp)
}
