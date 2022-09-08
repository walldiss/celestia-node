package das

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/celestiaorg/celestia-node/header"

	"github.com/stretchr/testify/assert"
)

func TestCoordinator(t *testing.T) {
	concurrency := 10
	samplingRange := uint64(10)
	networkHead := uint64(500)
	sampleFrom := uint64(genesisHeight)
	timeoutDelay := 125 * time.Second

	t.Run("test run", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), timeoutDelay)

		sampler := newMockSampler(sampleFrom, networkHead)

		coordinator := newSamplingCoordinator(concurrency, samplingRange, getterStab{}, onceMiddleWare(sampler.sample))
		go coordinator.run(ctx, sampler.checkpoint)

		// wait for coordinator to indicateDone catchup
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
		assert.NoError(t, coordinator.wait(stopCtx))
		assert.Equal(t, sampler.finalState(), newCheckpoint(coordinator.state.unsafeStats()))
	})

	t.Run("discovered new headers", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), timeoutDelay)

		sampler := newMockSampler(sampleFrom, networkHead)

		coordinator := newSamplingCoordinator(concurrency, samplingRange, getterStab{}, sampler.sample)
		go coordinator.run(ctx, sampler.checkpoint)

		time.Sleep(50 * time.Millisecond)
		// discover new height
		for i := 0; i < 200; i++ {
			// mess the order by running in go-routine
			sampler.discover(ctx, networkHead+uint64(i), coordinator.listen)
		}

		// wait for coordinator to indicateDone catchup
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
		assert.NoError(t, coordinator.wait(stopCtx))
		assert.Equal(t, sampler.finalState(), newCheckpoint(coordinator.state.unsafeStats()))
	})

	t.Run("prioritize newly discovered over known", func(t *testing.T) {
		sampleFrom := uint64(1)
		networkHead := uint64(10)
		toBeDiscovered := uint64(20)
		samplingRange := uint64(4)
		concurrency := 1

		sampler := newMockSampler(sampleFrom, networkHead)

		ctx, cancel := context.WithTimeout(context.Background(), timeoutDelay)

		// lock worker before start, to not let it indicateDone before discover
		lk := newLock(sampleFrom, sampleFrom)

		// expect worker to prioritize newly discovered  (20 -> 10) and then old (0 -> 10)
		order := newCheckOrder().addInterval(sampleFrom, samplingRange) // worker will pick up first job before discovery
		order.addStack(networkHead+1, toBeDiscovered, samplingRange)
		order.addInterval(samplingRange+1, toBeDiscovered)

		// start coordinator
		coordinator := newSamplingCoordinator(concurrency, samplingRange, getterStab{},
			lk.middleWare(
				order.middleWare(
					sampler.sample)),
		)
		go coordinator.run(ctx, sampler.checkpoint)

		// wait for worker to pick up first job
		time.Sleep(50 * time.Millisecond)

		// discover new height
		sampler.discover(ctx, toBeDiscovered, coordinator.listen)

		// check if no header were sampled yet
		assert.Equal(t, 0, sampler.sampledAmount())

		// unblock worker
		lk.release(sampleFrom)

		// wait for coordinator to indicateDone catchup
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
		assert.NoError(t, coordinator.wait(stopCtx))
		assert.Equal(t, sampler.finalState(), newCheckpoint(coordinator.state.unsafeStats()))
	})

	t.Run("priority routine should not lock other workers", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), timeoutDelay)

		sampler := newMockSampler(sampleFrom, networkHead)

		lk := newLock(sampleFrom, networkHead) // lock all workers before start
		coordinator := newSamplingCoordinator(concurrency, samplingRange, getterStab{},
			lk.middleWare(sampler.sample))
		go coordinator.run(ctx, sampler.checkpoint)

		time.Sleep(50 * time.Millisecond)
		// discover new height and lock it
		discovered := networkHead + 1000
		lk.add(discovered)
		sampler.discover(ctx, discovered, coordinator.listen)

		// check if no header were sampled yet
		assert.Equal(t, 0, sampler.sampledAmount())

		// unblock workers to resume sampling
		lk.releaseAll(discovered)

		// wait for coordinator to indicateDone catchup
		time.Sleep(50 * time.Millisecond)

		// check that only last header is pending
		assert.EqualValues(t, int(discovered-sampleFrom), sampler.doneAmount())
		assert.False(t, sampler.heightIsDone(discovered))

		lk.releaseAll()

		// wait for coordinator to indicateDone catchup
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
		assert.NoError(t, coordinator.wait(stopCtx))
		assert.Equal(t, sampler.finalState(), newCheckpoint(coordinator.state.unsafeStats()))
	})

	t.Run("failed should be stored", func(t *testing.T) {
		sampleFrom := uint64(1)
		ctx, cancel := context.WithTimeout(context.Background(), timeoutDelay)

		bornToFail := []uint64{4, 8, 15, 16, 23, 42}
		sampler := newMockSampler(sampleFrom, networkHead, bornToFail...)

		coordinator := newSamplingCoordinator(concurrency, samplingRange, getterStab{}, onceMiddleWare(sampler.sample))
		go coordinator.run(ctx, sampler.checkpoint)

		// wait for coordinator to indicateDone catchup
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
		assert.NoError(t, coordinator.wait(stopCtx))

		// set failed items in expectedState
		expectedState := sampler.finalState()
		for _, h := range bornToFail {
			expectedState.Failed[h] = 1
		}
		assert.Equal(t, expectedState, newCheckpoint(coordinator.state.unsafeStats()))
	})

	t.Run("failed should retry on restart", func(t *testing.T) {
		sampleFrom := uint64(50)
		ctx, cancel := context.WithTimeout(context.Background(), timeoutDelay)

		failedLastRun := map[uint64]int{4: 1, 8: 2, 15: 1, 16: 1, 23: 1, 42: 1, sampleFrom - 1: 1}
		failedAgain := []uint64{16}

		sampler := newMockSampler(sampleFrom, networkHead, failedAgain...)
		sampler.checkpoint.Failed = failedLastRun

		coordinator := newSamplingCoordinator(concurrency, samplingRange, getterStab{}, onceMiddleWare(sampler.sample))
		go coordinator.run(ctx, sampler.checkpoint)

		// wait for coordinator to indicateDone catchup
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
		assert.NoError(t, coordinator.wait(stopCtx))

		expectedState := sampler.finalState()
		expectedState.Failed = make(map[uint64]int)
		for _, v := range failedAgain {
			expectedState.Failed[v] = failedLastRun[v] + 1
		}
		assert.Equal(t, expectedState, newCheckpoint(coordinator.state.unsafeStats()))
	})
}

func BenchmarkCoordinator(b *testing.B) {
	concurrency := 100
	samplingRange := uint64(10)
	timeoutDelay := 5 * time.Second

	b.Run("bench run", func(b *testing.B) {
		ctx, cancel := context.WithTimeout(context.Background(), timeoutDelay)
		coordinator := newSamplingCoordinator(concurrency, samplingRange, newBenchGetter(),
			func(ctx context.Context, h *header.ExtendedHeader) error { return nil })
		go coordinator.run(ctx, checkpoint{
			SampleFrom:  1,
			NetworkHead: uint64(b.N),
		})

		// wait for coordinator to indicateDone catchup
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
			SampleFrom:  sampledBefore,
			NetworkHead: sampleTo,
			Failed:      make(map[uint64]int),
			Workers:     make([]workerCheckpoint, 0),
		},
		bornToFail: failMap,
		done:       make(map[uint64]int),
		finishedCh: make(chan struct{}),
	}
}

func (m *mockSampler) sample(ctx context.Context, h *header.ExtendedHeader) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	m.lock.Lock()
	defer m.lock.Unlock()

	height := uint64(h.Height)
	m.done[height]++

	if len(m.done) > int(m.NetworkHead-m.SampleFrom) && !m.finished {
		m.finished = true
		close(m.finishedCh)
	}

	if m.bornToFail[height] {
		return errors.New("born to fail, sad life")
	}

	if height > m.NetworkHead || height < m.SampleFrom {
		if m.Failed[height] == 0 {
			return fmt.Errorf("header: %v out of range: %v-%v", h, m.SampleFrom, m.NetworkHead)
		}
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
	finalState.SampleFrom = finalState.NetworkHead + 1
	return finalState
}

func (m *mockSampler) discover(ctx context.Context, newHeight uint64, emit func(ctx context.Context, h uint64)) {
	m.lock.Lock()
	defer m.lock.Unlock()

	if newHeight > m.checkpoint.NetworkHead {
		m.checkpoint.NetworkHead = newHeight
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
	return func(ctx context.Context, h *header.ExtendedHeader) error {
		o.lock.Lock()

		if len(o.queue) > 0 {
			// check last item in queue to be same as input
			if o.queue[0] != uint64(h.Height) {
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
	return func(ctx context.Context, h *header.ExtendedHeader) error {
		l.m.Lock()
		ch, blocked := l.blockList[uint64(h.Height)]
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

func onceMiddleWare(out sampleFn) sampleFn {
	db := make(map[int64]int)
	lock := sync.Mutex{}
	return func(ctx context.Context, h *header.ExtendedHeader) error {
		lock.Lock()
		defer lock.Unlock()
		db[h.Height]++
		if db[h.Height] > 1 {
			return fmt.Errorf("header sampled second time: %v", h.Height)
		}
		return out(ctx, h)
	}
}
