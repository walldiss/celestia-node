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
	concurrency := 100
	maxKnown := uint64(10000)
	sampleFrom := uint64(100)

	t.Run("first run", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		fetcher := newMockFetcher(sampleFrom, maxKnown)
		store := mockStore{T: t, expect: fetcher.finalState}

		manager := newSamplingManager(concurrency, 0, 0, fetcher.fetch, store.store)
		manager.run(ctx, fetcher.checkpoint)

		// wait for manager to finish catchup
		assert.NoError(t, manager.waitCatchUp(ctx))

		// check if all jobs were fetched successfully
		select {
		case <-ctx.Done():
			assert.NoError(t, ctx.Err())
		case <-fetcher.finishedCh:
		}

		assert.NoError(t, manager.stop(ctx))
	})

	t.Run("discovered new headers", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		fetcher := newMockFetcher(sampleFrom, maxKnown)
		store := mockStore{T: t, expect: fetcher.finalState}

		manager := newSamplingManager(concurrency, 0, 0, fetcher.fetch, store.store)
		manager.run(ctx, fetcher.checkpoint)

		// discover new height
		fetcher.discover(ctx, maxKnown+1000, manager.listen)
		// wait for manager to finish catchup
		assert.NoError(t, manager.waitCatchUp(ctx))

		// check if all jobs were fetched successfully
		select {
		case <-ctx.Done():
			assert.NoError(t, ctx.Err())
		case <-fetcher.finishedCh:
		}

		assert.NoError(t, manager.stop(ctx))
	})

	t.Run("prioritize newly discovered over known", func(t *testing.T) {
		maxKnown := uint64(10)
		toBeDiscovered := uint64(20)
		sampleFrom := uint64(1)

		fetcher := newMockFetcher(sampleFrom, maxKnown)
		store := mockStore{T: t, expect: fetcher.finalState}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// start manager with 1 routine in locked state
		lk := newLock(sampleFrom, sampleFrom) // lock worker before start

		// expect worker to prioritize newly discovered  (20 -> 10) and then old (0 -> 10)
		order := newCheckOrder().
			addInterval(sampleFrom, sampleFrom). // worker will pick up first job before discovery
			addInterval(toBeDiscovered, maxKnown+1).
			addInterval(sampleFrom+1, toBeDiscovered)

		// start manager
		manager := newSamplingManager(1, 0, 0,
			lk.middleWare(
				order.middleWare(
					fetcher.fetch)),
			store.store)
		manager.run(ctx, fetcher.checkpoint)

		// wait for worker to pick up first job
		time.Sleep(time.Second)

		// discover new height
		fetcher.discover(ctx, 20, manager.listen)

		// check if no header were sampled yet
		assert.Equal(t, 0, fetcher.fetchedAmount())

		// unblock worker
		lk.release(sampleFrom)

		// wait for manager to finish catchup
		assert.NoError(t, manager.waitCatchUp(ctx))

		// check if all jobs were fetched successfully
		select {
		case <-ctx.Done():
			assert.NoError(t, ctx.Err())
		case <-fetcher.finishedCh:
		}

		assert.NoError(t, manager.stop(ctx))
	})

	t.Run("priority routine should not lock other workers", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		fetcher := newMockFetcher(sampleFrom, maxKnown)
		store := mockStore{T: t, expect: fetcher.finalState}

		lk := newLock(sampleFrom, sampleFrom+uint64(concurrency)) // lock all workers before start
		manager := newSamplingManager(concurrency, 0, 0,
			lk.middleWare(fetcher.fetch), store.store)
		manager.run(ctx, fetcher.checkpoint)

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
		assert.EqualValues(t, int(discovered-sampleFrom), fetcher.doneAmount())
		assert.False(t, fetcher.checkDone(discovered))

		lk.releaseAll()
		// check if all jobs were fetched successfully
		select {
		case <-ctx.Done():
			assert.NoError(t, ctx.Err())
		case <-fetcher.finishedCh:
		}

		assert.NoError(t, manager.stop(ctx))
	})

	t.Run("failed should retry on restart", func(t *testing.T) {
		sampleFrom := uint64(1)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		bornToFail := []uint64{4, 8, 15, 16, 23, 42}
		fetcher := newMockFetcher(sampleFrom, maxKnown, bornToFail...)

		expectedState := fetcher.finalState()
		expectedState.MinSampled = bornToFail[0]
		for _, h := range bornToFail {
			expectedState.Skipped[h] = 1
		}

		store := mockStore{T: t, expect: func() checkpoint {
			return expectedState
		}}

		manager := newSamplingManager(concurrency, 0, 0, fetcher.fetch, store.store)
		manager.run(ctx, fetcher.checkpoint)

		// wait for manager to finish catchup
		time.Sleep(time.Second)

		// check if all jobs were fetched successfully
		select {
		case <-ctx.Done():
			assert.NoError(t, ctx.Err())
		case <-fetcher.finishedCh:
		}

		assert.NoError(t, manager.stop(ctx))
	})
}

type mockFetcher struct {
	lock sync.Mutex

	checkpoint
	bornToFail map[uint64]bool
	done       map[uint64]int

	finished   bool
	finishedCh chan struct{}
}

func newMockFetcher(sampleFrom, sampleTo uint64, bornToFail ...uint64) mockFetcher {
	failMap := make(map[uint64]bool)
	for _, h := range bornToFail {
		failMap[h] = true
	}
	return mockFetcher{
		checkpoint: checkpoint{
			MinSampled: sampleFrom,
			MaxKnown:   sampleTo,
			Skipped:    make(map[uint64]int),
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

	if h > m.MaxKnown || h < m.MinSampled {
		return fmt.Errorf("header: %v out of range: %v-%v", h, m.MinSampled, m.MaxKnown)
	}
	m.done[h]++

	if len(m.done) > int(m.MaxKnown-m.MinSampled) && !m.finished {
		m.finished = true
		close(m.finishedCh)
	}

	if m.bornToFail[h] {
		return errors.New("born to fail, sad life")
	}
	return nil
}

func (m *mockFetcher) checkDone(h uint64) bool {
	return m.done[h] != 0
}

func (m *mockFetcher) doneAmount() int {
	return len(m.done)
}

func (m *mockFetcher) finalState() checkpoint {
	m.lock.Lock()
	defer m.lock.Unlock()

	finalState := m.checkpoint
	finalState.MinSampled = finalState.MaxKnown
	return finalState
}

func (m *mockFetcher) discover(ctx context.Context, newHeight uint64, emit func(ctx context.Context, h uint64)) {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.checkpoint.MaxKnown = newHeight
	m.finishedCh = make(chan struct{})
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

func TestOrder(t *testing.T) {
	o := newCheckOrder().addInterval(0, 3).addInterval(3, 0)
	assert.Equal(t, []uint64{0, 1, 2, 3, 3, 2, 1, 0}, o.queue)
}

func (o *checkOrder) middleWare(out fetchFn) fetchFn {
	return func(ctx context.Context, h uint64) error {
		o.lock.Lock()

		if len(o.queue) > 0 {
			// check last item in queue to be same as input
			if o.queue[0] != h {
				o.lock.Unlock()
				return fmt.Errorf("expected height: %v,got: %v", o.queue[len(o.queue)-1], h)
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

type mockStore struct {
	*testing.T
	expect func() checkpoint
}

func (m *mockStore) store(_ context.Context, s state) {
	expected := m.expect()
	if len(expected.Skipped) == 0 {
		expected.Skipped = make(map[uint64]int)
	}
	assert.Equal(m, expected, s.toCheckPoint())
}
