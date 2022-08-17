package das

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestManager(t *testing.T) {
	concurrency := 10

	getter := mockGetter1{
		checkPoint: checkPoint{
			MinSampled: 1,
			MaxKnown:   100,
			Skipped:    nil,
		},
		done:       make(map[uint64]int),
		finishedCh: make(chan struct{}),
	}

	finalState := getter.checkPoint
	finalState.MinSampled = finalState.MaxKnown
	store := mockStore{T: t, expect: getter.finalState}

	t.Run("first run", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		manager := newSamplingManager(concurrency, 0, getter.fetch, store.store)
		manager.run(ctx, getter.checkPoint)

		// wait for manager to finish checkup
		assert.NoError(t, manager.waitCatchUp(ctx))

		// check if all jobs were fetched successfully
		select {
		case <-ctx.Done():
			assert.NoError(t, ctx.Err())
		case <-getter.finishedCh:
		}

		assert.NoError(t, manager.stop(ctx))
	})
	t.Run("discovered new headers", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		manager := newSamplingManager(concurrency, 0, getter.fetch, store.store)
		manager.run(ctx, getter.checkPoint)

		// discover new height
		getter.discover(ctx, 160, manager.listen)
		// wait for manager to finish checkup
		assert.NoError(t, manager.waitCatchUp(ctx))

		// check if all jobs were fetched successfully
		select {
		case <-ctx.Done():
			assert.NoError(t, ctx.Err())
		case <-getter.finishedCh:
		}

		assert.NoError(t, manager.stop(ctx))
	})

	t.Run("priority routine should not lock other workers", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		lk := newLock(1, uint64(concurrency)) // lock all workers before start
		manager := newSamplingManager(concurrency, 0, lk.middleWare(getter.fetch), store.store)
		manager.run(ctx, getter.checkPoint)

		// discover new height
		getter.discover(ctx, 160, manager.listen)

		// check of no header were sampled yet
		assert.Equal(t, 0, getter.fetchedAmount())

		lk.add(100)
		// unblock 1 worker to let it
		lk.release(1)

		// wait for manager to finish checkup
		assert.NoError(t, manager.waitCatchUp(ctx))

		// check if all jobs were fetched successfully
		select {
		case <-ctx.Done():
			assert.NoError(t, ctx.Err())
		case <-getter.finishedCh:
		}

		assert.NoError(t, manager.stop(ctx))
	})
	t.Run("failed should retry on restart", func(t *testing.T) {})
}

type mockGetter1 struct {
	lock sync.Mutex
	checkPoint
	done map[uint64]int

	finished   bool
	finishedCh chan struct{}
}

func (m *mockGetter1) fetch(ctx context.Context, h uint64) error {
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
	return nil
}

func (m *mockGetter1) finalState() checkPoint {
	m.lock.Lock()
	defer m.lock.Unlock()

	finalState := m.checkPoint
	finalState.MinSampled = finalState.MaxKnown
	return finalState
}

func (m *mockGetter1) discover(ctx context.Context, newHeight uint64, emit func(ctx context.Context, h uint64)) {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.checkPoint.MaxKnown = newHeight
	m.finishedCh = make(chan struct{})
	emit(ctx, newHeight)
}

func (m *mockGetter1) fetchedAmount() int {
	m.lock.Lock()
	defer m.lock.Unlock()
	return len(m.done)
}

type mockStore struct {
	*testing.T
	expect func() checkPoint
}

func (m *mockStore) store(_ context.Context, s state) {
	expected := m.expect()
	if len(expected.Skipped) == 0 {
		expected.Skipped = make(map[uint64]int)
	}
	assert.Equal(m, expected, s.checkPoint())
}

type lock struct {
	m         sync.Mutex
	blockList map[uint64]chan struct{} // all fetch calls will be blocked if item is in lock list
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

func (l *lock) add(h uint64) {
	l.m.Lock()
	defer l.m.Unlock()
	l.blockList[h] = make(chan struct{})
}

func (l *lock) release(h uint64) {
	l.m.Lock()
	defer l.m.Unlock()

	if ch, ok := l.blockList[h]; ok {
		close(ch)
		delete(l.blockList, h)
	}
}

func (l *lock) middleWare(out func(ctx context.Context, h uint64) error) func(ctx context.Context, h uint64) error {
	return func(ctx context.Context, h uint64) error {
		l.m.Lock()
		defer l.m.Unlock()

		if _, blocked := l.blockList[h]; !blocked {
			return out(ctx, h)
		}

		ch := l.blockList[h]
		select {
		case <-ch:
			return out(ctx, h)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
