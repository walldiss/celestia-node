package peers

import (
	"context"
	"sync/atomic"
	"time"
)

// syncPool accumulates peers from shrex.Sub validators and controls message retransmission.
// It will unlock the validator if two conditions are met:
//  1. an ExtendedHeader that corresponds to the data hash was received and verified by the node
//  2. the EDS corresponding to the data hash was synced by the node
type syncPool struct {
	*pool

	isValidDataHash    atomic.Bool
	validatorWaitCh    chan struct{}
	validatorWaitTimer *time.Timer

	// isSynced refers to whether the data hash corresponding to
	// the sync pool has been synced by the node
	isSynced   atomic.Bool
	waitSyncCh chan struct{}
}

func newSyncPool() *syncPool {
	return &syncPool{
		pool:       newPool(),
		waitSyncCh: make(chan struct{}),
	}
}

// waitValidation waits for ExtendedHeader to sync within timeout.
func (p *syncPool) waitValidation(ctx context.Context) (valid bool) {
	select {
	case <-p.validatorWaitCh:
		return p.isValidDataHash.Load()
	case <-ctx.Done():
		return false
	}
}

func (p *syncPool) WaitSync(ctx context.Context) (synced bool) {
	select {
	case <-p.waitSyncCh:
		return true
	case <-ctx.Done():
		return false
	}
}

func (p *syncPool) indicateValid() {
	if p.isValidDataHash.CompareAndSwap(false, true) {
		// unlock all awaiting Validators.
		// if unable to stop the timer, the channel was already closed by afterfunc
		if p.validatorWaitTimer.Stop() {
			close(p.validatorWaitCh)
		}
	}
}
