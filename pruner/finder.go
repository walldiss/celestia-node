package pruner

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/ipfs/go-datastore"

	"github.com/celestiaorg/celestia-node/header"
)

var (
	lastPrunedHeaderKey = datastore.NewKey("last_pruned_header")
)

type checkpoint struct {
	ds datastore.Datastore

	lastPrunedHeader atomic.Pointer[header.ExtendedHeader]

	// TODO @renaynay: keep track of failed roots to retry  in separate job
}

func newCheckpoint(ds datastore.Datastore) *checkpoint {
	return &checkpoint{ds: ds}
}

// avgBlockTime is the average block time in seconds.
var avgBlockTime = time.Second * 10

// findPruneableTarget returns the header that is the target of the next prune.
func (s *Service) findPruneableTarget(ctx context.Context) (*header.ExtendedHeader, error) {
	lastPruned := s.lastPruned()

	pruneCutoff := time.Now().Add(time.Duration(-s.window))

	// handle system clock reset
	if pruneCutoff.Before(lastPruned.Time()) {
		return nil, fmt.Errorf("system clock reset detected, last pruned header is within the prune window")
	}

	timeDiff := pruneCutoff.Sub(lastPruned.Time())
	estimatedCutoffHeight := lastPruned.Height() + uint64(timeDiff/avgBlockTime)

	head, err := s.getter.Head(ctx)
	if err != nil {
		return nil, err
	}
	if head.Height() < estimatedCutoffHeight {
		estimatedCutoffHeight = head.Height()
	}

	for {
		getCtx, cancel := context.WithTimeout(ctx, time.Second)
		nextHeader, err := s.getter.GetByHeight(getCtx, estimatedCutoffHeight)
		cancel()
		if err != nil {
			// we tried all the way back to the last pruned header
			if estimatedCutoffHeight == lastPruned.Height() {
				return nil, fmt.Errorf("failed to find pruneable headers: %w", err)
			}

			// try again with a lower cutoff
			estimatedCutoffHeight--
			continue
		}

		// estimated header is before the cutoff
		if nextHeader.Time().Before(pruneCutoff) {
			timeDiff = pruneCutoff.Sub(nextHeader.Time())
			// if the diff is less than 2x the average block time, we can assume we're close enough
			if timeDiff < avgBlockTime*2 {
				return nextHeader, nil
			}

			// if the next header is before the cutoff, we need to search forward
			estimatedCutoffHeight = nextHeader.Height() + uint64(timeDiff/avgBlockTime)
			continue
		}

		// we overshot the cutoff, so we need to search backwards
		newTimeDiff := nextHeader.Time().Sub(pruneCutoff)
		if errFactor := newTimeDiff / timeDiff; errFactor > 1 {
			// block time estimation was too low and thrown us too far forward, increase it and try again
			avgBlockTime *= errFactor + 1
			estimatedCutoffHeight = lastPruned.Height() + uint64(timeDiff/avgBlockTime)
			continue
		}

		timeDiff = newTimeDiff
		estimatedCutoffHeight = nextHeader.Height() - uint64(timeDiff/avgBlockTime)
	}
}

// initializeCheckpoint initializes the checkpoint, storing the earliest header in the chain.
func (s *Service) initializeCheckpoint(ctx context.Context) error {
	firstHeader, err := s.getter.GetByHeight(ctx, 1)
	if err != nil {
		return fmt.Errorf("failed to initialize checkpoint: %w", err)
	}

	return s.updateCheckpoint(ctx, firstHeader)
}

// loadCheckpoint loads the last checkpoint from disk, initializing it if it does not already exist.
func (s *Service) loadCheckpoint(ctx context.Context) error {
	bin, err := s.checkpoint.ds.Get(ctx, lastPrunedHeaderKey)
	if err != nil {
		if err == datastore.ErrNotFound {
			return s.initializeCheckpoint(ctx)
		}
		return fmt.Errorf("failed to load checkpoint: %w", err)
	}

	var lastPruned header.ExtendedHeader
	if err := lastPruned.UnmarshalJSON(bin); err != nil {
		return fmt.Errorf("failed to load checkpoint: %w", err)
	}

	s.checkpoint.lastPrunedHeader.Store(&lastPruned)
	return nil
}

// updateCheckpoint updates the checkpoint with the last pruned header height
// and persists it to disk.
func (s *Service) updateCheckpoint(ctx context.Context, lastPruned *header.ExtendedHeader) error {
	s.checkpoint.lastPrunedHeader.Store(lastPruned)

	bin, err := lastPruned.MarshalJSON()
	if err != nil {
		return err
	}

	return s.checkpoint.ds.Put(ctx, lastPrunedHeaderKey, bin)
}

func (s *Service) lastPruned() *header.ExtendedHeader {
	return s.checkpoint.lastPrunedHeader.Load()
}
