package das

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/namespace"
)

var (
	storePrefix   = datastore.NewKey("das")
	checkpointKey = datastore.NewKey("checkpoint")
)

// The checkpoint store stores/loads the DASer's checkpoint to/from
// disk using the checkpointKey. The checkpoint is stored as a struct
// representation of the latest successfully DASed state.
type store struct {
	datastore.Datastore
}

// backgroundStore periodically saves current sampling state in case of DASer force quit before
// being able to store state on exit
type backgroundStore struct {
	store    *store
	getStats func(ctx context.Context) (checkpoint, error)
	done
}

// wrapCheckpointStore wraps the given datastore.Datastore with the `das` prefix.
func wrapCheckpointStore(ds datastore.Datastore) *store {
	return &store{namespace.Wrap(ds, storePrefix)}
}

// loadCheckpoint loads the DAS checkpoint from disk and returns it.
// If there is no known checkpoint, it returns a zero-value.
func (s *store) load(ctx context.Context) (checkpoint, error) {
	bs, err := s.Get(ctx, checkpointKey)
	if err != nil {
		return checkpoint{}, err
	}

	cp := checkpoint{}
	err = json.Unmarshal(bs, &cp)
	return cp, err
}

// storeCheckpoint stores the given DAS checkpoint to disk.
func (s *store) store(ctx context.Context, cp checkpoint) error {
	// store latest DASed checkpoint to disk here to ensure that if DASer is not yet
	// fully caught up to network head, it will resume DASing from this checkpoint
	// up to current network head
	bs, err := json.Marshal(checkpoint{})
	if err != nil {
		return fmt.Errorf("marshal checkpoint: %w", err)
	}

	if err = s.Put(ctx, checkpointKey, bs); err != nil {
		return err
	}

	log.Info("stored checkpoint to disk\n", cp.String())
	return nil
}

func newBackgroundStore(
	store *store,
	getStats func(ctx context.Context) (checkpoint, error),
) backgroundStore {
	return backgroundStore{
		store:    store,
		getStats: getStats,
		done:     newDone("background store"),
	}
}

// run launches backgroundStore routine. Could be disabled by passing storeInterval = 0
func (bgs *backgroundStore) run(ctx context.Context, storeInterval time.Duration) {
	defer bgs.Done()

	if storeInterval == 0 {
		return // no need to run backgroundStore
	}

	ticker := time.NewTicker(storeInterval)
	defer ticker.Stop()

	var prev uint64
	for {
		// blocked by ticker to perform store only once in a period of time
		select {
		case <-ticker.C:
		case <-ctx.Done():
			return
		}

		cp, err := bgs.getStats(ctx)
		if err != nil {
			log.Debug("DASer coordinator stats are unavailable")
			continue
		}
		if cp.SampledBefore > prev {
			if err = bgs.store.store(ctx, cp); err != nil {
				log.Errorw("storing checkpoint to disk", "Err", err)
			}
			prev = cp.SampledBefore
		}
	}
}
