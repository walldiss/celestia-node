package das

import (
	"encoding/json"
	"time"

	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/namespace"
	"golang.org/x/net/context"
)

var (
	storePrefix   = datastore.NewKey("das")
	checkpointKey = datastore.NewKey("checkpoint")
)

type checkpointStore interface {
	load(ctx context.Context) (checkpoint, error)
	store(ctx context.Context, cp checkpoint)
}

type dataStore struct {
	datastore.Datastore
}

// backgroundStore periodically saves current sampling state in case of DASer force quit before
// being able saving state on exit
type backgroundStore struct {
	store    checkpointStore
	getStats func(ctx context.Context) (checkpoint, error)
	done
}

// wrapCheckpointStore wraps the given datastore.Datastore with the `das`
// prefix. The checkpoint store stores/loads the DASer's checkpoint to/from
// disk using the checkpointKey. The checkpoint is stored as an uint64
// representation of the height of the latest successfully DASed header.
func wrapCheckpointStore(ds datastore.Datastore) checkpointStore {
	return &dataStore{namespace.Wrap(ds, storePrefix)}
}

// loadCheckpoint loads the DAS checkpoint from disk and returns it.
// If there is no known checkpoint, it returns height 0.
func (s *dataStore) load(ctx context.Context) (checkpoint, error) {
	bs, err := s.Get(ctx, checkpointKey)
	if err != nil {
		// if no checkpoint was found, return zero-value checkpoint
		if err == datastore.ErrNotFound {
			log.Debug("checkpoint not found, starting sampling at block height 1")
			return checkpoint{}, nil
		}

		return checkpoint{}, err
	}

	cp := checkpoint{}
	err = json.Unmarshal(bs, &cp)
	return cp, err
}

// storeCheckpoint stores the given DAS checkpoint to disk.
func (s *dataStore) store(ctx context.Context, cp checkpoint) {
	// store latest DASed checkpoint to disk here to ensure that if DASer is not yet
	// fully caught up to network head, it will resume DASing from this checkpoint
	// up to current network head
	bs, err := json.Marshal(cp)
	if err != nil {
		log.Errorw("marshal checkpoint", "Err", err)
		return
	}

	if err = s.Put(ctx, checkpointKey, bs); err != nil {
		log.Errorw("storing checkpoint to disk", "Err", err)
	}
	log.Info("stored checkpoint to disk\n", cp.String())
}

func newBackgroundStore(store checkpointStore,
	getStats func(ctx context.Context) (checkpoint, error)) backgroundStore {
	return backgroundStore{
		store:    store,
		getStats: getStats,
		done:     newDone("background store"),
	}
}

func (bgs *backgroundStore) run(ctx context.Context, storeInterval time.Duration) {
	defer bgs.Done()

	if storeInterval == 0 {
		// run store routine only when storeInterval is non 0
		return
	}
	storeTicker := time.NewTicker(storeInterval)

	var prevSampledBefore uint64
	for {
		// blocked by ticker to perform store only once in period of time
		select {
		case <-storeTicker.C:
		case <-ctx.Done():
			return
		}

		cp, err := bgs.getStats(ctx)
		if err != nil {
			continue
		}
		if cp.SampledBefore > prevSampledBefore {
			bgs.store.store(ctx, cp)
			prevSampledBefore = cp.SampledBefore
		}
	}
}
