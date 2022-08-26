package das

import (
	"context"
	"encoding/json"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/namespace"
	"time"
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

type checkpoint struct {
	MinSampled uint64 `json:"min_sampled"`
	MaxKnown   uint64 `json:"max_known"`
	// failed will be put in priority upon restart
	Failed  map[uint64]int     `json:"failed,omitempty"`
	Workers []workerCheckpoint `json:"Workers,omitempty"`
}

type workerCheckpoint struct {
	From, To uint64
}

// backgroundStore periodically saves current sampling state in case of DASer force quit before
// being able saving state on exit
type backgroundStore struct {
	store    checkpointStore
	getStats func(ctx context.Context) (checkpoint, error)
	done
}

func newCheckpoint(stats SamplingStats) checkpoint {
	workers := make([]workerCheckpoint, 0, len(stats.Workers))
	for _, w := range stats.Workers {
		workers = append(workers, workerCheckpoint{
			From: w.Curr,
			To:   w.To,
		})
	}
	return checkpoint{
		MinSampled: stats.MinSampled,
		MaxKnown:   stats.MaxKnown,
		Failed:     stats.Failed,
		Workers:    workers,
	}
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
	log.Infow("stored checkpoint to disk", "checkpoint", cp)
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

	var prevMinSampled uint64
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
		if cp.MinSampled != prevMinSampled {
			bgs.store.store(ctx, cp)
			prevMinSampled = cp.MinSampled
		}
	}
}
