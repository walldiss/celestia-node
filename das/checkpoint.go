package das

import (
	"bytes"
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

type checkpointStore interface {
	load(ctx context.Context) (checkpoint, error)
	store(ctx context.Context, cp checkpoint)
}

type dataStore struct {
	datastore.Datastore
}

type checkpoint struct {
	SampledBefore uint64             `json:"sampled_before"`
	MaxKnown      uint64             `json:"max_known"`
	Failed        map[uint64]int     `json:"failed,omitempty"` // failed will be put in priority on restart
	Workers       []workerCheckpoint `json:"workers,omitempty"`
}

// workerCheckpoint will be used to resume worker on restart
type workerCheckpoint struct {
	From uint64 `json:"from"`
	To   uint64 `json:"to"`
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
		SampledBefore: stats.SampledBefore,
		MaxKnown:      stats.MaxKnown,
		Failed:        stats.Failed,
		Workers:       workers,
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
		if cp.SampledBefore != prevSampledBefore {
			bgs.store.store(ctx, cp)
			prevSampledBefore = cp.SampledBefore
		}
	}
}

func (c checkpoint) String() string {
	buf := bytes.Buffer{}
	buf.WriteString(fmt.Sprintf(
		"SampledBefore: %v, MaxKnown: %v",
		c.SampledBefore, c.MaxKnown))

	if len(c.Failed) > 0 {
		buf.WriteString(fmt.Sprintf("\nFailed: %v", c.Failed))
	}

	if len(c.Workers) > 0 {
		buf.WriteString("\nWorkers: ")
	}
	for _, w := range c.Workers {
		buf.WriteString(fmt.Sprintf("\n from: %v, to: %v", w.From, w.To))
	}
	return buf.String()
}
