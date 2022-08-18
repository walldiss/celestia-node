package das

import (
	"context"
	"encoding/json"

	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/namespace"
)

var (
	storePrefix   = datastore.NewKey("das")
	checkpointKey = datastore.NewKey("checkpoint")
)

type checkpoint struct {
	MinSampled uint64         `json:"min_sampled_height"` // lowest sampled height
	MaxKnown   uint64         `json:"max_known_height"`   // height of the newest known header
	Skipped    map[uint64]int `json:"skipped"`            // header's heights that been skipped with corresponding try count
}

// wrapCheckpointStore wraps the given datastore.Datastore with the `das`
// prefix. The checkpoint store stores/loads the DASer's checkpoint to/from
// disk using the checkpointKey. The checkpoint is stored as an uint64
// representation of the height of the latest successfully DASed header.
func wrapCheckpointStore(ds datastore.Datastore) datastore.Datastore {
	return namespace.Wrap(ds, storePrefix)
}

// loadCheckpoint loads the DAS checkpoint from disk and returns it.
// If there is no known checkpoint, it returns height 0.
func loadCheckpoint(ctx context.Context, ds datastore.Datastore) (checkpoint, error) {
	bs, err := ds.Get(ctx, checkpointKey)
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
func storeCheckpoint(ctx context.Context, ds datastore.Datastore, cp checkpoint) error {
	bs, err := json.Marshal(cp)
	if err != nil {
		return err
	}
	return ds.Put(ctx, checkpointKey, bs)
}
