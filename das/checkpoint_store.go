package das

import (
	"bytes"
	"context"
	"encoding/binary"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/namespace"
)

var (
	storePrefix   = datastore.NewKey("das")
	checkpointKey = datastore.NewKey("checkpoint")
)

type checkPoint struct {
	minSampledHeight uint64 // lowest sampled height
	maxKnownHeight   uint64 // height of the newest known header
	queue            []uint64
}

func (c checkPoint) encode() []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, c.minSampledHeight)
	binary.Write(buf, binary.LittleEndian, c.maxKnownHeight)
	binary.Write(buf, binary.LittleEndian, c.queue)

	return buf.Bytes()
}

func decode(data []byte) (checkPoint, error) {
	r64 := make([]uint64, (len(data)+7)/8)
	if len(r64) < 2 {
		log.Warnf("corrupted checkpoint data")
		return checkPoint{}, nil
	}

	rbuf := bytes.NewBuffer(data)
	err := binary.Read(rbuf, binary.LittleEndian, &r64)
	if err != nil {
		return checkPoint{}, err
	}
	return checkPoint{
		minSampledHeight: r64[0],
		maxKnownHeight:   r64[1],
		queue:            r64[2:],
	}, nil
}

// wrapCheckpointStore wraps the given datastore.Datastore with the `das`
// prefix. The checkpoint store stores/loads the DASer's checkpoint to/from
// disk using the checkpointKey. The checkpoint is stored as an uint64
// representation of the height of the latest successfully DASed header.
func wrapCheckpointStore(ds datastore.Datastore) datastore.Datastore {
	return namespace.Wrap(ds, storePrefix)
}

// loadCheckpoint loads the DAS checkpoint height from disk and returns it.
// If there is no known checkpoint, it returns height 0.
func loadCheckpoint(ctx context.Context, ds datastore.Datastore) (checkPoint, error) {
	bs, err := ds.Get(ctx, checkpointKey)
	if err != nil {
		// if no checkpoint was found, return checkpoint as 0
		// DASer begins sampling on discovering new header
		if err == datastore.ErrNotFound {
			log.Debug("checkpoint not found, starting sampling at block height 1")
			return checkPoint{}, nil
		}

		return checkPoint{}, err
	}

	return decode(bs)
}

// storeCheckpoint stores the given DAS checkpoint to disk.
func storeCheckpoint(ctx context.Context, ds datastore.Datastore, cp checkPoint) error {
	bs := cp.encode()
	return ds.Put(ctx, checkpointKey, bs)
}
