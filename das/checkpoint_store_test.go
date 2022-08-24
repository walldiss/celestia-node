package das

import (
	"context"
	"testing"
	"time"

	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/sync"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCheckpointStore(t *testing.T) {
	ds := wrapCheckpointStore(sync.MutexWrap(datastore.NewMapDatastore()))
	failed := make(map[uint64]int)
	failed[2] = 1
	failed[3] = 2
	checkpoint := checkpoint{
		MinSampled: 1,
		MaxKnown:   6,
		Failed:     failed,
		Workers: []workerState{
			{
				From:   1,
				To:     2,
				Curr:   1,
				Failed: []uint64{1},
				Err:    nil,
			},
			{
				From:   5,
				To:     10,
				Curr:   7,
				Failed: []uint64{6},
				Err:    nil,
			},
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer t.Cleanup(cancel)
	err := storeCheckpoint(ctx, ds, checkpoint)
	require.NoError(t, err)
	got, err := loadCheckpoint(ctx, ds)
	require.NoError(t, err)
	assert.Equal(t, checkpoint, got)
}
