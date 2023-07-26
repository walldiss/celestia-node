package eds

import (
	"context"
	"fmt"
	dsbadger "github.com/celestiaorg/go-ds-badger4"
	"github.com/filecoin-project/dagstore/index"
	"github.com/filecoin-project/dagstore/shard"
	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/namespace"
	"github.com/multiformats/go-multihash"
	"time"
)

// simpleInvertedIndex is an inverted index that only stores a single shard key per multihash. Its
// implementation is modified from the default upstream implementation in dagstore/index.
type simpleInvertedIndex struct {
	ds ds.Batching
}

// newSimpleInvertedIndex returns a new inverted index that only stores a single shard key per
// multihash. This is because we use badger as a storage backend, so updates are expensive, and we
// don't care which shard is used to serve a cid.
func newSimpleInvertedIndex(_ ds.Batching, path string) (*simpleInvertedIndex, error) {
	opts := dsbadger.DefaultOptions // this should be copied
	opts.NumGoroutines = 8
	//opts.LmaxCompaction = true
	opts.GcInterval = 0
	opts.GcSleep = time.Second
	opts.NumCompactors = 7 // Run at least 2 compactors. Zero-th compactor prioritizes L0.
	opts.NumLevelZeroTables = 1
	opts.NumMemtables = 10
	opts.NumLevelZeroTablesStall = 15

	dts, err := dsbadger.NewDatastore(path+"/inverted_index", &opts)
	if err != nil {
		return nil, fmt.Errorf("node: can't open Badger Datastore: %w", err)
	}

	dts.DiskUsage(context.Background())

	return &simpleInvertedIndex{
		ds: namespace.Wrap(dts, ds.NewKey("/inverted/index")),
	}, nil
}

func (s *simpleInvertedIndex) AddMultihashesForShard(
	ctx context.Context,
	mhIter index.MultihashIterator,
	sk shard.Key,
) error {
	// in the original implementation, a mutex is used here to prevent unnecessary updates to the
	// key. The amount of extra data produced by this is negligible, and the performance benefits
	// from removing the lock are significant (indexing is a hot path during sync).
	batch, err := s.ds.Batch(ctx)
	if err != nil || ctx.Err() != nil {
		err = fmt.Errorf("failed to create ds batch: %w", err)
		fmt.Println("ERROR IN INVERTED INDEX", err, "ctx err", ctx.Err(), time.Now())
		return err
	}

	if err := mhIter.ForEach(func(mh multihash.Multihash) error {
		key := ds.NewKey(string(mh))
		//ok, err := s.ds.Has(ctx, key)
		//if err != nil {
		//	return fmt.Errorf("failed to check if value for multihash exists %s, err: %w", mh, err)
		//}
		//
		//if !ok {
		if err := batch.Put(ctx, key, []byte(sk.String())); err != nil || ctx.Err() != nil {
			err = fmt.Errorf("failed to put mh=%s, err=%w", mh, err)
			fmt.Println("ERROR IN INVERTED INDEX", "ctx err", ctx.Err(), time.Now())
			return err
		}
		//}

		return nil
	}); err != nil || ctx.Err() != nil {
		err = fmt.Errorf("failed to add index entry: %w", err)
		fmt.Println("ERROR IN INVERTED INDEX", "ctx err", ctx.Err(), time.Now())
		return err
	}

	if err := batch.Commit(ctx); err != nil || ctx.Err() != nil {
		err = fmt.Errorf("failed to commit batch: %w", err)
		fmt.Println("ERROR IN INVERTED INDEX", "ctx err", ctx.Err(), time.Now())
		return err
	}

	if err := s.ds.Sync(ctx, ds.Key{}); err != nil || ctx.Err() != nil {
		err = fmt.Errorf("failed to sync puts: %w", err)
		fmt.Println("ERROR IN INVERTED INDEX", "ctx err", ctx.Err(), time.Now())
		return err
	}
	return nil
}

func (s *simpleInvertedIndex) GetShardsForMultihash(ctx context.Context, mh multihash.Multihash) ([]shard.Key, error) {
	key := ds.NewKey(string(mh))
	sbz, err := s.ds.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("failed to lookup index for mh %s, err: %w", mh, err)
	}

	return []shard.Key{shard.KeyFromString(string(sbz))}, nil
}
