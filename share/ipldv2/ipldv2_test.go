package ipldv2

import (
	"context"
	"testing"
	"time"

	"github.com/ipfs/boxo/bitswap"
	"github.com/ipfs/boxo/bitswap/network"
	"github.com/ipfs/boxo/blockservice"
	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/boxo/routing/offline"
	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	record "github.com/libp2p/go-libp2p-record"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/celestiaorg/rsmt2d"

	"github.com/celestiaorg/celestia-node/share/eds"
	"github.com/celestiaorg/celestia-node/share/eds/edstest"
)

// TestV2Roundtrip tests full protocol round trip of:
// EDS -> Sample -> IPLDBlock -> BlockService -> Bitswap and in reverse.
func TestV2RoundtripGetBlock(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	sqr := edstest.RandEDS(t, 8)

	path := t.TempDir() + "/eds_file"
	f, err := eds.CreateFile(path, sqr)
	require.NoError(t, err)

	b := NewBlockstore[*edsFileAndFS]((*edsFileAndFS)(f))
	client := remoteClient(ctx, t, b)

	axis := []rsmt2d.Axis{rsmt2d.Col, rsmt2d.Row}
	width := int(sqr.Width())
	for _, axis := range axis {
		for i := 0; i < width*width; i++ {
			smpl, err := NewSampleFromEDS(sqr, i, axis)
			require.NoError(t, err)

			cid, err := smpl.ID.Cid()
			require.NoError(t, err)

			blkOut, err := client.GetBlock(ctx, cid)
			require.NoError(t, err)
			assert.EqualValues(t, cid, blkOut.Cid())

			data, err := smpl.MarshalBinary()
			require.NoError(t, err)
			assert.EqualValues(t, data, blkOut.RawData())
		}
	}
}

func TestV2RoundtripGetBlocks(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	sqr := edstest.RandEDS(t, 16) // TODO(@Wondertan): does not work with more than 8

	path := t.TempDir() + "/eds_file"
	f, err := eds.CreateFile(path, sqr)
	require.NoError(t, err)

	b := NewBlockstore[*edsFileAndFS]((*edsFileAndFS)(f))
	client := remoteClient(ctx, t, b)

	set := cid.NewSet()
	axis := []rsmt2d.Axis{rsmt2d.Col, rsmt2d.Row}
	width := int(sqr.Width())
	for _, axis := range axis {
		for i := 0; i < width*width; i++ {
			smpl, err := NewSampleFromEDS(sqr, i, axis)
			require.NoError(t, err)

			cid, err := smpl.ID.Cid()
			require.NoError(t, err)

			set.Add(cid)
		}
	}

	blks := client.GetBlocks(ctx, set.Keys())
	err = set.ForEach(func(c cid.Cid) error {
		select {
		case blk := <-blks:
			assert.True(t, set.Has(blk.Cid()))
		case <-ctx.Done():
			return ctx.Err()
		}
		return nil
	})
	assert.NoError(t, err)
}

func remoteClient(ctx context.Context, t *testing.T, bstore blockstore.Blockstore) blockservice.BlockService {
	net, err := mocknet.FullMeshLinked(2)
	require.NoError(t, err)

	dstore := dssync.MutexWrap(ds.NewMapDatastore())
	routing := offline.NewOfflineRouter(dstore, record.NamespacedValidator{})
	_ = bitswap.New(
		ctx,
		network.NewFromIpfsHost(net.Hosts()[0], routing),
		bstore,
	)

	dstoreClient := dssync.MutexWrap(ds.NewMapDatastore())
	bstoreClient := blockstore.NewBlockstore(dstoreClient)
	routingClient := offline.NewOfflineRouter(dstoreClient, record.NamespacedValidator{})

	bitswapClient := bitswap.New(
		ctx,
		network.NewFromIpfsHost(net.Hosts()[1], routingClient),
		bstoreClient,
	)

	err = net.ConnectAllButSelf()
	require.NoError(t, err)

	return blockservice.New(bstoreClient, bitswapClient, blockservice.WithAllowlist(defaultAllowlist))
}