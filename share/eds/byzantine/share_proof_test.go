package byzantine

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/celestiaorg/celestia-app/pkg/da"
	"github.com/ipfs/go-cid"
	mdutils "github.com/ipfs/go-merkledag/test"
	"github.com/stretchr/testify/require"

	"github.com/celestiaorg/celestia-node/share"
	"github.com/celestiaorg/celestia-node/share/ipld"
)

func TestGetProof(t *testing.T) {
	const width = 4

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()
	bServ := mdutils.Bserv()

	shares := share.RandShares(t, width*width)
	in, err := share.AddShares(ctx, shares, bServ)
	require.NoError(t, err)

	dah := da.NewDataAvailabilityHeader(in)
	var tests = []struct {
		roots [][]byte
	}{
		{dah.RowsRoots},
		{dah.ColumnRoots},
	}

	for i, tt := range tests {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			for _, root := range tt.roots {
				rootCid := ipld.MustCidFromNamespacedSha256(root)
				for index := 0; uint(index) < in.Width(); index++ {
					proof := make([]cid.Cid, 0)
					proof, err = ipld.GetProof(ctx, bServ, rootCid, proof, index, int(in.Width()))
					require.NoError(t, err)
					node, err := ipld.GetLeaf(ctx, bServ, rootCid, index, int(in.Width()))
					require.NoError(t, err)
					inclusion := NewShareWithProof(index, node.RawData(), proof)
					require.True(t, inclusion.Validate(rootCid))
				}
			}
		})
	}
}

func TestGetProofs(t *testing.T) {
	const width = 4
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()
	bServ := mdutils.Bserv()

	shares := share.RandShares(t, width*width)
	in, err := share.AddShares(ctx, shares, bServ)
	require.NoError(t, err)

	dah := da.NewDataAvailabilityHeader(in)
	for _, root := range dah.ColumnRoots {
		rootCid := ipld.MustCidFromNamespacedSha256(root)
		data := make([][]byte, 0, in.Width())
		for index := 0; uint(index) < in.Width(); index++ {
			node, err := ipld.GetLeaf(ctx, bServ, rootCid, index, int(in.Width()))
			require.NoError(t, err)
			data = append(data, node.RawData()[9:])
		}

		proves, err := GetProofsForShares(ctx, bServ, rootCid, data)
		require.NoError(t, err)
		for _, proof := range proves {
			require.True(t, proof.Validate(rootCid))
		}
	}
}
