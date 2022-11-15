package share

import (
	"context"

	"github.com/celestiaorg/nmt"

	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"

	"github.com/celestiaorg/celestia-node/share/ipld"
	"github.com/celestiaorg/nmt/namespace"
)

// SharesWithProofs contains data with corresponding Merkle Proof
type SharesWithProofs struct {
	Root cid.Cid
	// Share is a full data including namespace
	Shares []Share
	// Proof is a Merkle Proof of current share, will be nil if GetSharesByNamespace called with
	// collectProofs == false
	Proof *nmt.Proof
}

// GetShare fetches and returns the data for leaf `leafIndex` of root `rootCid`.
func GetShare(
	ctx context.Context,
	bGetter blockservice.BlockGetter,
	rootCid cid.Cid,
	leafIndex int,
	totalLeafs int, // this corresponds to the extended square width
) (Share, error) {
	nd, err := ipld.GetLeaf(ctx, bGetter, rootCid, leafIndex, totalLeafs)
	if err != nil {
		return nil, err
	}

	return leafToShare(nd), nil
}

// GetShares walks the tree of a given root and puts shares into the given 'put' func.
// Does not return any error, and returns/unblocks only on success
// (got all shares) or on context cancellation.
func GetShares(ctx context.Context, bGetter blockservice.BlockGetter, root cid.Cid, shares int, put func(int, Share)) {
	ctx, span := tracer.Start(ctx, "get-shares")
	defer span.End()

	putNode := func(i int, leaf format.Node) {
		put(i, leafToShare(leaf))
	}
	ipld.GetLeaves(ctx, bGetter, root, shares, putNode)
}

// GetSharesByNamespace walks the tree of a given root and returns its shares within the
// given namespace.ID. If a share could not be retrieved, err is not nil, and the returned array
// contains nil shares in place of the shares it was unable to retrieve.
func GetSharesByNamespace(
	ctx context.Context,
	bGetter blockservice.BlockGetter,
	root cid.Cid,
	nID namespace.ID,
	maxShares int,
	collectProofs bool,
) (*SharesWithProofs, error) {
	ctx, span := tracer.Start(ctx, "get-shares-by-namespace")
	defer span.End()

	nodes, err := ipld.GetLeavesByNamespace(ctx, bGetter, root, nID, maxShares, collectProofs)
	if nodes == nil {
		return nil, err
	}

	shares := make([]Share, 0, nodes.LeavesEnd-nodes.LeavesStart)
	for _, leaf := range nodes.Leaves {
		if leaf != nil {
			shares = append(shares, leafToShare(leaf))
		}
	}

	proof := new(nmt.Proof)
	if collectProofs {
		*proof = nmt.NewInclusionProof(nodes.LeavesStart, nodes.LeavesEnd, nodes.Proofs, true)
	}
	return &SharesWithProofs{
		Root:   root,
		Shares: shares,
		Proof:  proof,
	}, nil
}

// leafToShare converts an NMT leaf into a Share.
func leafToShare(nd format.Node) Share {
	// * Additional namespace is prepended so that parity data can be identified with a parity
	// namespace, which we cut off
	return nd.RawData()[NamespaceSize:]
}
