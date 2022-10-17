package ipld

import (
	"fmt"
	"hash"

	"github.com/minio/sha256-simd"
	mhcore "github.com/multiformats/go-multihash/core"

	"github.com/celestiaorg/nmt"
)

func init() {
	// Register custom hasher in the multihash register.
	// Required for the Bitswap to hash and verify inbound data correctly
	mhcore.Register(sha256Namespace8Flagged, func() hash.Hash {
		return defaultHasher()
	})
}

// namespaceHasher implements hash.Hash over NMT Hasher.
// TODO: Move to NMT repo?
type namespaceHasher struct {
	*nmt.Hasher
	tp   byte
	data []byte
}

// defaultHasher constructs the namespaceHasher with default configuration
func defaultHasher() *namespaceHasher {
	return &namespaceHasher{Hasher: nmt.NewNmtHasher(sha256.New(), nmt.DefaultNamespaceIDLen, true)}
}

// Write writes the namespaced data to be hashed.
//
// Requires data of fixed size to match leaf or inner NMT nodes.
// Only one write is allowed.
func (n *namespaceHasher) Write(data []byte) (int, error) {
	if n.data != nil {
		return 0, fmt.Errorf("ipld: only one write to hasher is allowed")
	}

	innerNodeSize := (int(n.NamespaceLen) + n.Hash.Size()) * 2
	switch len(data) {
	default:
		panic("AAAA")
		return 0, fmt.Errorf("ipld: wrong sized data written to the hasher, len: %v, %v", len(data), innerNodeSize)
	case innerNodeSize:
		n.tp = nmt.NodePrefix
	case leafNodeSize:
		n.tp = nmt.LeafPrefix
	}

	n.data = data
	return len(n.data), nil
}

// Sum computes the hash.
// Does not append the given suffix and violating the interface.
func (n *namespaceHasher) Sum([]byte) []byte {
	isLeafData := n.tp == nmt.LeafPrefix
	if isLeafData {
		return n.Hasher.HashLeaf(n.data)
	}

	flagLen := int(n.NamespaceLen)
	sha256Len := n.Hasher.Size()
	return n.Hasher.HashNode(n.data[:flagLen+sha256Len], n.data[flagLen+sha256Len:])
}
