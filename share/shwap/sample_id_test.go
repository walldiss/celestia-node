package shwap

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/celestiaorg/celestia-node/share"
	"github.com/celestiaorg/celestia-node/share/eds/edstest"
)

func TestSampleID(t *testing.T) {
	square := edstest.RandEDS(t, 4)
	root, err := share.NewRoot(square)
	require.NoError(t, err)

	id, err := NewSampleID(1, 1, 1, root)
	require.NoError(t, err)

	data, err := id.MarshalBinary()
	require.NoError(t, err)

	idOut, err := SampleIDFromBinary(data)
	require.NoError(t, err)
	assert.EqualValues(t, id, idOut)

	err = idOut.Validate(root)
	require.NoError(t, err)
}