package full

import (
	"context"
	"github.com/celestiaorg/celestia-node/share"
	"github.com/celestiaorg/celestia-node/share/store"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/celestiaorg/celestia-app/pkg/da"

	"github.com/celestiaorg/celestia-node/header/headertest"
	"github.com/celestiaorg/celestia-node/share/mocks"
	"github.com/celestiaorg/celestia-node/share/testing/edstest"
)

//func TestShareAvailableOverMocknet_Full(t *testing.T) {
//	ctx, cancel := context.WithCancel(context.Background())
//	defer cancel()
//
//	net := availability_test.NewTestDAGNet(ctx, t)
//	_, root := RandNode(net, 32)
//
//	eh := headertest.RandExtendedHeaderWithRoot(t, root)
//	nd := Node(net)
//	net.ConnectAll()
//
//	err := nd.SharesAvailable(ctx, eh)
//	assert.NoError(t, err)
//}

func TestSharesAvailable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// RandServiceWithSquare creates a NewShareAvailability inside, so we can test it
	eds := edstest.RandEDS(t, 16)
	dah, err := da.NewDataAvailabilityHeader(eds)
	eh := headertest.RandExtendedHeaderWithRoot(t, &dah)

	getter := mocks.NewMockGetter(gomock.NewController(t))
	getter.EXPECT().GetEDS(gomock.Any(), eh).Return(eds, nil)

	store, err := store.NewStore(store.DefaultParameters(), t.TempDir())
	avail := NewShareAvailability(store, getter, nil)
	err = avail.SharesAvailable(ctx, eh)
	require.NoError(t, err)

	// Check if the store has the root
	has, err := store.HasByHash(ctx, dah.Hash())
	require.NoError(t, err)
	require.True(t, has)

	// Check if the store has the root linked to the height
	has, err = store.HasByHeight(ctx, eh.Height())
	require.NoError(t, err)
	require.True(t, has)
}

func TestSharesAvailable_StoredEds(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	eds := edstest.RandEDS(t, 4)
	dah, err := da.NewDataAvailabilityHeader(eds)
	eh := headertest.RandExtendedHeaderWithRoot(t, &dah)
	require.NoError(t, err)

	store, err := store.NewStore(store.DefaultParameters(), t.TempDir())
	avail := NewShareAvailability(store, nil, nil)

	prevHeigh := eh.Height() - 1
	f, err := store.Put(ctx, dah.Hash(), prevHeigh, eds)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	has, err := store.HasByHeight(ctx, eh.Height())
	require.NoError(t, err)
	require.False(t, has)

	err = avail.SharesAvailable(ctx, eh)
	require.NoError(t, err)

	has, err = store.HasByHeight(ctx, eh.Height())
	require.NoError(t, err)
	require.True(t, has)
}

func TestSharesAvailable_ErrNotAvailable(t *testing.T) {
	ctrl := gomock.NewController(t)
	getter := mocks.NewMockGetter(ctrl)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	eds := edstest.RandEDS(t, 4)
	dah, err := da.NewDataAvailabilityHeader(eds)
	eh := headertest.RandExtendedHeaderWithRoot(t, &dah)
	require.NoError(t, err)

	store, err := store.NewStore(store.DefaultParameters(), t.TempDir())
	avail := NewShareAvailability(store, getter, nil)

	errors := []error{share.ErrNotFound, context.DeadlineExceeded}
	for _, getterErr := range errors {
		getter.EXPECT().GetEDS(gomock.Any(), gomock.Any()).Return(nil, getterErr)
		err := avail.SharesAvailable(ctx, eh)
		require.ErrorIs(t, err, share.ErrNotAvailable)
	}
}
