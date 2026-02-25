package core

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/celestiaorg/celestia-app/v7/pkg/da"
	libhead "github.com/celestiaorg/go-header"

	"github.com/celestiaorg/celestia-node/header"
	"github.com/celestiaorg/celestia-node/libs/utils"
	"github.com/celestiaorg/celestia-node/share/shwap/p2p/shrex/shrexsub"
	"github.com/celestiaorg/celestia-node/store"
)

// listenerSubChSize is the per-subscription buffer size for headers delivered
// after EDS is stored locally.
const listenerSubChSize = 64

// Listener is responsible for listening to Core for
// new block events and converting new Core blocks into
// the main data structure used in the Celestia DA network:
// `ExtendedHeader`. After digesting the Core block, extending
// it, and generating the `ExtendedHeader`, the Listener
// broadcasts the new `ExtendedHeader` to the header-sub gossipsub
// network.
//
// Listener also implements libhead.Subscriber so that bridge-node DASers
// can subscribe to headers directly from the Listener rather than from
// the p2p network, guaranteeing that EDS is already in the local store
// before the DASer processes a header. Multiple concurrent subscriptions
// are supported; each receives every header via its own buffered channel.
type Listener struct {
	fetcher *BlockFetcher

	construct          header.ConstructFn
	store              *store.Store
	availabilityWindow time.Duration
	archival           bool

	headerBroadcaster libhead.Broadcaster[*header.ExtendedHeader]
	hashBroadcaster   shrexsub.BroadcastFn

	// subsMu protects subs.
	subsMu sync.Mutex
	// subs holds all active subscriptions. Each header is fan-out delivered
	// to every subscription after EDS is stored locally.
	subs []*listenerSubscription

	metrics *listenerMetrics

	chainID string

	listenerTimeout time.Duration
	cancel          context.CancelFunc
	closed          chan struct{}
}

func NewListener(
	bcast libhead.Broadcaster[*header.ExtendedHeader],
	fetcher *BlockFetcher,
	hashBroadcaster shrexsub.BroadcastFn,
	construct header.ConstructFn,
	store *store.Store,
	blocktime time.Duration,
	opts ...Option,
) (*Listener, error) {
	p := defaultParams()
	for _, opt := range opts {
		opt(&p)
	}

	var (
		metrics *listenerMetrics
		err     error
	)
	if p.metrics {
		metrics, err = newListenerMetrics()
		if err != nil {
			return nil, err
		}
	}

	return &Listener{
		fetcher:            fetcher,
		headerBroadcaster:  bcast,
		hashBroadcaster:    hashBroadcaster,
		construct:          construct,
		store:              store,
		availabilityWindow: p.availabilityWindow,
		archival:           p.archival,
		listenerTimeout:    5 * blocktime,
		metrics:            metrics,
		chainID:            p.chainID,
	}, nil
}

// Subscribe returns a new Subscription that receives every header after EDS is
// stored. Each subscription has its own buffered channel so slow consumers do
// not block the listener or other subscribers.
// Implements libhead.Subscriber for use by bridge-node DASers.
func (cl *Listener) Subscribe() (libhead.Subscription[*header.ExtendedHeader], error) {
	sub := &listenerSubscription{
		ch: make(chan *header.ExtendedHeader, listenerSubChSize),
	}
	sub.cancelFn = func() { cl.removeSub(sub) }

	cl.subsMu.Lock()
	cl.subs = append(cl.subs, sub)
	cl.subsMu.Unlock()

	return sub, nil
}

// SetVerifier is a no-op: headers from the core listener have already been
// validated locally before being written to subscriber channels.
func (cl *Listener) SetVerifier(func(context.Context, *header.ExtendedHeader) error) error {
	return nil
}

// removeSub unregisters sub and closes its channel.
func (cl *Listener) removeSub(sub *listenerSubscription) {
	cl.subsMu.Lock()
	defer cl.subsMu.Unlock()
	for i, s := range cl.subs {
		if s == sub {
			cl.subs = append(cl.subs[:i], cl.subs[i+1:]...)
			close(sub.ch)
			return
		}
	}
}

// notifySubs fan-outs eh to all active subscriptions. Non-blocking: if a
// subscription's buffer is full the header is dropped for that subscriber and
// a warning is logged.
func (cl *Listener) notifySubs(eh *header.ExtendedHeader) {
	cl.subsMu.Lock()
	defer cl.subsMu.Unlock()
	for _, sub := range cl.subs {
		select {
		case sub.ch <- eh:
		default:
			log.Warnw("listener: subscriber channel full, dropping header",
				"height", eh.Height())
		}
	}
}

// Start kicks off the Listener listener loop.
func (cl *Listener) Start(ctx context.Context) error {
	if cl.cancel != nil {
		return fmt.Errorf("listener: already started")
	}

	if err := cl.verifyChainID(ctx); err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	cl.cancel = cancel
	cl.closed = make(chan struct{})

	subs, err := cl.fetcher.SubscribeNewBlockEvent(ctx)
	if err != nil {
		return err
	}

	go cl.listen(ctx, subs)
	return nil
}

// verifyChainID checks that the core endpoint is on the expected network
// before starting the listener. This prevents connecting to a wrong network.
func (cl *Listener) verifyChainID(ctx context.Context) error {
	if cl.chainID == "" {
		return nil
	}

	networkID, err := cl.fetcher.ChainID(ctx)
	if err != nil {
		return fmt.Errorf("listener: fetching chain ID from core endpoint: %w", err)
	}

	if networkID != cl.chainID {
		return fmt.Errorf(
			"listener: core endpoint network mismatch: expected %q, got %q",
			cl.chainID, networkID,
		)
	}

	return nil
}

// Stop stops the listener loop.
func (cl *Listener) Stop(ctx context.Context) error {
	cl.cancel()
	select {
	case <-cl.closed:
		cl.cancel = nil
		cl.closed = nil
	case <-ctx.Done():
		return ctx.Err()
	}

	err := cl.metrics.Close()
	if err != nil {
		log.Warnw("listener: closing metrics", "err", err)
	}
	return nil
}

// listen kicks off a loop, listening for new block events from Core,
// generating ExtendedHeaders and broadcasting them to the header-sub
// gossipsub network.
func (cl *Listener) listen(ctx context.Context, sub <-chan SignedBlock) {
	defer close(cl.closed)
	defer log.Info("listener: listening stopped")
	timeout := time.NewTimer(cl.listenerTimeout)
	defer timeout.Stop()
	for {
		select {
		case b, ok := <-sub:
			if !ok {
				log.Error("underlying subscription was closed")
				return
			}

			if cl.chainID != "" && b.Header.ChainID != cl.chainID {
				// stop node if there is a critical issue with the block subscription
				panic(fmt.Sprintf("listener: received block with unexpected chain ID: expected %s,"+
					" received %s. blockHeight: %d blockHash: %x.",
					cl.chainID, b.Header.ChainID, b.Header.Height, b.Header.Hash()),
				)
			}

			log.Debugw("listener: new block from core", "height", b.Header.Height)

			err := cl.handleNewSignedBlock(ctx, b)
			if err != nil {
				log.Errorw("listener: handling new block msg",
					"height", b.Header.Height,
					"hash", b.Header.Hash().String(),
					"err", err)
			}
		case <-timeout.C:
			cl.metrics.subscriptionStuck(ctx)
			log.Error("underlying subscription is stuck")
		case <-ctx.Done():
			return
		}
		timeout.Reset(cl.listenerTimeout)
	}
}

func (cl *Listener) handleNewSignedBlock(ctx context.Context, b SignedBlock) error {
	var err error

	ctx, span := tracer.Start(ctx, "listener/handleNewSignedBlock")
	defer func() {
		utils.SetStatusAndEnd(span, err)
	}()
	span.SetAttributes(
		attribute.Int64("height", b.Header.Height),
	)

	eds, err := da.ConstructEDS(b.Data.Txs.ToSliceOfBytes(), b.Header.Version.App, -1)
	if err != nil {
		return fmt.Errorf("extending block data: %w", err)
	}

	// generate extended header
	eh, err := cl.construct(b.Header, b.Commit, b.ValidatorSet, eds)
	if err != nil {
		panic(fmt.Errorf("making extended header: %w", err))
	}
	span.AddEvent("listener: constructed extended header",
		trace.WithAttributes(attribute.Int("square_size", eh.DAH.SquareSize())),
	)

	err = storeEDS(ctx, eh, eds, cl.store, cl.availabilityWindow, cl.archival)
	if err != nil {
		return fmt.Errorf("storing EDS: %w", err)
	}
	span.AddEvent("listener: stored square")

	// fan-out header to all active subscribers after EDS is stored
	cl.notifySubs(eh)

	syncing, err := cl.fetcher.IsSyncing(ctx)
	if err != nil {
		return fmt.Errorf("getting sync state: %w", err)
	}
	span.AddEvent("listener: fetched sync state")

	// notify network of new EDS hash only if core is already synced
	if !syncing {
		err = cl.hashBroadcaster(ctx, shrexsub.Notification{
			DataHash: eh.DataHash.Bytes(),
			Height:   eh.Height(),
		})
		if err != nil && !errors.Is(err, context.Canceled) {
			log.Errorw("listener: broadcasting data hash",
				"height", b.Header.Height,
				"datahash", eh.DAH.String(), "err", err)
		}
	}

	// broadcast new ExtendedHeader, but if core is still syncing, notify only local subscribers
	err = cl.headerBroadcaster.Broadcast(ctx, eh, pubsub.WithLocalPublication(syncing))
	if err != nil && !errors.Is(err, context.Canceled) {
		log.Errorw("listener: broadcasting next header",
			"height", b.Header.Height,
			"err", err)
	}
	return nil
}

// listenerSubscription implements libhead.Subscription backed by a per-subscriber
// buffered channel. Each subscription receives every header independently.
type listenerSubscription struct {
	ch       chan *header.ExtendedHeader
	cancelFn func()
	once     sync.Once
}

func (s *listenerSubscription) NextHeader(ctx context.Context) (*header.ExtendedHeader, error) {
	select {
	case h, ok := <-s.ch:
		if !ok {
			return nil, context.Canceled
		}
		return h, nil
	case <-ctx.Done():
		// Auto-cancel so the subscription is cleaned up even if the caller
		// never calls Cancel() explicitly (e.g. context cancelled by API).
		s.cancel()
		return nil, ctx.Err()
	}
}

func (s *listenerSubscription) Cancel() {
	s.cancel()
}

// cancel is the internal idempotent cleanup: unregisters the subscription and
// closes its channel exactly once regardless of how many times it is called.
func (s *listenerSubscription) cancel() {
	s.once.Do(s.cancelFn)
}
