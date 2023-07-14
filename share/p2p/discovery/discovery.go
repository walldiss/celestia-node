package discovery

import (
	"context"
	"errors"
	"fmt"
	"time"

	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p/core/discovery"
	"github.com/libp2p/go-libp2p/core/event"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/host/eventbus"
	"golang.org/x/sync/errgroup"
)

var log = logging.Logger("share/discovery")

const (
	// rendezvousPoint is the namespace where peers advertise and discover each other.
	rendezvousPoint = "full"

	// eventbusBufSize is the size of the buffered channel to handle
	// events in libp2p. We specify a larger buffer size for the channel
	// to avoid overflowing and blocking subscription during disconnection bursts.
	// (by default it is 16)
	eventbusBufSize = 64

	// findPeersTimeout limits the FindPeers operation in time
	findPeersTimeout = time.Minute

	// retryTimeout defines time interval between discovery and advertise attempts.
	retryTimeout = time.Second
)

// discoveryRetryTimeout defines time interval between discovery attempts, needed for tests
var discoveryRetryTimeout = retryTimeout

// Discovery combines advertise and discover services and allows to store discovered nodes.
// TODO: The code here gets horribly hairy, so we should refactor this at some point
type Discovery struct {
	set       *limitedSet
	host      host.Host
	disc      discovery.Discovery
	connector *backoffConnector
	// onUpdatedPeers will be called on peer set changes
	onUpdatedPeers OnUpdatedPeers

	triggerDisc chan struct{}

	metrics *metrics

	cancel context.CancelFunc

	params Parameters
}

type OnUpdatedPeers func(peerID peer.ID, isAdded bool)

// NewDiscovery constructs a new discovery.
func NewDiscovery(
	h host.Host,
	d discovery.Discovery,
	opts ...Option,
) *Discovery {
	params := DefaultParameters()

	for _, opt := range opts {
		opt(&params)
	}

	return &Discovery{
		set:            newLimitedSet(params.PeersLimit),
		host:           h,
		disc:           d,
		connector:      newBackoffConnector(h, defaultBackoffFactory),
		onUpdatedPeers: func(peer.ID, bool) {},
		params:         params,
		triggerDisc:    make(chan struct{}),
	}
}

func (d *Discovery) Start(context.Context) error {
	ctx, cancel := context.WithCancel(context.Background())
	d.cancel = cancel

	if d.params.PeersLimit == 0 {
		log.Warn("peers limit is set to 0. Skipping discovery...")
		return nil
	}

	sub, err := d.host.EventBus().Subscribe(&event.EvtPeerConnectednessChanged{}, eventbus.BufSize(eventbusBufSize))
	if err != nil {
		return fmt.Errorf("subscribing for connection events: %w", err)
	}

	go d.discoveryLoop(ctx)
	go d.disconnectsLoop(ctx, sub)
	go d.connector.GC(ctx)
	return nil
}

func (d *Discovery) Stop(context.Context) error {
	d.cancel()
	return nil
}

// WithOnPeersUpdate chains OnPeersUpdate callbacks on every update of discovered peers list.
func (d *Discovery) WithOnPeersUpdate(f OnUpdatedPeers) {
	prev := d.onUpdatedPeers
	d.onUpdatedPeers = func(peerID peer.ID, isAdded bool) {
		prev(peerID, isAdded)
		f(peerID, isAdded)
	}
}

// Peers provides a list of discovered peers in the "full" topic.
// If Discovery hasn't found any peers, it blocks until at least one peer is found.
func (d *Discovery) Peers(ctx context.Context) ([]peer.ID, error) {
	return d.set.Peers(ctx)
}

// Discard removes the peer from the peer set and rediscovers more if soft peer limit is not
// reached. Reports whether peer was removed with bool.
func (d *Discovery) Discard(id peer.ID) bool {
	if !d.set.Contains(id) {
		return false
	}

	d.host.ConnManager().Unprotect(id, rendezvousPoint)
	d.connector.Backoff(id)
	d.set.Remove(id)
	d.onUpdatedPeers(id, false)
	log.Debugw("removed peer from the peer set", "peer", id.String())

	if d.set.Size() < d.set.Limit() {
		// trigger discovery
		select {
		case d.triggerDisc <- struct{}{}:
		default:
		}
	}

	return true
}

// Advertise is a utility function that persistently advertises a service through an Advertiser.
// TODO: Start advertising only after the reachability is confirmed by AutoNAT
func (d *Discovery) Advertise(ctx context.Context) {
	if d.params.AdvertiseInterval == -1 {
		log.Warn("AdvertiseInterval is set to -1. Skipping advertising...")
		return
	}

	timer := time.NewTimer(d.params.AdvertiseInterval)
	defer timer.Stop()
	for {
		_, err := d.disc.Advertise(ctx, rendezvousPoint)
		d.metrics.observeAdvertise(ctx, err)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Warnw("error advertising", "rendezvous", rendezvousPoint, "err", err)

			// we don't want retry indefinitely in busy loop
			// internal discovery mechanism may need some time before attempts
			errTimer := time.NewTimer(retryTimeout)
			select {
			case <-errTimer.C:
				errTimer.Stop()
				if !timer.Stop() {
					<-timer.C
				}
				continue
			case <-ctx.Done():
				errTimer.Stop()
				return
			}
		}

		log.Debugf("advertised")
		if !timer.Stop() {
			<-timer.C
		}
		timer.Reset(d.params.AdvertiseInterval)
		select {
		case <-timer.C:
		case <-ctx.Done():
			return
		}
	}
}

// discoveryLoop ensures we always have '~peerLimit' connected peers.
// It starts peer discovery per request and restarts the process until the soft limit reached.
func (d *Discovery) discoveryLoop(ctx context.Context) {
	t := time.NewTicker(discoveryRetryTimeout)
	defer t.Stop()
	for {
		// drain all previous ticks from channel
		drainChannel(t.C)
		select {
		case <-t.C:
			found := d.discover(ctx)
			if !found {
				// rerun discovery if amount of peers didn't reach the limit
				continue
			}
		case <-ctx.Done():
			return
		}

		select {
		case <-d.triggerDisc:
		case <-ctx.Done():
			return
		}
	}
}

// disconnectsLoop listen for disconnect events and ensures Discovery state
// is updated.
func (d *Discovery) disconnectsLoop(ctx context.Context, sub event.Subscription) {
	defer sub.Close()

	for {
		select {
		case <-ctx.Done():
			return
		case e, ok := <-sub.Out():
			if !ok {
				log.Error("connection subscription was closed unexpectedly")
				return
			}

			if evnt := e.(event.EvtPeerConnectednessChanged); evnt.Connectedness == network.NotConnected {
				d.Discard(evnt.Peer)
			}
		}
	}
}

// discover finds new peers and reports whether it succeeded.
func (d *Discovery) discover(ctx context.Context) bool {
	size := d.set.Size()
	want := d.set.Limit() - size
	if want == 0 {
		log.Debugw("reached soft peer limit, skipping discovery", "size", size)
		return true
	}
	// TODO @renaynay: eventually, have a mechanism to catch if wanted amount of peers
	//  has not been discovered in X amount of time so that users are warned of degraded
	//  FN connectivity.
	log.Debugw("discovering peers", "want", want)

	// we use errgroup as it provide limits
	var wg errgroup.Group
	// limit to minimize chances of overreaching the limit
	wg.SetLimit(int(d.set.Limit()))

	findCtx, findCancel := context.WithTimeout(ctx, findPeersTimeout)
	defer func() {
		// some workers could still be running, wait them to finish before canceling findCtx
		wg.Wait() //nolint:errcheck
		findCancel()
	}()

	peers, err := d.disc.FindPeers(findCtx, rendezvousPoint)
	if err != nil {
		log.Error("unable to start discovery", "err", err)
		return false
	}

	for {
		select {
		case p, ok := <-peers:
			if !ok {
				break
			}

			peer := p
			wg.Go(func() error {
				if findCtx.Err() != nil {
					log.Debug("find has been canceled, skip peer")
					return nil
				}

				// we don't pass findCtx so that we don't cancel in progress connections
				// that are likely to be valuable
				if !d.handleDiscoveredPeer(ctx, peer) {
					return nil
				}

				size := d.set.Size()
				log.Debugw("found peer", "peer", peer.ID.String(), "found_amount", size)
				if size < d.set.Limit() {
					return nil
				}

				log.Infow("discovered wanted peers", "amount", size)
				findCancel() // stop discovery when we are done
				return nil
			})

			continue
		case <-findCtx.Done():
		}

		isEnoughPeers := d.set.Size() >= d.set.Limit()
		d.metrics.observeFindPeers(ctx, isEnoughPeers)
		log.Debugw("discovery finished", "discovered_wanted", isEnoughPeers)
		return isEnoughPeers
	}
}

// handleDiscoveredPeer adds peer to the internal if can connect or is connected.
// Report whether it succeeded.
func (d *Discovery) handleDiscoveredPeer(ctx context.Context, peer peer.AddrInfo) bool {
	logger := log.With("peer", peer.ID.String())
	switch {
	case peer.ID == d.host.ID():
		d.metrics.observeHandlePeer(ctx, handlePeerSkipSelf)
		logger.Debug("skip handle: self discovery")
		return false
	case len(peer.Addrs) == 0:
		d.metrics.observeHandlePeer(ctx, handlePeerEmptyAddrs)
		logger.Debug("skip handle: empty address list")
		return false
	case d.set.Size() >= d.set.Limit():
		d.metrics.observeHandlePeer(ctx, handlePeerEnoughPeers)
		logger.Debug("skip handle: enough peers found")
		return false
	}

	switch d.host.Network().Connectedness(peer.ID) {
	case network.Connected:
		d.connector.Backoff(peer.ID) // we still have to backoff the connected peer
	case network.NotConnected:
		err := d.connector.Connect(ctx, peer)
		if errors.Is(err, errBackoffNotEnded) {
			d.metrics.observeHandlePeer(ctx, handlePeerBackoff)
			logger.Debug("skip handle: backoff")
			return false
		}
		if err != nil {
			d.metrics.observeHandlePeer(ctx, handlePeerConnErr)
			logger.Debugw("unable to connect", "err", err)
			return false
		}
	default:
		panic("unknown connectedness")
	}

	if !d.set.Add(peer.ID) {
		d.metrics.observeHandlePeer(ctx, handlePeerInSet)
		logger.Debug("peer is already in discovery set")
		return false
	}
	d.onUpdatedPeers(peer.ID, true)
	d.metrics.observeHandlePeer(ctx, handlePeerConnected)
	logger.Debug("added peer to set")

	// tag to protect peer from being killed by ConnManager
	// NOTE: This is does not protect from remote killing the connection.
	//  In the future, we should design a protocol that keeps bidirectional agreement on whether
	//  connection should be kept or not, similar to mesh link in GossipSub.
	d.host.ConnManager().Protect(peer.ID, rendezvousPoint)
	return true
}

func drainChannel(c <-chan time.Time) {
	for {
		select {
		case <-c:
		default:
			return
		}
	}
}
