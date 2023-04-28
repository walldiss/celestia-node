package discovery

import (
	"context"
	"sync"
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
	topic = "full"

	// eventbusBufSize is the size of the buffered channel to handle
	// events in libp2p
	eventbusBufSize = 32

	findPeersStuckWarnDelay = time.Minute
)

// waitF calculates time to restart announcing.
var waitF = func(ttl time.Duration) time.Duration {
	return 7 * ttl / 8
}

type Parameters struct {
	// PeersLimit defines the soft limit of FNs to connect to via discovery.
	// Set 0 to disable.
	PeersLimit int
	// DiscoveryInterval is an interval between discovery sessions.
	// Set -1 to disable.
	DiscoveryInterval time.Duration
	// AdvertiseInterval is a interval between advertising sessions.
	// Set -1 to disable.
	// NOTE: only full and bridge can advertise themselves.
	AdvertiseInterval time.Duration
}

func DefaultParameters() Parameters {
	return Parameters{
		PeersLimit:        5,
		DiscoveryInterval: time.Minute,
		AdvertiseInterval: time.Hour * 8,
	}
}

func (p *Parameters) Validate() error {
	return nil
}

// Discovery combines advertise and discover services and allows to store discovered nodes.
// TODO: The code here gets horribly hairy, so we should refactor this at some point
type Discovery struct {
	params Parameters

	set            *limitedSet
	host           host.Host
	disc           discovery.Discovery
	connector      *backoffConnector
	onUpdatedPeers OnUpdatedPeers

	connectingLk sync.Mutex
	connecting   map[peer.ID]context.CancelFunc

	cancel context.CancelFunc
}

type OnUpdatedPeers func(peerID peer.ID, isAdded bool)

// NewDiscovery constructs a new discovery.
func NewDiscovery(
	h host.Host,
	d discovery.Discovery,
	params Parameters,
) *Discovery {
	return &Discovery{
		params:         params,
		set:            newLimitedSet(params.PeersLimit),
		host:           h,
		disc:           d,
		connector:      newBackoffConnector(h, defaultBackoffFactory),
		onUpdatedPeers: func(peer.ID, bool) {},
		connecting:     make(map[peer.ID]context.CancelFunc),
	}
}

func (d *Discovery) Start(context.Context) error {
	ctx, cancel := context.WithCancel(context.Background())
	d.cancel = cancel
	go d.ensurePeers(ctx)
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

// handlePeersFound receives peers and tries to establish a connection with them.
// Peer will be added to PeerCache if connection succeeds.
func (d *Discovery) handlePeerFound(ctx context.Context, peer peer.AddrInfo, cancelFind context.CancelFunc) {
	if peer.ID == d.host.ID() ||
		len(peer.Addrs) == 0 ||
		d.set.Contains(peer.ID) ||
		d.set.Size() >= d.set.Limit() {
		return
	}

	if d.host.Network().Connectedness(peer.ID) == network.Connected {
		d.set.Add(peer.ID)
		log.Debugw("added peer to set", "id", peer.ID)
		if d.set.Size() >= d.set.Limit() {
			log.Infow("soft peer limit reached", "count", d.set.Size())
			cancelFind()
		}
		d.host.ConnManager().Protect(peer.ID, topic)
		d.connector.Backoff(peer.ID)

		// and notify our subscribers
		d.onUpdatedPeers(peer.ID, true)
		return
	}

	d.connectingLk.Lock()
	if _, ok := d.connecting[peer.ID]; ok {
		d.connectingLk.Unlock()
		return
	}
	d.connecting[peer.ID] = cancelFind
	d.connectingLk.Unlock()

	err := d.connector.Connect(ctx, peer)
	if err != nil {
		d.connectingLk.Lock()
		delete(d.connecting, peer.ID)
		d.connectingLk.Unlock()
		return
	}

	// tag to protect peer from being killed by ConnManager
	// NOTE: This is does not protect from remote killing the connection.
	//  In the future, we should design a protocol that keeps bidirectional agreement on whether
	//  connection should be kept or not, similar to mesh link in GossipSub.
	d.host.ConnManager().Protect(peer.ID, topic)
}

// ensurePeers ensures we always have 'peerLimit' connected peers.
// It starts peer discovery every 30 seconds until peer cache reaches peersLimit.
// Discovery is restarted if any previously connected peers disconnect.
func (d *Discovery) ensurePeers(ctx context.Context) {
	if d.params.PeersLimit == 0 || d.params.DiscoveryInterval == -1 {
		log.Warn("peers limit is set to 0 and/or discovery interval is set to -1. Skipping discovery...")
		return
	}
	// subscribe on EventBus in order to catch disconnected peers and restart
	// the discovery. We specify a larger buffer size for the channel where
	// EvtPeerConnectednessChanged events are sent (by default it is 16, we
	// specify 32) to avoid any blocks on writing to the full channel.
	sub, err := d.host.EventBus().Subscribe(&event.EvtPeerConnectednessChanged{}, eventbus.BufSize(eventbusBufSize))
	if err != nil {
		log.Error(err)
		return
	}
	defer sub.Close()

	// starting to listen to subscriptions async will help us to avoid any blocking
	// in the case when we will not have the needed amount of FNs and will be blocked in `FindPeers`.
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case e, ok := <-sub.Out():
				if !ok {
					return
				}
				// listen to disconnect event to remove peer from set and reset backoff time
				// reset timer in order to restart the discovery, once stored peer is disconnected
				evnt := e.(event.EvtPeerConnectednessChanged)
				switch evnt.Connectedness {
				case network.NotConnected:
					if !d.set.Contains(evnt.Peer) {
						continue
					}

					d.host.ConnManager().UntagPeer(evnt.Peer, topic)
					d.connector.ResetBackoff(evnt.Peer)
					d.set.Remove(evnt.Peer)
					d.onUpdatedPeers(evnt.Peer, false)
					log.Debugw("removed peer from the peer set",
						"peer", evnt.Peer, "status", evnt.Connectedness.String())
				case network.Connected:
					peerID := evnt.Peer
					d.connectingLk.Lock()
					cancelFind, ok := d.connecting[peerID]
					d.connectingLk.Unlock()
					if !ok {
						continue
					}

					log.Debugw("added peer to set", "id", peerID)
					d.set.Add(peerID)
					// and notify our subscribers
					d.onUpdatedPeers(peerID, true)

					// first do Add and only after check the limit
					// so that peer set represents the actual number of connections we made
					// which can go slightly over peersLimit
					if d.set.Size() >= d.set.Limit() {
						log.Infow("soft peer limit reached", "count", d.set.Size())
						cancelFind()
					}

					d.connectingLk.Lock()
					delete(d.connecting, peerID)
					d.connectingLk.Unlock()
				}
			}
		}
	}()
	go d.connector.GC(ctx)

	t := time.NewTicker(d.params.DiscoveryInterval)
	defer t.Stop()
	for {
		d.findPeers(ctx)
		if d.set.Size() < d.set.Limit() {
			// rerun discovery is amount of peers didn't reach the limit
			continue
		}

		t.Reset(d.params.DiscoveryInterval)
		select {
		case <-t.C:
		case <-ctx.Done():
			return
		}
	}
}

func (d *Discovery) findPeers(ctx context.Context) {
	if d.set.Size() >= d.set.Limit() {
		log.Debugw("reached soft peer limit, skipping discovery", "size", d.set.Size())
		return
	}
	log.Infow("below soft peer limit, discovering peers", "amount", d.set.Limit())

	// we use errgroup as it obeys the context
	wg := errgroup.Group{}
	// limit to minimize chances of overreaching the limit
	wg.SetLimit(d.set.Limit())
	defer wg.Wait() //nolint:errcheck

	// stop discovery when we are done
	findCtx, findCancel := context.WithCancel(ctx)
	defer findCancel()

	peers, err := d.disc.FindPeers(findCtx, topic)
	if err != nil {
		log.Warn(err)
		return
	}

	ticker := time.NewTicker(findPeersStuckWarnDelay)
	defer ticker.Stop()
	var amount int
	for {
		ticker.Reset(findPeersStuckWarnDelay)
		select {
		case <-findCtx.Done():
			log.Debugw("found enough peers", "amount", d.set.Size())
			return
		case <-ticker.C:
			log.Error("wasn't able to find new peers for long time")
			continue
		case p, ok := <-peers:
			if !ok {
				log.Debugw("discovery channel closed", "find_is_canceled", findCtx.Err() != nil)
				return
			}

			amount++
			peer := p
			log.Debugw("found peer", "found_amount", amount)
			wg.Go(func() error {
				if findCtx.Err() != nil {
					log.Debug("find has been canceled, skip peer")
					return nil
				}
				// pass the cancel so that we cancel FindPeers when we connected to enough peers
				d.handlePeerFound(ctx, peer, findCancel)
				return nil
			})
		}
	}
}

// Advertise is a utility function that persistently advertises a service through an Advertiser.
// TODO: Start advertising only after the reachability is confirmed by AutoNAT
func (d *Discovery) Advertise(ctx context.Context) {
	if d.params.AdvertiseInterval == -1 {
		return
	}

	timer := time.NewTimer(d.params.AdvertiseInterval)
	defer timer.Stop()
	for {
		ttl, err := d.disc.Advertise(ctx, topic)
		if err != nil {
			log.Debugf("Error advertising %s: %s", topic, err.Error())
			if ctx.Err() != nil {
				return
			}

			select {
			case <-timer.C:
				timer.Reset(d.params.AdvertiseInterval)
				continue
			case <-ctx.Done():
				return
			}
		}

		log.Debugf("advertised")
		select {
		case <-timer.C:
			timer.Reset(waitF(ttl))
		case <-ctx.Done():
			return
		}
	}
}

// Peers provides a list of discovered peers in the "full" topic.
// If Discovery hasn't found any peers, it blocks until at least one peer is found.
func (d *Discovery) Peers(ctx context.Context) ([]peer.ID, error) {
	return d.set.Peers(ctx)
}
