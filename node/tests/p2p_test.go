package tests

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/libp2p/go-libp2p-core/event"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"

	"github.com/celestiaorg/celestia-node/node"
	"github.com/celestiaorg/celestia-node/node/tests/swamp"
)

/*
Test-Case: Full/Light Nodes connection to Bridge as a Bootstapper
Steps:
1. Create a Bridge Node(BN)
2. Start a BN
3. Create full/light nodes with bridge node as bootsrapped peer
4. Start full/light nodes
5. Check that nodes are connected to bridge
*/
func TestUseBridgeNodeAsBootstraper(t *testing.T) {
	sw := swamp.NewSwamp(t)

	bridge := sw.NewBridgeNode()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)

	err := bridge.Start(ctx)
	require.NoError(t, err)

	addr := host.InfoFromHost(bridge.Host)

	full := sw.NewFullNode(node.WithBootstrappers([]peer.AddrInfo{*addr}))
	light := sw.NewLightNode(node.WithBootstrappers([]peer.AddrInfo{*addr}))
	nodes := []*node.Node{full, light}
	for index := range nodes {
		require.NoError(t, nodes[index].Start(ctx))
		assert.Equal(t, *addr, nodes[index].Bootstrappers[0])
		assert.True(t, nodes[index].Host.Network().Connectedness(addr.ID) == network.Connected)
	}
}

/*
 Test-Case: Add peer to blacklist
 Steps:
 1. Create a Full Node(BN)
 2. Start a FN
 3. Create a Light Node(LN)
 5. Start a LN
 6. Explicitly block FN id by LN
 7. Check FN is allowed to dial with LN
 8. Check LN is not allowed to dial with FN
*/
func TestAddPeerToBlackList(t *testing.T) {
	sw := swamp.NewSwamp(t)
	full := sw.NewFullNode()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	require.NoError(t, full.Start(ctx))

	addr := host.InfoFromHost(full.Host)
	light := sw.NewLightNode()
	require.NoError(t, light.Start(ctx))
	require.NoError(t, light.ConnGater.BlockPeer(addr.ID))

	require.True(t, full.ConnGater.InterceptPeerDial(host.InfoFromHost(light.Host).ID))
	require.False(t, light.ConnGater.InterceptPeerDial(addr.ID))
}

/*
Test-Case: Connect Full And Light using Bridge node as a bootstrapper
Steps:
1. Create a Bridge Node(BN)
2. Start a BN
3. Create full/light nodes with bridge node as bootsrapped peer
4. Start full/light nodes
5. Ensure that nodes are connected to bridge
6. Wait until light will find full node
7. Check that full and light nodes are connected to each other
8. Stop FN and ensure that it's not connected to LN
*/
func TestBootstrapNodesFromBridgeNode(t *testing.T) {
	sw := swamp.NewSwamp(t)
	cfg := node.DefaultConfig(node.Bridge)
	cfg.P2P.Bootstrapper = true
	const defaultTimeInterval = time.Second * 10
	var defaultOptions = []node.Option{
		node.WithRefreshRoutingTablePeriod(defaultTimeInterval),
		node.WithDiscoveryInterval(defaultTimeInterval),
		node.WithAdvertiseInterval(defaultTimeInterval),
	}

	bridgeConfig := append([]node.Option{node.WithConfig(cfg)}, defaultOptions...)
	bridge := sw.NewBridgeNode(bridgeConfig...)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)

	err := bridge.Start(ctx)
	require.NoError(t, err)
	addr := host.InfoFromHost(bridge.Host)

	nodesConfig := append([]node.Option{node.WithBootstrappers([]peer.AddrInfo{*addr})}, defaultOptions...)
	full := sw.NewFullNode(nodesConfig...)
	light := sw.NewLightNode(nodesConfig...)
	nodes := []*node.Node{full, light}
	ch := make(chan struct{})
	sub, err := light.Host.EventBus().Subscribe(&event.EvtPeerConnectednessChanged{})
	require.NoError(t, err)
	defer sub.Close()
	for index := range nodes {
		require.NoError(t, nodes[index].Start(ctx))
		assert.Equal(t, *addr, nodes[index].Bootstrappers[0])
		assert.True(t, nodes[index].Host.Network().Connectedness(addr.ID) == network.Connected)
	}
	addrFull := host.InfoFromHost(full.Host)
	go func() {
		for e := range sub.Out() {
			connStatus := e.(event.EvtPeerConnectednessChanged)
			if connStatus.Peer == full.Host.ID() {
				ch <- struct{}{}
			}
		}
	}()

	select {
	case <-ctx.Done():
		t.Fatal("peer was not found")
	case <-ch:
		assert.True(t, light.Host.Network().Connectedness(addrFull.ID) == network.Connected)
	}

	sw.Disconnect(t, light.Host.ID(), full.Host.ID())
	require.NoError(t, full.Stop(ctx))
	select {
	case <-ctx.Done():
		t.Fatal("peer was not disconnected")
	case <-ch:
		assert.True(t, light.Host.Network().Connectedness(addrFull.ID) == network.NotConnected)
	}
}

/*
Test-Case: Restart full node discovery after one node is disconnected
Steps:
1. Create a Bridge Node(BN)
2. Start a BN
3. Create 2 full nodes with bridge node as bootstrapper peer and start them
4. Check that nodes are connected to each other
5. Create one more node with disabled discovery
6. Disconnect FNs from each other
7. Check that the last FN is connected to one of the nodes
*NOTE*: this test will take some time because it relies on several cycles of peer discovery
*/
func TestRestartNodeDiscovery(t *testing.T) {
	sw := swamp.NewSwamp(t)
	cfg := node.DefaultConfig(node.Bridge)
	cfg.P2P.Bootstrapper = true
	const defaultTimeInterval = time.Second * 2
	const fullNodes = 2
	var defaultOptions = []node.Option{
		node.WithPeersLimit(fullNodes),
		node.WithRefreshRoutingTablePeriod(defaultTimeInterval),
		node.WithDiscoveryInterval(defaultTimeInterval),
		node.WithAdvertiseInterval(defaultTimeInterval),
	}
	bridgeConfig := append([]node.Option{node.WithConfig(cfg)}, defaultOptions...)
	bridge := sw.NewBridgeNode(bridgeConfig...)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)

	err := bridge.Start(ctx)
	require.NoError(t, err)
	addr := host.InfoFromHost(bridge.Host)
	nodes := make([]*node.Node, fullNodes)
	nodesConfig := append([]node.Option{node.WithBootstrappers([]peer.AddrInfo{*addr})}, defaultOptions...)
	for index := 0; index < fullNodes; index++ {
		nodes[index] = sw.NewFullNode(nodesConfig...)
	}

	identitySub, err := nodes[0].Host.EventBus().Subscribe(&event.EvtPeerIdentificationCompleted{})
	require.NoError(t, err)
	defer identitySub.Close()

	for index := 0; index < fullNodes; index++ {
		require.NoError(t, nodes[index].Start(ctx))
		assert.True(t, nodes[index].Host.Network().Connectedness(addr.ID) == network.Connected)
	}

	// wait until full nodes connect each other
	e := <-identitySub.Out()
	connStatus := e.(event.EvtPeerIdentificationCompleted)
	id := connStatus.Peer
	if id != nodes[1].Host.ID() {
		t.Fatal("unexpected peer connected")
	}
	require.True(t, nodes[0].Host.Network().Connectedness(id) == network.Connected)

	// create one more node with disabled discovery
	nodesConfig[1] = node.WithPeersLimit(0)
	node := sw.NewFullNode(nodesConfig...)
	connectSub, err := nodes[0].Host.EventBus().Subscribe(&event.EvtPeerConnectednessChanged{})
	require.NoError(t, err)
	defer connectSub.Close()
	sw.Disconnect(t, nodes[0].Host.ID(), nodes[1].Host.ID())
	require.NoError(t, node.Start(ctx))
	for {
		select {
		case <-ctx.Done():
			require.True(t, nodes[0].Host.Network().Connectedness(node.Host.ID()) == network.Connected)
		case conn := <-connectSub.Out():
			status := conn.(event.EvtPeerConnectednessChanged)
			if status.Peer != node.Host.ID() {
				continue
			}
			require.True(t, status.Connectedness == network.Connected)
			return
		}
	}
}
