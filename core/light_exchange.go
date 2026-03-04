package core

import (
	"context"
	"fmt"

	libhead "github.com/celestiaorg/go-header"

	"github.com/celestiaorg/celestia-node/header"
)

// LightExchange wraps a P2P exchange with a core exchange fallback for Head().
// It is used by light nodes with a core endpoint configured to provide
// a fallback head source when all P2P bootstrap peers are unreachable.
type LightExchange struct {
	p2p  libhead.Exchange[*header.ExtendedHeader]
	core *Exchange
}

// NewLightExchange creates a new LightExchange.
func NewLightExchange(
	p2p libhead.Exchange[*header.ExtendedHeader],
	core *Exchange,
) *LightExchange {
	return &LightExchange{
		p2p:  p2p,
		core: core,
	}
}

// Head tries P2P first, falls back to core if P2P fails.
func (le *LightExchange) Head(
	ctx context.Context,
	opts ...libhead.HeadOption[*header.ExtendedHeader],
) (*header.ExtendedHeader, error) {
	head, err := le.p2p.Head(ctx, opts...)
	if err == nil {
		return head, nil
	}
	log.Warnw("p2p head request failed, falling back to core endpoint", "error", err)
	coreHead, coreErr := le.core.Head(ctx, opts...)
	if coreErr != nil {
		return nil, fmt.Errorf("p2p: %w; core: %w", err, coreErr)
	}
	return coreHead, nil
}

// Get delegates to P2P exchange.
func (le *LightExchange) Get(ctx context.Context, hash libhead.Hash) (*header.ExtendedHeader, error) {
	return le.p2p.Get(ctx, hash)
}

// GetByHeight delegates to P2P exchange.
func (le *LightExchange) GetByHeight(ctx context.Context, height uint64) (*header.ExtendedHeader, error) {
	return le.p2p.GetByHeight(ctx, height)
}

// GetRangeByHeight delegates to P2P exchange.
func (le *LightExchange) GetRangeByHeight(
	ctx context.Context,
	from *header.ExtendedHeader,
	to uint64,
) ([]*header.ExtendedHeader, error) {
	return le.p2p.GetRangeByHeight(ctx, from, to)
}
