package das

import (
	"context"

	"go.uber.org/fx"

	libhead "github.com/celestiaorg/go-header"

	corep "github.com/celestiaorg/celestia-node/core"
	"github.com/celestiaorg/celestia-node/das"
	"github.com/celestiaorg/celestia-node/header"
	"github.com/celestiaorg/celestia-node/nodebuilder/node"
)

func ConstructModule(tp node.Type, cfg *Config) fx.Option {
	// If DASer is disabled, provide the stub implementation for any node type
	if !cfg.Enabled {
		return fx.Module(
			"das",
			fx.Provide(newDaserStub),
		)
	}

	baseComponents := fx.Options(
		fx.Supply(*cfg),
		fx.Error(cfg.Validate()),
		fx.Provide(
			func(c Config) []das.Option {
				return []das.Option{
					das.WithSamplingRange(c.SamplingRange),
					das.WithConcurrencyLimit(c.ConcurrencyLimit),
					das.WithBackgroundStoreInterval(c.BackgroundStoreInterval),
					das.WithSampleTimeout(c.SampleTimeout),
				}
			},
		),
		fx.Provide(fx.Annotate(
			newDASer,
			fx.OnStart(func(ctx context.Context, daser *das.DASer) error {
				return daser.Start(ctx)
			}),
			fx.OnStop(func(ctx context.Context, daser *das.DASer) error {
				return daser.Stop(ctx)
			}),
		)),
		// Module is needed for the RPC handler
		fx.Provide(func(d *das.DASer) Module {
			return d
		}),
	)

	switch tp {
	case node.Bridge:
		// Bridge nodes receive headers directly from the core Listener after EDS is
		// stored locally. Override the p2p-based subscriber with the Listener so that
		// the DASer never races against the Listener for EDS availability.
		return fx.Module("das",
			baseComponents,
			fx.Decorate(func(l *corep.Listener) libhead.Subscriber[*header.ExtendedHeader] {
				return l
			}),
		)
	default:
		return fx.Module("das", baseComponents)
	}
}
