package pruner

import (
	"context"
	"fmt"
	"time"

	"github.com/ipfs/go-datastore"
	logging "github.com/ipfs/go-log/v2"

	hdr "github.com/celestiaorg/go-header"

	"github.com/celestiaorg/celestia-node/header"
)

var log = logging.Logger("pruner/service")

// Service handles the pruning routine for the node using the
// prune Pruner.
type Service struct {
	pruner Pruner
	window AvailabilityWindow

	getter hdr.Getter[*header.ExtendedHeader] // TODO @renaynay: expects a header service with access to sync head

	checkpoint        *checkpoint
	failedHeaders     map[uint64]error
	maxPruneablePerGC uint64
	numBlocksInWindow uint64

	ctx    context.Context
	cancel context.CancelFunc
	doneCh chan struct{}

	params  Params
	metrics *metrics
}

func NewService(
	p Pruner,
	window AvailabilityWindow,
	getter hdr.Getter[*header.ExtendedHeader],
	ds datastore.Datastore,
	blockTime time.Duration,
	opts ...Option,
) *Service {
	params := DefaultParams()
	for _, opt := range opts {
		opt(&params)
	}

	// TODO @renaynay
	numBlocksInWindow := uint64(time.Duration(window) / blockTime)

	return &Service{
		pruner:            p,
		window:            window,
		getter:            getter,
		checkpoint:        newCheckpoint(ds),
		numBlocksInWindow: numBlocksInWindow,
		// TODO @distractedmind: make this configurable?
		maxPruneablePerGC: numBlocksInWindow * 2,
		doneCh:            make(chan struct{}),
		params:            params,
	}
}

func (s *Service) Start(context.Context) error {
	s.ctx, s.cancel = context.WithCancel(context.Background())

	err := s.loadCheckpoint(s.ctx)
	if err != nil {
		return err
	}

	go s.prune()
	return nil
}

func (s *Service) Stop(ctx context.Context) error {
	s.cancel()

	select {
	case <-s.doneCh:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("pruner unable to exit within context deadline")
	}
}

func (s *Service) prune() {
	if s.params.gcCycle == time.Duration(0) {
		// Service is disabled, exit
		close(s.doneCh)
		return
	}

	ticker := time.NewTicker(s.params.gcCycle)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			close(s.doneCh)
			return
		case <-ticker.C:
			target, err := s.findPruneableTarget(s.ctx)
			if err != nil {
				// TODO @renaynay: record + report errors properly
				// TODO @walldiss: not critical, we will retry on next tick
				continue
			}

			from, to := s.lastPruned().Height(), target.Height()
			for height := from; height < to; height++ {
				err = s.pruneHeight(s.ctx, height)
				if err != nil {
					// TODO: @distractedm1nd: updatecheckpoint should be called on the last NON-ERRORED header
					log.Errorf("failed to prune header %d: %s", height, err)
					s.failedHeaders[height] = err
				}
				s.metrics.observePrune(s.ctx, err != nil)
			}

			err = s.updateCheckpoint(s.ctx, target)
			if err != nil {
				// TODO @renaynay: record + report errors properly
				continue
			}
		}
	}
}

// TODO: @walldiss: this should be Pruner interface
func (s *Service) pruneHeight(ctx context.Context, height uint64) error {
	// TODO @renaynay: make deadline a param ? / configurable?
	ctx, cancel := context.WithDeadline(s.ctx, time.Now().Add(time.Minute))
	defer cancel()

	eh, err := s.getter.GetByHeight(ctx, height)
	if err != nil {
		// header not found, so no data stored, we can skip pruning of this height
		log.Debugf("pruner: header not found at height %d", height)
		return nil
	}
	return s.pruner.Prune(ctx, eh)
}
