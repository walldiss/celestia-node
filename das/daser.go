package das

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/ipfs/go-datastore"
	logging "github.com/ipfs/go-log/v2"

	"github.com/celestiaorg/celestia-node/fraud"
	"github.com/celestiaorg/celestia-node/header"
	"github.com/celestiaorg/celestia-node/ipld"
	"github.com/celestiaorg/celestia-node/service/share"
)

var log = logging.Logger("das")

// TODO: parameters needs performance testing on real network to define optimal values
const (
	samplingRange           = 64
	defaultConcurrencyLimit = 64
	storeInterval           = 10 * time.Minute
)

// DASer continuously validates availability of data committed to headers.
type DASer struct {
	da     share.Availability
	bcast  fraud.Broadcaster
	hsub   header.Subscriber // listens for new headers in the network
	getter header.Getter     // retrieves past headers

	sampler       *samplingCoordinator
	intervalStore backgroundStore
	subscriber    subscriber

	cancel         context.CancelFunc
	subscriberDone chan struct{}
	running        int32
}

type listenFn func(ctx context.Context, height uint64)
type fetchFn func(context.Context, uint64) error
type storeFn func(context.Context, checkpoint)

// NewDASer creates a new DASer.
func NewDASer(
	da share.Availability,
	hsub header.Subscriber,
	getter header.Getter,
	dstore datastore.Datastore,
	bcast fraud.Broadcaster,
) *DASer {
	d := &DASer{
		da:             da,
		bcast:          bcast,
		hsub:           hsub,
		getter:         getter,
		subscriberDone: make(chan struct{}),
	}

	cstore := wrapCheckpointStore(dstore)
	d.sampler = newSamplingCoordinator(defaultConcurrencyLimit, d.fetch, cstore)
	d.intervalStore = newBackgroundStore(cstore, d.sampler.getCheckpoint)
	d.subscriber = newSubscriber()

	return d
}

// Start initiates subscription for new ExtendedHeaders and spawns a sampling routine.
func (d *DASer) Start(ctx context.Context) error {
	if !atomic.CompareAndSwapInt32(&d.running, 0, 1) {
		return fmt.Errorf("da: DASer already started")
	}

	sub, err := d.hsub.Subscribe()
	if err != nil {
		return err
	}

	// load latest DASed checkpoint
	cp, err := d.sampler.store.load(ctx)
	if err != nil {
		h, err := d.getter.Head(ctx)
		if err == nil {
			cp = checkpoint{
				SampledBefore: 1,
				MaxKnown:      uint64(h.Height),
			}

			log.Warnw("checkpoint not found, initializing with height 1")
		}
	}
	log.Info("loaded checkpoint:\n", cp.String())

	d.sampler.state = initSamplingState(samplingRange, cp)

	runCtx, cancel := context.WithCancel(context.Background())
	d.cancel = cancel

	go d.sampler.run(runCtx)
	go d.subscriber.run(runCtx, sub, d.sampler.listen)
	go d.intervalStore.run(runCtx, storeInterval)

	return nil
}

// Stop stops sampling.
func (d *DASer) Stop(ctx context.Context) error {
	if !atomic.CompareAndSwapInt32(&d.running, 1, 0) {
		return fmt.Errorf("da: DASer already stopped")
	}

	d.cancel()
	if err := d.sampler.finished(ctx); err != nil {
		return fmt.Errorf("DASer force quit: %w", err)
	}

	if err := d.intervalStore.wait(ctx); err != nil {
		return fmt.Errorf("DASer force quit: %w", err)
	}

	return d.subscriber.wait(ctx)
}

func (d *DASer) fetch(ctx context.Context, sampleHeight uint64) error {
	h, err := d.getter.GetByHeight(ctx, sampleHeight)
	if err != nil {
		return fmt.Errorf("getting: %w", err)
	}
	if err = d.sampleHeader(ctx, h); err != nil {
		return fmt.Errorf("sampling: %w", err)
	}
	return nil
}

func (d *DASer) sampleHeader(ctx context.Context, h *header.ExtendedHeader) error {
	if h == nil {
		return nil
	}

	startTime := time.Now()

	err := d.da.SharesAvailable(ctx, h.DAH)
	if err != nil {
		if err == context.Canceled {
			return nil
		}
		var byzantineErr *ipld.ErrByzantine
		if errors.As(err, &byzantineErr) {
			log.Warn("Propagating proof...")
			sendErr := d.bcast.Broadcast(ctx, fraud.CreateBadEncodingProof(h.Hash(), uint64(h.Height), byzantineErr))
			if sendErr != nil {
				log.Errorw("fraud proof propagating failed", "Err", sendErr)
			}
		}
		log.Errorw("sampling failed", "height", h.Height, "hash", h.Hash(),
			"square width", len(h.DAH.RowsRoots), "data root", h.DAH.Hash(), "Err", err)
		// report previous height as the last successfully sampled height
		return err
	}

	sampleTime := time.Since(startTime)
	log.Infow("sampled header", "height", h.Height, "hash", h.Hash(),
		"square width", len(h.DAH.RowsRoots), "finished (s)", sampleTime.Seconds())

	return nil
}

func (d *DASer) SamplingStats(ctx context.Context) (SamplingStats, error) {
	return d.sampler.getStats(ctx)
}

type done struct {
	name     string
	finished chan struct{}
}

func newDone(name string) done {
	return done{
		name:     name,
		finished: make(chan struct{}),
	}
}

func (sm *done) Done() {
	close(sm.finished)
}
func (sm *done) wait(ctx context.Context) error {
	select {
	case <-sm.finished:
	case <-ctx.Done():
		return fmt.Errorf("%v stuck: %w", sm.name, ctx.Err())
	}
	return nil
}
