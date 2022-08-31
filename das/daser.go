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
	// samplingCoordinator will paginate headers into jobs with fixed size.
	//  samplingRange is the maximum amount of headers processed in one job.
	samplingRange = 64

	// defaultConcurrencyLimit defines maximum amount of sampling workers running in parallel.
	defaultConcurrencyLimit = 16

	// backgroundStoreInterval is e period of time for background store to perform a checkpoint backup.
	backgroundStoreInterval = 10 * time.Minute

	// priorityCap defines size limit of priority queue
	priorityCap = defaultConcurrencyLimit * 4
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
type sampleFn func(context.Context, uint64) error

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
	d.sampler = newSamplingCoordinator(defaultConcurrencyLimit, d.sample, cstore)
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
		log.Warnw("checkpoint not found, initializing with height 1")

		cp = checkpoint{
			SampledBefore: 1,
			MaxKnown:      1,
		}

		if h, err := d.getter.Head(ctx); err == nil {
			cp.MaxKnown = uint64(h.Height)
		}
	}
	log.Info("starting DASer from checkpoint:\n", cp.String())

	d.sampler.state = initCoordinatorState(samplingRange, cp)

	runCtx, cancel := context.WithCancel(context.Background())
	d.cancel = cancel

	go d.sampler.run(runCtx)
	go d.subscriber.run(runCtx, sub, d.sampler.listen)
	go d.intervalStore.run(runCtx, backgroundStoreInterval)

	return nil
}

// Stop stops sampling.
func (d *DASer) Stop(ctx context.Context) error {
	if !atomic.CompareAndSwapInt32(&d.running, 1, 0) {
		return nil
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

func (d *DASer) sample(ctx context.Context, sampleHeight uint64) error {
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
