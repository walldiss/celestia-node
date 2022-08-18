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

const (
	priorityBufferSize = 1024
	concurrency        = 256
	jobsBufferSize     = 16
	storeInterval      = 10 * time.Minute
)

type Config struct {
	da     share.Availability
	bcast  fraud.Broadcaster
	hsub   header.Subscriber   // listens for new headers in the network
	getter header.Getter       // retrieves past headers
	cstore datastore.Datastore // checkpoint store
}

// DASer continuously validates availability of data committed to headers.
type DASer struct {
	Config

	sampler *samplingManager

	cancel         context.CancelFunc
	subscriberDone chan struct{}
	closed         int32
}

type result struct {
	height uint64
	err    error
}

type listenFn func(ctx context.Context, height uint64)

// NewDASer creates a new DASer.
func NewDASer(
	da share.Availability,
	hsub header.Subscriber,
	getter header.Getter,
	cstore datastore.Datastore,
	bcast fraud.Broadcaster,
) *DASer {
	d := &DASer{
		Config: Config{
			da:     da,
			bcast:  bcast,
			hsub:   hsub,
			getter: getter,
			cstore: wrapCheckpointStore(cstore),
		},
		subscriberDone: make(chan struct{}),
	}
	d.sampler = newSamplingManager(concurrency, jobsBufferSize, storeInterval, d.fetch, d.storeState)

	return d
}

// Start initiates subscription for new ExtendedHeaders and spawns a sampling routine.
func (d *DASer) Start(ctx context.Context) error {
	if d.cancel != nil {
		return fmt.Errorf("da: DASer already started")
	}

	sub, err := d.hsub.Subscribe()
	if err != nil {
		return err
	}

	// load latest DASed checkpoint
	cp, err := loadCheckpoint(ctx, d.cstore)
	if err != nil {
		h, err := d.getter.Head(ctx)
		if err == nil {
			cp = checkpoint{
				MinSampled: 1,
				MaxKnown:   uint64(h.Height),
			}

			log.Warnw("set max known height from store", "height", cp.MaxKnown)
		}
	}
	log.Infow("loaded checkpoint", "height", cp)

	runCtx, cancel := context.WithCancel(context.Background())
	d.cancel = cancel

	go d.sampler.run(runCtx, cp)
	// subscribe to new headers
	go d.runSubscriber(runCtx, sub, d.sampler.listen)

	return nil
}

// Stop stops sampling.
func (d *DASer) Stop(ctx context.Context) error {
	if atomic.CompareAndSwapInt32(&d.closed, 0, 1) {
		d.cancel()

		// shutdown the sampler
		if err := d.sampler.stop(ctx); err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				return errors.New("DASer force quit")
			}
			return err
		}

		// wait for subscriber to quit
		select {
		case <-d.subscriberDone:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (d *DASer) fetch(ctx context.Context, height uint64) error {
	h, err := d.getter.GetByHeight(ctx, height)
	if err != nil {
		return err
	}

	return d.sampleHeader(ctx, h)
}

func (d *DASer) storeState(ctx context.Context, s state) {
	// store latest DASed checkpoint to disk here to ensure that if DASer is not yet
	// fully caught up to network head, it will resume DASing from this checkpoint
	// up to current network head
	if err := storeCheckpoint(ctx, d.cstore, s.checkPoint()); err != nil {
		log.Errorw("storing checkpoint to disk", "err", err)
	}
	log.Infow("stored checkpoint to disk", "checkpoint", s.checkPoint())
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
				log.Errorw("fraud proof propagating failed", "err", sendErr)
			}
		}
		log.Errorw("sampling failed", "height", h.Height, "hash", h.Hash(),
			"square width", len(h.DAH.RowsRoots), "data root", h.DAH.Hash(), "err", err)
		// report previous height as the last successfully sampled height
		return err
	}

	sampleTime := time.Since(startTime)
	log.Infow("sampled header", "height", h.Height, "hash", h.Hash(),
		"square width", len(h.DAH.RowsRoots), "finished (s)", sampleTime.Seconds())

	return nil
}

func (d *DASer) runSubscriber(ctx context.Context, sub header.Subscription, emit listenFn) {
	defer sub.Cancel()

	for {
		// subscribe for new headers to keep sampling process up-to-date with current network state
		h, err := sub.NextHeader(ctx)
		if err != nil {
			if err == context.Canceled {
				close(d.subscriberDone)
				return
			}

			log.Errorw("failed to get next header", "err", err)
			continue
		}
		log.Infow("found new header", "height", h.Height)

		emit(ctx, uint64(h.Height))
	}
}

func runWorker(ctx context.Context,
	inCh <-chan uint64,
	outCh chan<- result,
	fetch func(ctx2 context.Context, uint642 uint64) error,
	num int) {
	log.Infow("started worker ", "num", num)

	for height := range inCh {
		err := fetch(ctx, height)

		select {
		case outCh <- result{
			height: height,
			err:    err,
		}:
		case <-ctx.Done():
			return
		}
	}
}
func (d *DASer) SampleRoutineState() RoutineState {
	return RoutineState{}
}

func (d *DASer) CatchUpRoutineState() JobInfo {
	return JobInfo{}
}

//
//func (d *DASer) updateSampleState(h *header.ExtendedHeader, err error) {
//	height := uint64(h.Height)
//
//	d.state.sampleLk.Lock()
//	defer d.state.sampleLk.Unlock()
//	d.state.sample.LatestSampledHeight = height
//	d.state.sample.LatestSampledSquareWidth = uint64(len(h.DAH.RowsRoots))
//	d.state.sample.Error = err
//}
//
//// CatchUpRoutineState reports the current state of the
//// DASer's `catchUp` routine.
//func (d *DASer) CatchUpRoutineState() JobInfo {
//	d.state.catchUpLk.RLock()
//	state := d.state.catchUp
//	d.state.catchUpLk.RUnlock()
//	return state
//}
//
//func (d *DASer) recordJobDetails(job *catchUpJob) {
//	d.state.catchUpLk.Lock()
//	defer d.state.catchUpLk.Unlock()
//	d.state.catchUp.ID++
//	d.state.catchUp.From = uint64(job.from)
//	d.state.catchUp.To = uint64(job.to)
//	d.state.catchUp.Start = time.Now()
//}
