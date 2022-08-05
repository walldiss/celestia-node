package das

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
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

type Config struct {
	da     share.Availability
	bcast  fraud.Broadcaster
	hsub   header.Subscriber // listens for new headers in the network
	getter header.Getter     // retrieves past headers

	cstore datastore.Datastore // checkpoint store
}

// DASer continuously validates availability of data committed to headers.
type DASer struct {
	Config

	state         *samplingState
	sampleTimeout time.Duration
	concurrency   int
	stats         atomic.Value

	nextCh      chan uint64
	discoveryCh chan *header.ExtendedHeader
	retryCh     chan uint64

	cancel    context.CancelFunc
	closed    int32
	workersWg *sync.WaitGroup
	done      chan struct{}
}

type samplingState struct {
	priority         []uint64 //  priority for headers in order of increasing priority
	inProgress       map[uint64]struct{}
	maxKnownHeight   uint64
	sampleFromHeight uint64
}

// NewDASer creates a new DASer.
func NewDASer(
	da share.Availability,
	hsub header.Subscriber,
	getter header.Getter,
	cstore datastore.Datastore,
	bcast fraud.Broadcaster,
) *DASer {
	wrappedDS := wrapCheckpointStore(cstore)
	return &DASer{
		Config: Config{
			da:     da,
			bcast:  bcast,
			hsub:   hsub,
			getter: getter,
			cstore: wrappedDS,
		},
		state:         nil,
		sampleTimeout: 20 * time.Second, //TODO: add to config
		concurrency:   10,               //TODO: add to config
		// stats:         atomic.Value{},
		nextCh:      make(chan uint64, 16),
		discoveryCh: make(chan *header.ExtendedHeader),
		retryCh:     make(chan uint64),
		workersWg:   new(sync.WaitGroup),
		done:        make(chan struct{}),
	}
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
	checkpoint, err := loadCheckpoint(ctx, d.cstore)
	if err != nil {
		return err
	}
	log.Infow("loaded checkpoint", "height", checkpoint)

	runCtx, cancel := context.WithCancel(context.Background())
	d.cancel = cancel
	d.state = checkpoint.samplingState()

	// discover new headers
	go d.runDiscovery(runCtx, sub)
	// fan-out routine
	go d.samplingManager(runCtx)

	for i := 0; i < d.concurrency; i++ {
		go d.startWorker(runCtx, i)
	}

	return nil
}

// Stop stops sampling.
func (d *DASer) Stop(ctx context.Context) error {
	if atomic.CompareAndSwapInt32(&d.closed, 0, 1) {
		d.cancel()

		select {
		case <-d.done:
			// all workers are done, can store state now
			d.storeState(ctx, d.state)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
	//	// Stop func can now be invoked twice in one Lifecycle, when:
	//	// * BEFP is received;
	//	// * node is stopping;
	//	// this statement helps avoiding panic on the second Stop.
	//	// If ctx.Err is not nil then it means that Stop was called for the first time.
	//	// NOTE: we are expecting *only* ContextCancelled error here.
	//	//if d.ctx.Err() == context.Canceled {
	//	//	return nil
	//	//}
	//	//d.cancel()
	//	//// wait for both sampling routines to exit
	//	//for i := 0; i < 2; i++ {
	//	//	select {
	//	//	case <-d.catchUpDn:
	//	//	case <-d.sampleDn:
	//	//	case <-ctx.Done():
	//	//		return ctx.Err()
	//	//	}
	//	//}
	//	//d.cancel = nil
	//	//return nil
}

func (c checkPoint) samplingState() *samplingState {
	//sort in Asc order
	sort.Slice(c.queue, func(i, j int) bool { return c.queue[i] < c.queue[j] })
	return &samplingState{
		priority:         c.queue,
		inProgress:       make(map[uint64]struct{}),
		maxKnownHeight:   c.maxKnownHeight,
		sampleFromHeight: c.minSampledHeight,
	}
}

func (d *DASer) runDiscovery(ctx context.Context, sub header.Subscription) {
	for {
		h, err := sub.NextHeader(ctx)
		if err != nil {
			if err == context.Canceled {
				return
			}

			log.Errorw("failed to get next header", "err", err)
			continue
		}

		select {
		case d.discoveryCh <- h:
		case <-ctx.Done():
			return
		}
	}
}

func (d *DASer) samplingManager(ctx context.Context) {
	noop := make(chan uint64)
	for {
		// d.updateSampleStats() TODO:implement me

		nextCh := d.nextCh
		next, ok := d.state.nextHeight()
		if !ok {
			nextCh = noop
		}

		select {
		case nextCh <- next:
			d.state.setInProgress(next)
		case last := <-d.discoveryCh:
			d.state.setLast(uint64(last.Height))
		case failed := <-d.retryCh:
			d.state.retry(failed)
		case <-ctx.Done():
			close(d.nextCh)
			d.workersWg.Wait()
			return
		}
	}
}

func (d *DASer) startWorker(ctx context.Context, num int) {
	d.workersWg.Add(1)
	defer d.workersWg.Done()

	for height := range d.nextCh {
		log.Infow("fetching from worker", "height", height, "worker", num)
		err := d.fetch(ctx, height)
		log.Infow("fetched from worker", "worker", num, "error", err)
		if err != nil {
			select {
			case d.retryCh <- height: //TODO: no handling, just retry for now
			case <-ctx.Done():
				return
			}
		}
	}
}

func (d *DASer) fetch(ctx context.Context, height uint64) error {
	ctx, cancel := context.WithTimeout(ctx, d.sampleTimeout)
	defer cancel()

	h, err := d.getter.GetByHeight(ctx, uint64(height))
	if err != nil {
		return err
	}

	return d.sampleHeader(ctx, h)
}

func (s samplingState) retry(failed uint64) {
	delete(s.inProgress, failed)

	// put item back to priority for retry
	s.priority = append(s.priority, s.maxKnownHeight)

	return
}

func (s *samplingState) setLast(last uint64) {
	if s.maxKnownHeight == 0 {
		log.Infow("discovered first header, starting sampling")
		// there is no known header yet, start sampling over
		s.sampleFromHeight, s.maxKnownHeight = 1, last
		return
	}

	// put newly discovered heights with the highest priority
	// TODO:
	//  if header height advanced for more than 2147483647, it will panic.
	//  maybe if difference is high, just restart the process over from last,
	//  instead of blowing the priority slice. Imo, would keep as is
	for last > s.maxKnownHeight {
		s.maxKnownHeight++
		s.priority = append(s.priority, s.maxKnownHeight)
	}
}

func (s *samplingState) nextHeight() (uint64, bool) {
	if len(s.priority) > 0 {
		return s.priority[len(s.priority)-1], true
	}

	//TODO: is height == 0 possible to sample?
	if s.sampleFromHeight > 0 {
		return s.sampleFromHeight, true
	}

	return 0, false
}

func (s *samplingState) setInProgress(height uint64) {
	s.inProgress[height] = struct{}{}

	if height == s.sampleFromHeight {
		s.sampleFromHeight--
		return
	}

	var next uint64
	if len(s.priority) > 0 {
		next = s.priority[len(s.priority)-1]
		s.priority = s.priority[:len(s.priority)-1]
	}

	if next != height {
		log.Fatalf("unexpected next height. expected: %v, actual: %s ", next, height) //TODO:remove checks
	}
}

func (s *samplingState) checkPoint() checkPoint {
	for h := range s.inProgress {
		s.priority = append(s.priority, h)
	}
	return checkPoint{
		minSampledHeight: s.sampleFromHeight,
		maxKnownHeight:   s.maxKnownHeight,
		queue:            s.priority,
	}
}

func (d *DASer) storeState(ctx context.Context, s *samplingState) {
	// store latest DASed checkpoint to disk here to ensure that if DASer is not yet
	// fully caught up to network head, it will resume DASing from this checkpoint
	// up to current network head
	// TODO @renaynay: Implement Share Cache #180 to ensure no duplicate DASing over same
	//  header
	if err := storeCheckpoint(ctx, d.cstore, s.checkPoint()); err != nil {
		log.Errorw("storing checkpoint to disk", "err", err)
	}
	log.Infow("stored checkpoint to disk")
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

//
//// sample validates availability for each Header received from header subscription.
//func (d *DASer) sample(ctx context.Context, sub header.Subscription, checkpoint int64) {
//	// indicate sampling routine is running
//	d.indicateRunning()
//	defer func() {
//		sub.Cancel()
//		// indicate sampling routine is stopped
//		d.indicateStopped()
//		// send done signal
//		d.sampleDn <- struct{}{}
//	}()
//
//	// sampleHeight tracks the maxKnownHeight successful height of this routine
//	sampleHeight := checkpoint
//
//	for {
//		h, err := sub.NextHeader(ctx)
//		if err != nil {
//			if err == context.Canceled {
//				return
//			}
//
//			log.Errorw("failed to get next header", "err", err)
//			continue
//		}
//
//		// If the next header coming through gossipsub is not adjacent
//		// to our maxKnownHeight DASed header, kick off routine to DAS all headers
//		// between maxKnownHeight DASed header and h. This situation could occur
//		// either on start or due to network latency/disconnection.
//		if h.Height > sampleHeight+1 {
//			// DAS headers between maxKnownHeight DASed height up to the current
//			// header
//			job := &catchUpJob{
//				from: sampleHeight,
//				to:   h.Height - 1,
//			}
//			select {
//			case <-ctx.Done():
//				return
//			case d.jobsCh <- job:
//			}
//		}
//
//		err = d.sampleHeader(ctx, h)
//		if err != nil {
//			// record error
//			d.updateSampleState(h, err)
//			log.Warn("DASer SAMPLING ROUTINE WILL BE STOPPED. IN ORDER TO CONTINUE SAMPLING, " +
//				"RE-START THE NODE")
//			return
//		}
//
//		d.updateSampleState(h, nil)
//		sampleHeight = h.Height
//	}
//}
//
//// catchUpJob represents a catch-up job. (from:to]
//type catchUpJob struct {
//	from, to int64
//}

//// catchUpManager manages catch-up jobs, performing them one at a time, exiting
//// only once context is canceled and storing latest DASed checkpoint to disk.
//func (d *DASer) catchUpManager(ctx context.Context, checkpoint checkPoint) {
//
//	defer func() {
//		// store latest DASed checkpoint to disk here to ensure that if DASer is not yet
//		// fully caught up to network head, it will resume DASing from this checkpoint
//		// up to current network head
//		// TODO @renaynay: Implement Share Cache #180 to ensure no duplicate DASing over same
//		//  header
//		if err := storeCheckpoint(ctx, d.cstore, checkpoint); err != nil {
//			log.Errorw("storing checkpoint to disk", "height", checkpoint, "err", err)
//		}
//		log.Infow("stored checkpoint to disk", "checkpoint", checkpoint)
//		// signal that catch-up routine finished
//		d.catchUpDn <- struct{}{}
//	}()
//
//	for {
//		select {
//		case <-ctx.Done():
//			return
//		case job := <-d.jobsCh:
//			// record details of incoming job
//			d.recordJobDetails(job)
//			// perform catchUp routine
//			height, err := d.catchUp(ctx, job)
//			// store the height of the maxKnownHeight successfully sampled header
//			checkpoint = height
//			// exit routine if a catch-up job was unsuccessful
//			if err != nil {
//				// record error
//				d.state.catchUpLk.Lock()
//				d.state.catchUp.Error = err
//				d.state.catchUpLk.Unlock()
//
//				log.Errorw("catch-up routine failed", "attempted range: from", job.from, "to", job.to)
//				log.Warn("DASer CATCH-UP SAMPLING ROUTINE WILL BE STOPPED. IN ORDER TO CONTINUE SAMPLING, " +
//					"RE-START THE NODE")
//				return
//			}
//		}
//	}
//}
//
//// catchUp starts a sampling routine for headers starting at the next header
//// after the `from` height and exits the loop once `to` is reached. (from:to]
//func (d *DASer) catchUp(ctx context.Context, job *catchUpJob) (int64, error) {
//	log.Infow("sampling past headers", "from", job.from, "to", job.to)
//
//	// start sampling from height at checkpoint+1 since the
//	// checkpoint height is DASed by broader sample routine
//	for height := job.from + 1; height <= job.to; height++ {
//		h, err := d.getter.GetByHeight(ctx, uint64(height))
//		if err != nil {
//			if err == context.Canceled {
//				// report previous height as the maxKnownHeight successfully sampled height and
//				// error as nil since the routine was ordered to stop
//				return height - 1, nil
//			}
//
//			log.Errorw("failed to get next header", "height", height, "err", err)
//			// report previous height as the maxKnownHeight successfully sampled height
//			return height - 1, err
//		}
//
//		err = d.sampleHeader(ctx, h)
//		if err != nil {
//			return h.Height - 1, err
//		}
//		d.state.catchUpLk.Lock()
//		d.state.catchUp.Height = uint64(h.Height)
//		d.state.catchUpLk.Unlock()
//	}
//	d.state.catchUpLk.Lock()
//	d.state.catchUp.End = time.Now()
//	d.state.catchUpLk.Unlock()
//
//	jobDetails := d.CatchUpRoutineState()
//	log.Infow("successfully sampled past headers", "from", job.from,
//		"to", job.to, "finished (s)", jobDetails.Duration())
//	// report successful retry
//	return job.to, nil
//}
//
//// SampleRoutineState reports the current state of the
//// DASer's main sampling routine.
//func (d *DASer) SampleRoutineState() RoutineState {
//	d.state.sampleLk.RLock()
//	state := d.state.sample
//	d.state.sampleLk.RUnlock()
//	return state
//}

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
//func (d *DASer) indicateRunning() {
//	d.state.sampleLk.Lock()
//	defer d.state.sampleLk.Unlock()
//	d.state.sample.IsRunning = true
//}
//
//func (d *DASer) indicateStopped() {
//	d.state.sampleLk.Lock()
//	defer d.state.sampleLk.Unlock()
//	d.state.sample.IsRunning = false
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
//
//func (d *DASer) recordJobDetails(job *catchUpJob) {
//	d.state.catchUpLk.Lock()
//	defer d.state.catchUpLk.Unlock()
//	d.state.catchUp.ID++
//	d.state.catchUp.From = uint64(job.from)
//	d.state.catchUp.To = uint64(job.to)
//	d.state.catchUp.Start = time.Now()
//}
