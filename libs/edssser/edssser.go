package edssser

import (
	"context"
	"errors"
	"fmt"
	"github.com/celestiaorg/celestia-app/pkg/wrapper"
	"github.com/celestiaorg/celestia-node/share"
	"github.com/celestiaorg/celestia-node/share/ipld"
	"github.com/celestiaorg/celestia-node/share/sharetest"
	"github.com/celestiaorg/nmt"
	"github.com/celestiaorg/rsmt2d"
	"sync"
	"testing"
	"time"

	"github.com/ipfs/go-datastore"

	"github.com/celestiaorg/celestia-app/pkg/da"

	"github.com/celestiaorg/celestia-node/share/eds"
)

type Config struct {
	EDSSize     int
	EDSWrites   int
	EnableLog   bool
	StatLogFreq int
	OpTimeout   time.Duration
}

// EDSsser stand for EDS Store Stresser.
type EDSsser struct {
	config     Config
	datastore  datastore.Batching
	edsstoreMu sync.Mutex
	edsstore   *eds.Store
}

func NewEDSsser(path string, datastore datastore.Batching, cfg Config) (*EDSsser, error) {
	edsstore, err := eds.NewStore(path, datastore)
	if err != nil {
		return nil, err
	}

	return &EDSsser{
		config:    cfg,
		datastore: datastore,
		edsstore:  edsstore,
	}, nil
}

func (ss *EDSsser) Run(ctx context.Context) (stats Stats, err error) {
	ss.edsstoreMu.Lock()
	defer ss.edsstoreMu.Unlock()

	err = ss.edsstore.Start(ctx)
	if err != nil {
		return stats, err
	}
	defer func() {
		err = errors.Join(err, ss.edsstore.Stop(ctx))
	}()

	edsHashes, err := ss.edsstore.List()
	if err != nil {
		return stats, err
	}
	fmt.Printf("recovered %d EDSes\n\n", len(edsHashes))

	t := &testing.T{}
	for toWrite := ss.config.EDSWrites - len(edsHashes); ctx.Err() == nil && toWrite > 0; toWrite-- {
		took, err := ss.put(ctx, t)

		stats.TotalWritten++
		stats.TotalTime += took
		if took < stats.MinTime || stats.MinTime == 0 {
			stats.MinTime = took
		} else if took > stats.MaxTime {
			stats.MaxTime = took
		}

		if ss.config.EnableLog {
			if stats.TotalWritten%ss.config.StatLogFreq == 0 {
				fmt.Println(stats.Finalize())
			}
			if err != nil {
				fmt.Println("ERROR", err.Error(), "took", took, "at", time.Now())
				return
			}
			if took > time.Second*20 {
				fmt.Println("long put", "size", ss.config.EDSSize, "took", took, "at", time.Now())
				return
			}

			fmt.Println("square written", "size", ss.config.EDSSize, "took", took, "at", time.Now())
		}
	}

	return stats, nil
}

func (ss *EDSsser) put(ctx context.Context, t *testing.T) (time.Duration, error) {
	ctx, cancel := context.WithTimeout(ctx, ss.config.OpTimeout)
	defer cancel()

	// divide by 2 to get ODS size as expected by RandEDS
	odsSize := ss.config.EDSSize / 2
	shares := sharetest.RandShares(t, odsSize*odsSize)
	adder := ipld.NewProofsAdder(odsSize * 2)
	square, err := rsmt2d.ComputeExtendedDataSquare(
		shares, share.DefaultRSMT2DCodec(),
		wrapper.NewConstructor(uint64(odsSize),
			nmt.NodeVisitor(adder.VisitFn()),
		))
	if err != nil {
		return 0, fmt.Errorf("compute eds: %w", err)
	}

	dah, err := da.NewDataAvailabilityHeader(square)
	if err != nil {
		return 0, fmt.Errorf("creating dah: %w", err)
	}

	ctx = ipld.CtxWithProofsAdder(ctx, adder)
	now := time.Now()
	err = ss.edsstore.Put(ctx, dah.Hash(), square)
	took := time.Since(now)

	if err != nil {
		return took, fmt.Errorf("put: %w", err)
	}
	return took, nil
}

type Stats struct {
	TotalWritten                         int
	TotalTime, MinTime, MaxTime, AvgTime time.Duration
	// Deviation ?
}

func (stats Stats) Finalize() Stats {
	if stats.TotalTime != 0 {
		stats.AvgTime = stats.TotalTime / time.Duration(stats.TotalWritten)
	}
	return stats
}

func (stats Stats) String() string {
	return fmt.Sprintf(`
TotalWritten %d
TotalWritingTime %v
MaxTime %s
MinTime %s
AvgTime %s
`,
		stats.TotalWritten,
		stats.TotalTime,
		stats.MaxTime,
		stats.MinTime,
		stats.AvgTime,
	)
}
