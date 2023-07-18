package main

import (
	"context"
	"errors"
	"expvar"
	_ "expvar"
	"fmt"
	"github.com/pyroscope-io/client/pyroscope"
	"go.uber.org/zap"
	"math"
	"net/http"
	"net/http/pprof"
	"os"

	"github.com/mitchellh/go-homedir"
	"github.com/spf13/cobra"

	"github.com/celestiaorg/celestia-node/libs/edssser"
	"github.com/celestiaorg/celestia-node/nodebuilder"
	"github.com/celestiaorg/celestia-node/nodebuilder/node"
)

const (
	edsStorePathFlag   = "path"
	edsWritesFlag      = "writes"
	edsSizeFlag        = "size"
	edsDisableLogFlag  = "disable-log"
	edsLogStatFreqFlag = "log-stat-freq"
	edsCleanupFlag     = "cleanup"
	edsFreshStartFlag  = "fresh"

	pyroscopeEndpoint = "pyroscope"
)

func init() {
	edsStoreCmd.AddCommand(edsStoreStress)

	defaultPath := "~/.edssser"
	path, err := homedir.Expand(defaultPath)
	if err != nil {
		panic(err)
	}

	pathFlagUsage := fmt.Sprintf("Directory path to use for stress test. Uses %s by default.", defaultPath)
	edsStoreStress.Flags().String(edsStorePathFlag, path, pathFlagUsage)
	edsStoreStress.Flags().String(pyroscopeEndpoint, "", "Pyroscope address")
	edsStoreStress.Flags().Int(edsWritesFlag, math.MaxInt, "Total EDS writes to make. MaxInt by default.")
	edsStoreStress.Flags().Int(edsSizeFlag, 128, "Chooses EDS size. 128 by default.")
	edsStoreStress.Flags().Bool(edsDisableLogFlag, false, "Disables logging. Enabled by default.")
	edsStoreStress.Flags().Int(edsLogStatFreqFlag, 10, "Write statistic logging frequency. 10 by default.")
	edsStoreStress.Flags().Bool(edsCleanupFlag, false, "Cleans up the store on stop. Disabled by default.")
	edsStoreStress.Flags().Bool(edsFreshStartFlag, false, "Cleanup previous state on start. Disabled by default.")

	// kill redundant print
	nodebuilder.PrintKeyringInfo = false
}

var edsStoreCmd = &cobra.Command{
	Use:   "eds-store [subcommand]",
	Short: "Collection of eds-store related utilities",
}

var edsStoreStress = &cobra.Command{
	Use:          "stress",
	Short:        `Runs eds.Store stress test over default node.Store Datastore backend (e.g. Badger).`,
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, args []string) (err error) {
		// expose expvar vars over http
		go http.ListenAndServe(":9999", http.DefaultServeMux)

		endpoint, _ := cmd.Flags().GetString(pyroscopeEndpoint)
		if endpoint != "" {
			mux := http.NewServeMux()
			mux.HandleFunc("/debug/pprof/", pprof.Index)
			mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
			mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
			mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
			mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
			mux.Handle("/debug/vars", expvar.Handler())
			err := http.ListenAndServe("0.0.0.0:6000", mux) //nolint:gosec
			if err != nil {
				fmt.Println("failed to start pprof server", err)
			} else {
				fmt.Println("started pprof server on port 6000")
			}

			_, err = pyroscope.Start(pyroscope.Config{
				ApplicationName: "cel-shred.stresser",
				ServerAddress:   endpoint,
				Logger:          zap.L().Sugar(),
				ProfileTypes: []pyroscope.ProfileType{
					pyroscope.ProfileCPU,
					pyroscope.ProfileAllocObjects,
					pyroscope.ProfileAllocSpace,
					pyroscope.ProfileInuseObjects,
					pyroscope.ProfileInuseSpace,
				},
			})
			if err != nil {
				fmt.Printf("failed to launch pyroscope with addr: %s err: %s\n", endpoint, err.Error())
			}
			fmt.Println("run pyroscope on:", endpoint)
		}

		path, _ := cmd.Flags().GetString(edsStorePathFlag)
		fmt.Printf("using %s\n", path)

		freshStart, _ := cmd.Flags().GetBool(edsFreshStartFlag)
		if freshStart {
			err = os.RemoveAll(path)
			if err != nil {
				return err
			}
		}

		cleanup, _ := cmd.Flags().GetBool(edsCleanupFlag)
		if cleanup {
			defer func() {
				err = errors.Join(err, os.RemoveAll(path))
			}()
		}

		disableLog, _ := cmd.Flags().GetBool(edsDisableLogFlag)
		logFreq, _ := cmd.Flags().GetInt(edsLogStatFreqFlag)
		edsWrites, _ := cmd.Flags().GetInt(edsWritesFlag)
		edsSize, _ := cmd.Flags().GetInt(edsSizeFlag)
		cfg := edssser.Config{
			EDSSize:     edsSize,
			EDSWrites:   edsWrites,
			EnableLog:   !disableLog,
			StatLogFreq: logFreq,
		}

		err = nodebuilder.Init(*nodebuilder.DefaultConfig(node.Full), path, node.Full)
		if err != nil {
			return err
		}

		nodestore, err := nodebuilder.OpenStore(path, nil)
		if err != nil {
			return err
		}
		defer func() {
			err = errors.Join(err, nodestore.Close())
		}()

		datastore, err := nodestore.Datastore()
		if err != nil {
			return err
		}

		stresser, err := edssser.NewEDSsser(path, datastore, cfg)
		if err != nil {
			return err
		}

		stats, err := stresser.Run(cmd.Context())
		if !errors.Is(err, context.Canceled) {
			return err
		}

		fmt.Printf("%s", stats.Finalize())
		return nil
	},
}
