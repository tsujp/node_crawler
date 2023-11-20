package main

import (
	"fmt"
	"net/http"
	"path"
	"sync"

	_ "modernc.org/sqlite"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/node-crawler/pkg/api"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/urfave/cli/v2"
)

var (
	apiCommand = &cli.Command{
		Name:   "api",
		Usage:  "API server for the crawler",
		Action: startAPI,
		Flags: []cli.Flag{
			&apiListenAddrFlag,
			&snapshotFilenameFlag,
			&busyTimeoutFlag,
			&crawlerDBFlag,
			&dropNodesTimeFlag,
			&enodeFlag,
			&metricsAddressFlag,
			&statsUpdateFrequencyFlag,
		},
	}
)

func startAPI(cCtx *cli.Context) error {
	db, err := openDBReader(cCtx)
	if err != nil {
		return fmt.Errorf("open db failed: %w", err)
	}
	defer db.Close()

	wg := new(sync.WaitGroup)
	wg.Add(1)

	// Start the API deamon
	api := api.New(
		db,
		statsUpdateFrequencyFlag.Get(cCtx),
		enodeFlag.Get(cCtx),
		path.Dir(snapshotFilenameFlag.Get(cCtx)),
	)
	go api.StartServer(
		wg,
		apiListenAddrFlag.Get(cCtx),
	)

	// Start metrics server
	metricsAddr := metricsAddressFlag.Get(cCtx)
	log.Info("starting metrics server", "address", metricsAddr)
	http.Handle("/metrics", promhttp.Handler())
	http.ListenAndServe(metricsAddr, nil)

	wg.Wait()

	return nil
}
