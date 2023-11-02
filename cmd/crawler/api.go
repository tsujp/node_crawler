package main

import (
	"fmt"
	"net/http"
	"sync"

	_ "modernc.org/sqlite"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/node-crawler/pkg/api"
	"github.com/ethereum/node-crawler/pkg/database"
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
			&busyTimeoutFlag,
			&crawlerDBFlag,
			&dropNodesTimeFlag,
			&metricsAddress,
		},
	}
)

func startAPI(cCtx *cli.Context) error {
	db, err := initDBReader(
		crawlerDBFlag.Get(cCtx),
		busyTimeoutFlag.Get(cCtx),
	)
	if err != nil {
		return fmt.Errorf("init db failed: %w", err)
	}
	defer db.Close()

	var wg sync.WaitGroup
	wg.Add(3)

	// Start the API deamon
	apiAddress := cCtx.String(apiListenAddrFlag.Name)
	apiDeamon := api.New(apiAddress, database.NewAPIDB(db))
	go apiDeamon.HandleRequests(&wg)

	// Start metrics server
	metricsAddr := metricsAddress.Get(cCtx)
	log.Info("starting metrics server", "address", metricsAddr)
	http.Handle("/metrics", promhttp.Handler())
	http.ListenAndServe(metricsAddr, nil)

	wg.Wait()

	return nil
}
