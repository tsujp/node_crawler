// Copyright 2021 The go-ethereum Authors
// This file is part of go-ethereum.
//
// go-ethereum is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// go-ethereum is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with go-ethereum. If not, see <http://www.gnu.org/licenses/>.

package main

import (
	"fmt"
	"net/http"
	"time"

	_ "modernc.org/sqlite"

	"github.com/oschwald/geoip2-golang"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/node-crawler/pkg/crawlerv2"
	"github.com/ethereum/node-crawler/pkg/disc"

	"github.com/urfave/cli/v2"
)

var (
	//nolint:exhaustruct  // We don't need to specify everything.
	crawlerCommand = &cli.Command{
		Name:   "crawl",
		Usage:  "Crawl the ethereum network",
		Action: crawlNodesV2,
		Flags: []cli.Flag{
			&autovacuumFlag,
			&bootnodesFlag,
			&busyTimeoutFlag,
			&crawlerDBFlag,
			&geoipdbFlag,
			&listenAddrFlag,
			&metricsAddressFlag,
			&nextCrawlFailFlag,
			&nextCrawlNotEthFlag,
			&nextCrawlSuccessFlag,
			&nodeFileFlag,
			&nodeKeyFileFlag,
			&nodeURLFlag,
			&nodedbFlag,
			&snapshotFilenameFlag,
			&statsCopyFrequencyFlag,
			&statsDBFlag,
			&timeoutFlag,
			&workersFlag,
		},
	}
)

func openGeoIP(cCtx *cli.Context) (*geoip2.Reader, error) {
	geoipFile := geoipdbFlag.Get(cCtx)

	if geoipFile == "" {
		return nil, nil
	}

	geoipDB, err := geoip2.Open(geoipFile)
	if err != nil {
		return nil, fmt.Errorf("opening geoip database failed: %w", err)
	}

	return geoipDB, nil
}

func crawlNodesV2(cCtx *cli.Context) error {
	geoipDB, err := openGeoIP(cCtx)
	if err != nil {
		return fmt.Errorf("open geoip2 failed: %w", err)
	}

	if geoipDB != nil {
		defer geoipDB.Close()
	}

	db, err := openDBWriter(cCtx, geoipDB)
	if err != nil {
		return fmt.Errorf("open db failed: %w", err)
	}
	defer db.Close()

	go db.TableStatsMetricsDaemon(5 * time.Minute)
	go db.SnapshotDaemon(snapshotFilenameFlag.Get(cCtx))
	go db.CleanerDaemon(15 * time.Minute)
	go db.CopyStatsDaemon(statsCopyFrequencyFlag.Get(cCtx))

	nodeKey, err := readNodeKey(cCtx)
	if err != nil {
		return fmt.Errorf("node key failed: %w", err)
	}

	disc, err := disc.New(db, listenAddrFlag.Get(cCtx), nodeKey)
	if err != nil {
		return fmt.Errorf("create disc failed: %w", err)
	}
	defer disc.Close()

	err = disc.StartDaemon()
	if err != nil {
		return fmt.Errorf("start disc daemon failed: %w", err)
	}

	crawler, err := crawlerv2.NewCrawler(
		db,
		disc.DiscV4(),
		disc.DiscV5(),
		nodeKey,
		listenAddrFlag.Get(cCtx),
		workersFlag.Get(cCtx),
	)
	if err != nil {
		return fmt.Errorf("create crawler v2 failed: %w", err)
	}

	err = crawler.StartDaemon()
	if err != nil {
		return fmt.Errorf("start crawler v2 failed: %w", err)
	}

	// Start metrics server
	metricsAddr := metricsAddressFlag.Get(cCtx)
	log.Info("starting metrics server", "address", metricsAddr)
	http.Handle("/metrics", promhttp.Handler())
	http.ListenAndServe(metricsAddr, nil)

	disc.Wait()
	crawler.Wait()

	return nil
}
