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
	"crypto/ecdsa"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	_ "modernc.org/sqlite"

	"github.com/oschwald/geoip2-golang"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/ethereum/go-ethereum/cmd/utils"
	gethCommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/node-crawler/pkg/common"
	"github.com/ethereum/node-crawler/pkg/crawler"
	"github.com/ethereum/node-crawler/pkg/crawlerv2"
	"github.com/ethereum/node-crawler/pkg/database"
	"github.com/ethereum/node-crawler/pkg/disc"

	"github.com/urfave/cli/v2"
)

var (
	crawlerCommand = &cli.Command{
		Name:   "crawl",
		Usage:  "Crawl the ethereum network",
		Action: crawlNodesV2,
		Flags: []cli.Flag{
			&autovacuumFlag,
			&backupFilenameFlag,
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
			&timeoutFlag,
			&workersFlag,
			utils.GoerliFlag,
			utils.HoleskyFlag,
			utils.NetworkIdFlag,
			utils.SepoliaFlag,
		},
	}
)

func initDBReader(dbName string, busyTimeout uint64) (*sql.DB, error) {
	db, err := openSQLiteDB(dbName, busyTimeout, "", "")
	if err != nil {
		return nil, fmt.Errorf("opening database failed: %w", err)
	}

	return db, nil
}

func initDBWriter(dbName string, autovacuum string, busyTimeout uint64) (*sql.DB, error) {
	db, err := openSQLiteDB(dbName, busyTimeout, autovacuum, "wal")
	if err != nil {
		return nil, fmt.Errorf("opening database failed: %w", err)
	}

	return db, nil
}

func readNodeKey(cCtx *cli.Context) (*ecdsa.PrivateKey, error) {
	nodeKey, err := crypto.GenerateKey()
	if err != nil {
		return nil, fmt.Errorf("generating node key failed: %w", err)
	}

	nodeKeyFileName := nodeKeyFileFlag.Get(cCtx)

	nodeKeyFile, err := os.ReadFile(nodeKeyFileName)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			nodeKeyString := hex.EncodeToString(crypto.FromECDSA(nodeKey))

			err = os.WriteFile(nodeKeyFileName, []byte(nodeKeyString), 0o600)
			if err != nil {
				return nil, fmt.Errorf("writing new node file failed: %w", err)
			}

			return nodeKey, nil
		}

		return nil, fmt.Errorf("reading node key file failed: %w", err)
	}

	nodeKeyBytes, err := hex.DecodeString(strings.TrimSpace(string(nodeKeyFile)))
	if err != nil {
		return nil, fmt.Errorf("hex decoding node key failed: %w", err)
	}

	nodeKey, err = crypto.ToECDSA(nodeKeyBytes)
	if err != nil {
		return nil, fmt.Errorf("ecdsa parsing of node key failed: %w", err)
	}

	return nodeKey, nil
}

func crawlNodesV2(cCtx *cli.Context) error {
	sqlite, err := initDBWriter(
		crawlerDBFlag.Get(cCtx),
		autovacuumFlag.Get(cCtx),
		busyTimeoutFlag.Get(cCtx),
	)
	if err != nil {
		return fmt.Errorf("init db failed: %w", err)
	}
	defer sqlite.Close()

	geoipFile := geoipdbFlag.Get(cCtx)
	var geoipDB *geoip2.Reader

	if geoipFile != "" {
		geoipDB, err = geoip2.Open(geoipFile)
		if err != nil {
			return fmt.Errorf("opening geoip database failed: %w", err)
		}
		defer func() { _ = geoipDB.Close() }()
	}

	db := database.NewDB(
		sqlite,
		geoipDB,
		nextCrawlSuccessFlag.Get(cCtx),
		nextCrawlFailFlag.Get(cCtx),
		nextCrawlNotEthFlag.Get(cCtx),
	)

	err = db.CreateTables()
	if err != nil {
		return fmt.Errorf("create tables failed: %w", err)
	}

	go db.TableStatsMetricsDaemon(5 * time.Minute)
	go db.BackupDaemon(backupFilenameFlag.Get(cCtx))

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

	crawler, err := crawlerv2.NewCrawlerV2(
		db,
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

func crawlNodes(ctx *cli.Context) error {
	var inputSet common.NodeSet
	var geoipDB *geoip2.Reader
	var db *sql.DB
	var err error

	nodesFile := ctx.String(nodeFileFlag.Name)

	if nodesFile != "" && gethCommon.FileExist(nodesFile) {
		inputSet = common.LoadNodesJSON(nodesFile)
	}

	if ctx.IsSet(crawlerDBFlag.Name) {
		db, err = initDBWriter(
			ctx.String(crawlerDBFlag.Name),
			ctx.String(autovacuumFlag.Name),
			ctx.Uint64(busyTimeoutFlag.Name),
		)
		if err != nil {
			panic(err)
		}
	}

	nodeDB, err := enode.OpenDB(ctx.String(nodedbFlag.Name))
	if err != nil {
		panic(err)
	}

	geoipFile := ctx.String(geoipdbFlag.Name)
	if geoipFile != "" {
		geoipDB, err = geoip2.Open(geoipFile)
		if err != nil {
			return err
		}
		defer func() { _ = geoipDB.Close() }()
	}

	nodeKey, err := readNodeKey(ctx)
	if err != nil {
		return fmt.Errorf("loading node key failed: %w", err)
	}

	crawler := crawler.Crawler{
		NetworkID:  ctx.Uint64(utils.NetworkIdFlag.Name),
		NodeURL:    ctx.String(nodeURLFlag.Name),
		ListenAddr: ctx.String(listenAddrFlag.Name),
		NodeKey:    nodeKey,
		Bootnodes:  ctx.StringSlice(bootnodesFlag.Name),
		Timeout:    ctx.Duration(timeoutFlag.Name),
		Workers:    ctx.Uint64(workersFlag.Name),
		Sepolia:    ctx.Bool(utils.SepoliaFlag.Name),
		Goerli:     ctx.Bool(utils.GoerliFlag.Name),
		Holesky:    ctx.Bool(utils.HoleskyFlag.Name),
		NodeDB:     nodeDB,
	}

	for {
		updatedSet := crawler.CrawlRound(inputSet, db, geoipDB)
		if nodesFile != "" {
			updatedSet.WriteNodesJSON(nodesFile)
		}
	}
}
