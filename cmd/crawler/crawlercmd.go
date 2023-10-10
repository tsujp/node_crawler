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
	"database/sql"
	"fmt"
	"os"
	"time"

	_ "modernc.org/sqlite"

	"github.com/oschwald/geoip2-golang"

	"github.com/ethereum/go-ethereum/cmd/utils"
	gethCommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/node-crawler/pkg/common"
	"github.com/ethereum/node-crawler/pkg/crawler"
	"github.com/ethereum/node-crawler/pkg/crawlerdb"
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
			&bootnodesFlag,
			&busyTimeoutFlag,
			&crawlerDBFlag,
			&geoipdbFlag,
			&listenAddrFlag,
			&nodeFileFlag,
			&nodeURLFlag,
			&nodedbFlag,
			&nodekeyFlag,
			&timeoutFlag,
			&v1Flag,
			&workersFlag,
			utils.GoerliFlag,
			utils.HoleskyFlag,
			utils.NetworkIdFlag,
			utils.SepoliaFlag,
		},
	}
)

func initDB(dbName string, autovacuum string, busyTimeout uint64) (*sql.DB, error) {
	shouldInit := false
	if _, err := os.Stat(dbName); os.IsNotExist(err) {
		shouldInit = true
	}

	db, err := openSQLiteDB(dbName, autovacuum, busyTimeout)
	if err != nil {
		return nil, fmt.Errorf("opening database failed: %w", err)
	}

	log.Info("Connected to db")
	if shouldInit {
		log.Info("DB did not exist, init")
		if err := crawlerdb.CreateDB(db); err != nil {
			return nil, fmt.Errorf("init database failed: %w", err)
		}
	}

	return db, nil
}

func crawlNodesV2(cCtx *cli.Context) error {
	if cCtx.Bool(v1Flag.Name) {
		go crawlNodes(cCtx)
	}

	db, err := initDB(
		cCtx.String(crawlerDBFlag.Name)+"_v2",
		cCtx.String(autovacuumFlag.Name),
		cCtx.Uint64(busyTimeoutFlag.Name),
	)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	discDB := database.NewDiscDB(db)

	err = discDB.CreateTables()
	if err != nil {
		panic(err)
	}

	disc, err := disc.New(discDB, cCtx.String(listenAddrFlag.Name))
	if err != nil {
		panic(err)
	}

	err = disc.StartDaemon()
	if err != nil {
		panic(err)
	}

	for {
		time.Sleep(time.Hour)
	}
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
		db, err = initDB(
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

	crawler := crawler.Crawler{
		NetworkID:  ctx.Uint64(utils.NetworkIdFlag.Name),
		NodeURL:    ctx.String(nodeURLFlag.Name),
		ListenAddr: ctx.String(listenAddrFlag.Name),
		NodeKey:    ctx.String(nodekeyFlag.Name),
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
