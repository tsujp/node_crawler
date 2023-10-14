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
	"os"
	"strings"

	_ "modernc.org/sqlite"

	"github.com/oschwald/geoip2-golang"

	"github.com/ethereum/go-ethereum/cmd/utils"
	gethCommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/node-crawler/pkg/common"
	"github.com/ethereum/node-crawler/pkg/crawler"
	"github.com/ethereum/node-crawler/pkg/crawlerdb"
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
			&bootnodesFlag,
			&busyTimeoutFlag,
			&crawlerDBFlag,
			&geoipdbFlag,
			&listenAddrFlag,
			&nodeFileFlag,
			&nodeURLFlag,
			&nodedbFlag,
			&nodeKeyFileFlag,
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
	if v1Flag.Get(cCtx) {
		go crawlNodes(cCtx)
	}

	sqlite, err := initDB(
		crawlerDBFlag.Get(cCtx)+"_v2",
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

	db := database.NewDB(sqlite, geoipDB)

	err = db.CreateTables()
	if err != nil {
		return fmt.Errorf("create tables failed: %w", err)
	}

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
		core.DefaultGenesisBlock(),
		utils.NetworkIdFlag.Get(cCtx),
	)
	if err != nil {
		return fmt.Errorf("create crawler v2 failed: %w", err)
	}

	err = crawler.StartDaemon()
	if err != nil {
		return fmt.Errorf("start crawler v2 failed: %w", err)
	}

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