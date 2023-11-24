package main

import (
	"context"
	"crypto/ecdsa"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/node-crawler/pkg/database"
	"github.com/oschwald/geoip2-golang"
	"github.com/urfave/cli/v2"
)

func openSQLiteDB(name string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", name)
	if err != nil {
		return nil, fmt.Errorf("opening database failed: %w", err)
	}

	return db, nil
}

func openDBWriter(cCtx *cli.Context, geoipDB *geoip2.Reader) (*database.DB, error) {
	sqlite, err := openSQLiteDB(crawlerDBFlag.Get(cCtx))
	if err != nil {
		return nil, fmt.Errorf("opening database failed: %w", err)
	}

	db := database.NewDB(
		sqlite,
		geoipDB,
		nextCrawlSuccessFlag.Get(cCtx),
		nextCrawlFailFlag.Get(cCtx),
		nextCrawlNotEthFlag.Get(cCtx),
	)

	conn, err := sqlite.Conn(context.Background())
	if err != nil {
		return nil, fmt.Errorf("acquire conn failed: %w", err)
	}

	err = db.SetPragmas(
		conn,
		"main",
		busyTimeoutFlag.Get(cCtx),
		autovacuumFlag.Get(cCtx),
		"wal",
	)
	if err != nil {
		return nil, fmt.Errorf("setting main pragmas failed: %w", err)
	}

	err = db.Migrate()
	if err != nil {
		return nil, fmt.Errorf("database migration failed: %w", err)
	}

	// Setup stats DB
	err = db.AttachStatsDB("stats.db")
	if err != nil {
		return nil, fmt.Errorf("attach stats failed: %w", err)
	}

	err = db.SetPragmas(
		db.StatsConn(),
		"stats",
		busyTimeoutFlag.Get(cCtx),
		autovacuumFlag.Get(cCtx),
		"wal",
	)
	if err != nil {
		return nil, fmt.Errorf("setting status pragmas failed: %w", err)
	}

	err = db.CreateStatsTables()
	if err != nil {
		return nil, fmt.Errorf("create stats tables failed: %w", err)
	}

	return db, nil
}

func openDBReader(cCtx *cli.Context) (*database.DB, error) {
	sqlite, err := openSQLiteDB(crawlerDBFlag.Get(cCtx))
	if err != nil {
		return nil, fmt.Errorf("opening database failed: %w", err)
	}

	db := database.NewAPIDB(sqlite)

	conn, err := sqlite.Conn(context.Background())
	if err != nil {
		return nil, fmt.Errorf("acquire conn failed: %w", err)
	}

	err = db.SetPragmas(
		conn,
		"main",
		busyTimeoutFlag.Get(cCtx),
		"",
		"",
	)
	if err != nil {
		return nil, fmt.Errorf("settings main pragmas failed: %w", err)
	}

	// Setup stats DB
	err = db.AttachStatsDB("stats.db")
	if err != nil {
		return nil, fmt.Errorf("attach stats failed: %w", err)
	}

	err = db.SetPragmas(
		db.StatsConn(),
		"stats",
		busyTimeoutFlag.Get(cCtx),
		"",
		"",
	)
	if err != nil {
		return nil, fmt.Errorf("setting status pragmas failed: %w", err)
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
