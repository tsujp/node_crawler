package main

import (
	"crypto/ecdsa"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/angaz/sqlu"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/node-crawler/pkg/database"
	"github.com/oschwald/geoip2-golang"
	"github.com/urfave/cli/v2"
)

func openSQLiteDB(cCtx *cli.Context, mode sqlu.Param) (*sql.DB, error) {
	dsn := sqlu.ConnParams{
		Filename: crawlerDBFlag.Get(cCtx),
		Mode:     mode,
		Pragma: []sqlu.Pragma{
			sqlu.PragmaBusyTimeout(int64(busyTimeoutFlag.Get(cCtx))),
			sqlu.PragmaJournalSizeLimit(128 * 1024 * 1024), // 128MiB
			sqlu.PragmaAutoVacuumIncremental,
			sqlu.PragmaJournalModeWAL,
			sqlu.PragmaSynchronousNormal,
		},
		Attach: []sqlu.AttachParams{
			{
				Filename: statsDBFlag.Get(cCtx),
				Database: "stats",
				Mode:     mode,
				Pragma: []sqlu.Pragma{
					sqlu.PragmaJournalSizeLimit(128 * 1024 * 1024), // 128MiB
					sqlu.PragmaAutoVacuumIncremental,
					sqlu.PragmaJournalModeWAL,
					sqlu.PragmaSynchronousNormal,
				},
			},
		},
	}

	db, err := sql.Open("sqlite", dsn.ConnectionString())
	if err != nil {
		return nil, fmt.Errorf("opening database: %w", err)
	}

	return db, nil
}

func openDBWriter(cCtx *cli.Context, geoipDB *geoip2.Reader) (*database.DB, error) {
	sqlite, err := openSQLiteDB(cCtx, sqlu.ParamModeRWC)
	if err != nil {
		return nil, fmt.Errorf("opening database: %w", err)
	}

	db := database.NewDB(
		sqlite,
		geoipDB,
		nextCrawlSuccessFlag.Get(cCtx),
		nextCrawlFailFlag.Get(cCtx),
		nextCrawlNotEthFlag.Get(cCtx),
	)

	err = db.MigrateCrawler()
	if err != nil {
		return nil, fmt.Errorf("database migration failed: %w", err)
	}

	err = db.MigrateStats()
	if err != nil {
		return nil, fmt.Errorf("stats database migration failed: %w", err)
	}

	return db, nil
}

func openDBReader(cCtx *cli.Context) (*database.DB, error) {
	sqlite, err := openSQLiteDB(cCtx, sqlu.ParamModeRO)
	if err != nil {
		return nil, fmt.Errorf("opening database failed: %w", err)
	}

	db := database.NewAPIDB(sqlite)

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
