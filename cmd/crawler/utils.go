package main

import (
	"crypto/ecdsa"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/node-crawler/pkg/database"
	"github.com/oschwald/geoip2-golang"
	"github.com/urfave/cli/v2"
)

func openSQLiteDB(cCtx *cli.Context, mode string) (*sql.DB, error) {
	attachURI := buildConnURI(
		cCtx,
		statsDBFlag.Get(cCtx),
		mode,
		url.Values{
			"_name": []string{"stats"},
		},
	)

	uri := buildConnURI(
		cCtx,
		crawlerDBFlag.Get(cCtx),
		mode,
		url.Values{
			"_attach": []string{attachURI},
		},
	)

	db, err := sql.Open("sqlite_attach", uri)
	if err != nil {
		return nil, fmt.Errorf("opening database failed: %w", err)
	}

	return db, nil
}

func buildConnURI(
	cCtx *cli.Context,
	filename string,
	mode string,
	query url.Values,
) string {
	//nolint:exhaustruct
	uri := url.URL{
		Path: filename,
	}

	busyTimeout := busyTimeoutFlag.Get(cCtx)
	autovacuum := autovacuumFlag.Get(cCtx)

	if busyTimeout != 0 {
		query.Add("_pragma", fmt.Sprintf("busy_timeout=%d", busyTimeout))
	}
	if autovacuum != "" {
		query.Add("_pragma", fmt.Sprintf("auto_vacuum=%s", autovacuum))
	}

	query.Add("_pragma", "journal_mode=wal")
	query.Add("_pragma", "synchronous=normal")

	query.Set("mode", mode)

	uri.RawQuery = query.Encode()

	return uri.String()
}

func openDBWriter(cCtx *cli.Context, geoipDB *geoip2.Reader) (*database.DB, error) {
	sqlite, err := openSQLiteDB(cCtx, "rwc")
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
	sqlite, err := openSQLiteDB(cCtx, "ro")
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
