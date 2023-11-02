package database

import (
	"database/sql"
	_ "embed"
	"encoding/hex"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/node-crawler/pkg/metrics"
	"github.com/oschwald/geoip2-golang"
)

type DB struct {
	db      *sql.DB
	geoipDB *geoip2.Reader

	nextCrawlSucces int64
	nextCrawlFail   int64
	nextCrawlNotEth int64
}

func NewAPIDB(db *sql.DB) *DB {
	return NewDB(db, nil, 0, 0, 0)
}

func NewDB(
	db *sql.DB,
	geoipDB *geoip2.Reader,
	nextCrawlSucces time.Duration,
	nextCrawlFail time.Duration,
	nextCrawlNotEth time.Duration,
) *DB {
	return &DB{
		db:      db,
		geoipDB: geoipDB,

		nextCrawlSucces: int64(nextCrawlSucces.Seconds()),
		nextCrawlFail:   int64(nextCrawlFail.Seconds()),
		nextCrawlNotEth: int64(nextCrawlNotEth.Seconds()),
	}
}

//go:embed sql/schema.sql
var schema string

func (db *DB) CreateTables() error {
	_, err := db.db.Exec(schema)
	if err != nil {
		return fmt.Errorf("creating table discovered_nodes failed: %w", err)
	}

	return nil
}

func (db *DB) migrateDiscoveredNodes(old *sql.DB) error {
	rows, err := old.Query(`
		SELECT
			id,
			node,
			ip_address,
			unixepoch(first_found),
			unixepoch(last_found),
			unixepoch(next_crawl)
		FROM discovered_nodes
	`)
	if err != nil {
		return fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	tx, err := db.db.Begin()
	if err != nil {
		return fmt.Errorf("starting tx failed: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`
		INSERT INTO discovered_nodes (
			node_id,
			network_address,
			ip_address,
			first_found,
			last_found,
			next_crawl
		) VALUES (
			?, ?, ?, ?, ?, ?
		)
	`)
	if err != nil {
		return fmt.Errorf("prepared statement failed: %w", err)
	}
	defer stmt.Close()

	for rows.Next() {
		var nodeID, networkAddress, ipAddress string
		var firstFound, lastFound, nextCrawl int64

		rows.Scan(
			&nodeID,
			&networkAddress,
			&ipAddress,
			&firstFound,
			&lastFound,
			&nextCrawl,
		)

		newID := make([]byte, len(nodeID)/2)
		_, err := hex.Decode(newID, []byte(nodeID))
		if err != nil {
			panic(err)
		}

		_, err = stmt.Exec(
			newID,
			networkAddress,
			ipAddress,
			firstFound,
			lastFound,
			nextCrawl,
		)
		if err != nil {
			return fmt.Errorf("insert exec failed: %w", err)
		}
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("tx commit failed: %w", err)
	}

	return nil
}

func (db *DB) migrateBlocks(old *sql.DB) error {
	rows, err := old.Query(`
		SELECT
			block_hash,
			network_id,
			unixepoch(timestamp),
			block_number,
			parent_hash
		FROM blocks
	`)
	if err != nil {
		return fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	tx, err := db.db.Begin()
	if err != nil {
		return fmt.Errorf("starting tx failed: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`
		INSERT INTO blocks (
			block_hash,
			network_id,
			timestamp,
			block_number,
			parent_hash
		) VALUES (
			?, ?, ?, ?, ?
		)
	`)
	if err != nil {
		return fmt.Errorf("prepared statement failed: %w", err)
	}
	defer stmt.Close()

	for rows.Next() {
		var blockHash, parentHash string
		var networkID, timestamp, blockNumber int64

		rows.Scan(
			&blockHash,
			&networkID,
			&timestamp,
			&blockNumber,
			&parentHash,
		)

		blockHash = strings.TrimPrefix(blockHash, "0x")
		parentHash = strings.TrimPrefix(parentHash, "0x")

		newBlockHash := make([]byte, len(blockHash)/2)
		_, err := hex.Decode(newBlockHash, []byte(blockHash))
		if err != nil {
			panic(err)
		}
		newParentHash := make([]byte, len(parentHash)/2)
		_, err = hex.Decode(newParentHash, []byte(parentHash))
		if err != nil {
			panic(err)
		}

		_, err = stmt.Exec(
			newBlockHash,
			networkID,
			timestamp,
			blockNumber,
			newParentHash,
		)
		if err != nil {
			return fmt.Errorf("insert exec failed: %w", err)
		}
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("tx commit failed: %w", err)
	}

	return nil
}

func (db *DB) migrateCrawlHistory(old *sql.DB) error {
	rows, err := old.Query(`
		SELECT
			id,
			unixepoch(crawled_at),
			direction,
			error
		FROM crawl_history
	`)
	if err != nil {
		return fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	tx, err := db.db.Begin()
	if err != nil {
		return fmt.Errorf("start tx failed: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`
		INSERT INTO crawl_history (
			node_id,
			crawled_at,
			direction,
			error
		) VALUES (
			?, ?, ?, ?
		)
	`)
	if err != nil {
		return fmt.Errorf("prepared statement failed: %w", err)
	}
	defer stmt.Close()

	for rows.Next() {
		var id, direction string
		var e *string
		var crawledAt int64

		rows.Scan(
			&id,
			&crawledAt,
			&direction,
			&e,
		)

		newID := make([]byte, len(id)/2)
		_, err := hex.Decode(newID, []byte(id))
		if err != nil {
			panic(err)
		}

		_, err = stmt.Exec(
			newID,
			crawledAt,
			direction,
			e,
		)
		if err != nil {
			return fmt.Errorf("insert exec failed: %w", err)
		}
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("commit tx failed: %w", err)
	}

	return nil
}

func (db *DB) migrateCrawledNodes(old *sql.DB) error {
	rows, err := old.Query(`
		SELECT
			id,
			unixepoch(updated_at),
			client_name,
			rlpx_version,
			capabilities,
			network_id,
			fork_id,
			next_fork_id,
			head_hash,
			ip,
			connection_type,
			country,
			city,
			latitude,
			longitude
		FROM crawled_nodes
	`)
	if err != nil {
		return fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	tx, err := db.db.Begin()
	if err != nil {
		return fmt.Errorf("start tx failed: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`
		INSERT INTO crawled_nodes (
			node_id,
			updated_at,
			client_name,
			rlpx_version,
			capabilities,
			network_id,
			fork_id,
			next_fork_id,
			head_hash,
			ip_address,
			connection_type,
			country,
			city,
			latitude,
			longitude
		) VALUES (
			?,
			?,
			?,
			?,
			?,
			?,
			?,
			?,
			?,
			?,
			?,
			?,
			?,
			?,
			?
		)
	`)
	if err != nil {
		return fmt.Errorf("prepared statement failed: %w", err)
	}
	defer stmt.Close()

	for rows.Next() {
		var id string
		var clientName, caps, forkID, headHash, ipAddress, connectionType, country, city *string
		var updatedAt int64
		var rlpxVersion, networkID, nextForkID *int64
		var latitude, longitude *float64

		rows.Scan(
			&id,
			&updatedAt,
			&clientName,
			&rlpxVersion,
			&caps,
			&networkID,
			&forkID,
			&nextForkID,
			&headHash,
			&ipAddress,
			&connectionType,
			&country,
			&city,
			&latitude,
			&longitude,
		)

		newID := make([]byte, len(id)/2)
		_, err := hex.Decode(newID, []byte(id))
		if err != nil {
			panic(err)
		}

		var newForkID *uint32 = nil
		if forkID != nil {
			fidb := make([]byte, 4)
			_, err := hex.Decode(fidb, []byte(*forkID))
			if err != nil {
				panic(err)
			}

			fid := BytesToUnit32(fidb)
			newForkID = &fid
		}

		var newHeadHash *[]byte = nil
		if headHash != nil {
			hh := strings.TrimPrefix(*headHash, "0x")
			nhh := make([]byte, len(hh)/2)
			_, err := hex.Decode(nhh, []byte(hh))
			if err != nil {
				panic(err)
			}

			newHeadHash = &nhh
		}

		_, err = stmt.Exec(
			newID,
			updatedAt,
			clientName,
			rlpxVersion,
			caps,
			networkID,
			newForkID,
			nextForkID,
			newHeadHash,
			ipAddress,
			connectionType,
			country,
			city,
			latitude,
			longitude,
		)
		if err != nil {
			return fmt.Errorf("insert exec failed: %w", err)
		}
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("commit tx failed: %w", err)
	}

	return nil
}

func (db *DB) Migrate(old *sql.DB) error {
	var err error

	err = db.migrateDiscoveredNodes(old)
	if err != nil {
		return fmt.Errorf("migrate discovered_nodes failed: %w", err)
	}

	err = db.migrateCrawlHistory(old)
	if err != nil {
		return fmt.Errorf("migrate crawl_history failed: %w", err)
	}

	err = db.migrateBlocks(old)
	if err != nil {
		return fmt.Errorf("migrate blocks failed: %w", err)
	}

	err = db.migrateCrawledNodes(old)
	if err != nil {
		return fmt.Errorf("migrate crawled_nodes failed: %w", err)
	}

	return nil
}

type tableStats struct {
	totalDiscoveredNodes int64
	totalCrawledNodes    int64
	totalBlocks          int64
	totalToCrawl         int64
	databaseSizeBytes    int64
}

func (db *DB) getTableStats() (*tableStats, error) {
	var err error

	start := time.Now()
	defer metrics.ObserveDBQuery("total_disc_nodes", start, err)

	rows, err := db.QueryRetryBusy(`
		SELECT
			(SELECT COUNT(*) FROM discovered_nodes),
			(SELECT COUNT(*) FROM discovered_nodes WHERE next_crawl < unixepoch()),
			(SELECT COUNT(*) FROM crawled_nodes),
			(SELECT COUNT(*) FROM blocks),
			(
				SELECT page_count * page_size
				FROM pragma_page_count(), pragma_page_size()
			)
	`)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		stats := tableStats{}

		err = rows.Scan(
			&stats.totalDiscoveredNodes,
			&stats.totalToCrawl,
			&stats.totalCrawledNodes,
			&stats.totalBlocks,
			&stats.databaseSizeBytes,
		)
		if err != nil {
			return nil, fmt.Errorf("scan failed: %w", err)
		}

		return &stats, nil
	}

	return nil, sql.ErrNoRows
}

// Meant to be run as a goroutine
//
// Periodically collects the table stat metrics
func (db *DB) TableStatsMetricsDaemon(frequency time.Duration) {
	for {
		next := time.Now().Add(frequency)

		stats, err := db.getTableStats()
		if err != nil {
			log.Error("get table stats failed", "err", err)

			continue
		}

		metrics.DatabaseStats.WithLabelValues("crawled_nodes").Set(float64(stats.totalCrawledNodes))
		metrics.DatabaseStats.WithLabelValues("discovered_nodes").Set(float64(stats.totalDiscoveredNodes))
		metrics.DatabaseStats.WithLabelValues("to_crawl").Set(float64(stats.totalToCrawl))
		metrics.DatabaseStats.WithLabelValues("blocks").Set(float64(stats.totalBlocks))
		metrics.DatabaseStats.WithLabelValues("database_bytes").Set(float64(stats.databaseSizeBytes))

		time.Sleep(time.Until(next))
	}
}

func retryBusy[T any](f func() (T, error)) (T, error) {
	retry := 0

	for {
		result, err := f()
		if err != nil && retry < 5 && strings.Contains(err.Error(), "database is locked (5) (SQLITE_BUSY)") {
			// retry 0: 2^0 * 50 + 100 = 150 ms
			// retry 1: 2^1 * 50 + 100 = 200 ms
			// retry 2: 2^2 * 50 + 100 = 300 ms
			// retry 3: 2^3 * 50 + 100 = 500 ms
			// retry 4: 2^4 * 50 + 100 = 900 ms
			time.Sleep(time.Duration((math.Pow(2, float64(retry))*50)+100) * time.Millisecond)

			retry += 1

			metrics.DatabaseRetries.WithLabelValues(strconv.Itoa(retry)).Inc()

			continue
		}

		return result, err
	}
}

func (d *DB) ExecRetryBusy(query string, args ...any) (sql.Result, error) {
	return retryBusy(func() (sql.Result, error) {
		return d.db.Exec(query, args...)
	})
}

func (db *DB) QueryRetryBusy(query string, args ...any) (*sql.Rows, error) {
	return retryBusy(func() (*sql.Rows, error) {
		return db.db.Query(query, args...)
	})
}
