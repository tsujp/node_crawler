package database

import (
	"database/sql"
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

	nextCrawlSucces string
	nextCrawlFail   string
	nextCrawlNotEth string
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

		nextCrawlSucces: strconv.Itoa(int(nextCrawlSucces.Seconds())) + " seconds",
		nextCrawlFail:   strconv.Itoa(int(nextCrawlFail.Seconds())) + " seconds",
		nextCrawlNotEth: strconv.Itoa(int(nextCrawlNotEth.Seconds())) + " seconds",
	}
}

func (d *DB) CreateTables() error {
	_, err := d.db.Exec(`
		CREATE TABLE IF NOT EXISTS discovered_nodes (
			id			TEXT	PRIMARY KEY,
			node		TEXT	NOT NULL,
			ip_address	TEXT	NOT NULL,
			first_found	TEXT	NOT NULL DEFAULT CURRENT_TIMESTAMP,
			last_found	TEXT	NOT NULL DEFAULT CURRENT_TIMESTAMP,
			next_crawl	TEXT	NOT NULL DEFAULT CURRENT_TIMESTAMP
		) STRICT;

		CREATE INDEX IF NOT EXISTS id_next_crawl
			ON discovered_nodes (id, next_crawl);
		CREATE INDEX IF NOT EXISTS discovered_nodes_next_crawl_node
			ON discovered_nodes (next_crawl, node);

		CREATE TABLE IF NOT EXISTS crawled_nodes (
			id				TEXT	PRIMARY KEY,
			updated_at		TEXT	NOT NULL DEFAULT CURRENT_TIMESTAMP,
			client_name		TEXT	DEFAULT NULL,
			rlpx_version	INTEGER	DEFAULT NULL,
			capabilities	TEXT	DEFAULT NULL,
			network_id		INTEGER	DEFAULT NULL,
			fork_id			TEXT	DEFAULT NULL,
			next_fork_id	INTEGER	DEFAULT NULL,
			block_height	TEXT	DEFAULT NULL,
			head_hash		TEXT	DEFAULT NULL,
			ip				TEXT	DEFAULT NULL,
			connection_type	TEXT	DEFAULT NULL,
			country			TEXT	DEFAULT NULL,
			city			TEXT	DEFAULT NULL,
			latitude		REAL	DEFAULT NULL,
			longitude		REAL	DEFAULT NULL,
			sequence		INTEGER	DEFAULT NULL
		) STRICT;

		CREATE INDEX IF NOT EXISTS id_last_seen
			ON crawled_nodes (id, updated_at);
		CREATE INDEX IF NOT EXISTS crawled_nodes_network_id
			ON crawled_nodes (network_id);

		CREATE TABLE IF NOT EXISTS crawl_history (
			id			TEXT	NOT NULL,
			crawled_at	TEXT	NOT NULL DEFAULT CURRENT_TIMESTAMP,
			direction	TEXT	NOT NULL,
			error		TEXT	DEFAULT NULL,

			PRIMARY KEY (id, crawled_at)
		) STRICT;

		CREATE TABLE IF NOT EXISTS blocks (
			block_hash		TEXT	NOT NULL,
			network_id		INTEGER NOT NULL,
			timestamp		TEXT	NOT NULL,
			block_number	INTEGER	NOT NULL,
			parent_hash		TEXT	DEFAULT NULL,

			PRIMARY KEY (block_hash, network_id)
		) STRICT;

		CREATE INDEX IF NOT EXISTS blocks_block_hash_timestamp
			ON blocks (block_hash, timestamp);
	`)
	if err != nil {
		return fmt.Errorf("creating table discovered_nodes failed: %w", err)
	}

	return nil
}

type tableStats struct {
	totalDiscoveredNodes int
	totalCrawledNodes    int
	totalBlocks          int
	totalToCrawl         int
	databaseSizeBytes    int
}

func (db *DB) getTableStats() (*tableStats, error) {
	var err error

	start := time.Now()
	defer metrics.ObserveDBQuery("total_disc_nodes", time.Since(start), err)

	rows, err := db.QueryRetryBusy(`
		SELECT
			(SELECT COUNT(*) FROM discovered_nodes),
			(SELECT COUNT(*) FROM discovered_nodes WHERE next_crawl < CURRENT_TIMESTAMP),
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
