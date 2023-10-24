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
			id			TEXT		PRIMARY KEY,
			node		TEXT		NOT NULL,
			ip_address	TEXT		NOT NULL,
			first_found	TIMESTAMPTZ	NOT NULL DEFAULT CURRENT_TIMESTAMP,
			last_found	TIMESTAMPTZ	NOT NULL DEFAULT CURRENT_TIMESTAMP,
			next_crawl	TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
		);

		CREATE INDEX IF NOT EXISTS id_next_crawl
			ON discovered_nodes (id, next_crawl);

		CREATE TABLE IF NOT EXISTS crawled_nodes (
			id				TEXT		PRIMARY KEY,
			updated_at		TIMESTAMPTZ	NOT NULL DEFAULT CURRENT_TIMESTAMP,
			client_name		TEXT		DEFAULT NULL,
			rlpx_version	INTEGER		DEFAULT NULL,
			capabilities	TEXT		DEFAULT NULL,
			network_id		INTEGER		DEFAULT NULL,
			fork_id			TEXT		DEFAULT NULL,
			next_fork_id	INTEGER		DEFAULT NULL,
			block_height	TEXT		DEFAULT NULL,
			head_hash		TEXT		DEFAULT NULL,
			ip				TEXT		DEFAULT NULL,
			connection_type	TEXT		DEFAULT NULL,
			country			TEXT		DEFAULT NULL,
			city			TEXT		DEFAULT NULL,
			latitude		REAL		DEFAULT NULL,
			longitude		REAL		DEFAULT NULL,
			sequence		INTEGER		DEFAULT NULL
		);

		CREATE INDEX IF NOT EXISTS id_last_seen
			ON crawled_nodes (id, updated_at);
		CREATE INDEX IF NOT EXISTS crawled_nodes_network_id
			ON crawled_nodes (network_id);

		CREATE TABLE IF NOT EXISTS crawl_history (
			id			TEXT		NOT NULL,
			crawled_at	TIMESTAMPTZ	NOT NULL DEFAULT CURRENT_TIMESTAMP,
			direction	TEXT		NOT NULL,
			error		TEXT		DEFAULT NULL,

			PRIMARY KEY (id, crawled_at)
		);
	`)
	if err != nil {
		return fmt.Errorf("creating table discovered_nodes failed: %w", err)
	}

	return nil
}

func (db *DB) getTableStats() (int, int, int, error) {
	var err error

	start := time.Now()
	defer metrics.ObserveDBQuery("total_disc_nodes", time.Since(start), err)

	rows, err := db.QueryRetryBusy(`
		SELECT
			(SELECT COUNT(*) FROM discovered_nodes),
			(SELECT COUNT(*) FROM discovered_nodes WHERE next_crawl < CURRENT_TIMESTAMP),
			(SELECT COUNT(*) FROM crawled_nodes)
	`)
	if err != nil {
		return -1, -1, -1, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	var totalDisc, toCrawl, totalCrawled int

	for rows.Next() {
		err = rows.Scan(&totalDisc, &toCrawl, &totalCrawled)
		if err != nil {
			return -1, -1, -1, fmt.Errorf("scan failed: %w", err)
		}

		return totalDisc, toCrawl, totalCrawled, nil
	}

	return -1, -1, -1, sql.ErrNoRows
}

// Meant to be run as a goroutine
//
// Periodically collects the table stat metrics
func (db *DB) TableStatsMetricsDaemon(frequency time.Duration) {
	for {
		next := time.Now().Add(frequency)

		totalDisc, toCrawl, totalCrawled, err := db.getTableStats()
		if err != nil {
			log.Error("get table stats failed", "err", err)

			continue
		}

		metrics.TableStatsCrawledNodeCount.Set(float64(totalCrawled))
		metrics.TableStatsDiscNodes.Set(float64(totalDisc))
		metrics.TableStatsNodesToCrawl.Set(float64(toCrawl))

		time.Sleep(time.Until(next))
	}
}

func (d *DB) ExecRetryBusy(query string, args ...any) (sql.Result, error) {
	retry := 0

	for {
		result, err := d.db.Exec(query, args...)
		if err != nil && retry < 5 && strings.Contains(err.Error(), "database is locked (5) (SQLITE_BUSY)") {
			// retry 0: 2^0 * 50 + 100 = 150 ms
			// retry 1: 2^1 * 50 + 100 = 200 ms
			// retry 2: 2^2 * 50 + 100 = 300 ms
			// retry 3: 2^3 * 50 + 100 = 500 ms
			// retry 4: 2^4 * 50 + 100 = 900 ms
			time.Sleep(time.Duration((math.Pow(2, float64(retry))*50)+100) * time.Millisecond)

			retry += 1

			continue
		}

		return result, err
	}
}

func (d *DB) QueryRetryBusy(query string, args ...any) (*sql.Rows, error) {
	retry := 0

	for {
		rows, err := d.db.Query(query, args...)
		if err != nil && retry < 5 && strings.Contains(err.Error(), "database is locked (5) (SQLITE_BUSY)") {
			// retry 0: 2^0 * 50 + 100 = 150 ms
			// retry 1: 2^1 * 50 + 100 = 200 ms
			// retry 2: 2^2 * 50 + 100 = 300 ms
			// retry 3: 2^3 * 50 + 100 = 500 ms
			// retry 4: 2^4 * 50 + 100 = 900 ms
			time.Sleep(time.Duration((math.Pow(2, float64(retry))*50)+100) * time.Millisecond)

			retry += 1

			continue
		}

		return rows, err
	}

}
