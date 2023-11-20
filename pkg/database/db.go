package database

import (
	"database/sql"
	_ "embed"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/node-crawler/pkg/metrics"
	"github.com/oschwald/geoip2-golang"
)

type DB struct {
	db        *sql.DB
	statsConn *sql.Conn
	geoipDB   *geoip2.Reader

	nextCrawlSucces int64
	nextCrawlFail   int64
	nextCrawlNotEth int64

	wLock sync.Mutex
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
		db:        db,
		statsConn: nil,
		geoipDB:   geoipDB,

		nextCrawlSucces: int64(nextCrawlSucces.Seconds()),
		nextCrawlFail:   int64(nextCrawlFail.Seconds()),
		nextCrawlNotEth: int64(nextCrawlNotEth.Seconds()),

		wLock: sync.Mutex{},
	}
}

func (db *DB) Close() error {
	return db.db.Close()
}

func (db *DB) StatsConn() *sql.Conn {
	return db.statsConn
}

func (db *DB) AnalyzeDaemon(frequency time.Duration) {
	for {
		start := time.Now()
		nextAnalyze := start.Add(frequency)

		db.wLock.Lock()
		_, err := db.db.Exec("ANALYZE")
		if err != nil {
			log.Error("ANALYZE failed", "err", err)
		}
		db.wLock.Unlock()
		metrics.ObserveDBQuery("analyze", start, err)

		time.Sleep(time.Until(nextAnalyze))
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
	defer metrics.ObserveDBQuery("table_stats", start, err)

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

	if rows.Next() {
		//nolint:exhaustruct
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
