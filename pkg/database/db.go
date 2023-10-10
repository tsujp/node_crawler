package database

import (
	"database/sql"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/oschwald/geoip2-golang"
)

type DB struct {
	db      *sql.DB
	geoipDB *geoip2.Reader
}

func NewDB(db *sql.DB, geoipDB *geoip2.Reader) *DB {
	return &DB{
		db:      db,
		geoipDB: geoipDB,
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

		CREATE INDEX IF NOT EXISTS id_next_crawl ON discovered_nodes (id, next_crawl);

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
			sequence		INTEGER		DEFAULT NULL,
			score			INTEGER		DEFAULT NULL
		);

		CREATE INDEX IF NOT EXISTS id_last_seen ON crawled_nodes (id, updated_at);

		CREATE TABLE IF NOT EXISTS crawl_history (
			id			TEXT		NOT NULL,
			crawled_at	TIMESTAMPTZ	NOT NULL DEFAULT CURRENT_TIMESTAMP,
			error		TEXT		DEFAULT NULL,

			PRIMARY KEY (id, crawled_at)
		);
	`)
	if err != nil {
		return fmt.Errorf("creating table discovered_nodes failed: %w", err)
	}

	return nil
}

func (d *DB) ExecRetryBusy(retry int, query string, args ...any) (sql.Result, error) {
	result, err := d.db.Exec(query, args...)
	if err != nil && retry < 5 && strings.Contains(err.Error(), "database is locked (5) (SQLITE_BUSY)") {
		// retry 0: 2^0 * 50 + 100 = 150 ms
		// retry 1: 2^1 * 50 + 100 = 200 ms
		// retry 2: 2^2 * 50 + 100 = 300 ms
		// retry 3: 2^3 * 50 + 100 = 500 ms
		// retry 4: 2^4 * 50 + 100 = 900 ms
		time.Sleep(time.Duration((math.Pow(2, float64(retry))*50)+100) * time.Millisecond)

		return d.ExecRetryBusy(retry+1, query, args...)
	}

	return result, err
}
