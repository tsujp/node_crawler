package database

import (
	"database/sql"
	"fmt"
)

func (db *DB) MigrateStats() error {
	return db.migrate(
		"stats",
		[]migrationFn{
			migrateStats000Schema,
			migrateStats001AddUserData,
		},
		migrateStatsIndexes,
	)
}

func migrateStatsIndexes(tx *sql.Tx) error {
	_, err := tx.Exec(`
		CREATE INDEX IF NOT EXISTS stats.crawled_nodes_timestamp
			ON crawled_nodes (timestamp);
	`)
	if err != nil {
		return fmt.Errorf("exec failed: %w", err)
	}

	return nil
}

func migrateStats000Schema(tx *sql.Tx) error {
	_, err := tx.Exec(`
		CREATE TABLE IF NOT EXISTS stats.crawled_nodes (
			timestamp		INTEGER	NOT NULL,
			client_name		TEXT	DEFAULT NULL,
			client_version	TEXT	DEFAULT NULL,
			client_os		TEXT	DEFAULT NULL,
			client_arch		TEXT	DEFAULT NULL,
			network_id		INTEGER	DEFAULT NULL,
			fork_id			INTEGER	DEFAULT NULL,
			next_fork_id	INTEGER	DEFAULT NULL,
			country			TEXT	DEFAULT NULL,
			synced			INTEGER	NOT NULL,
			dial_success	INTEGER NOT NULL,
			total			INTEGER NOT NULL
		) STRICT;
	`)
	if err != nil {
		return fmt.Errorf("create initial schema failed: %w", err)
	}

	return nil
}

func migrateStats001AddUserData(tx *sql.Tx) error {
	_, err := tx.Exec(`
		ALTER TABLE stats.crawled_nodes RENAME TO crawled_nodes_old;

		CREATE TABLE stats.crawled_nodes (
			timestamp			INTEGER	NOT NULL,
			client_name			TEXT	DEFAULT NULL,
			client_user_data	TEXT	DEFAULT NULL,
			client_version		TEXT	DEFAULT NULL,
			client_os			TEXT	DEFAULT NULL,
			client_arch			TEXT	DEFAULT NULL,
			network_id			INTEGER	DEFAULT NULL,
			fork_id				INTEGER	DEFAULT NULL,
			next_fork_id		INTEGER	DEFAULT NULL,
			country				TEXT	DEFAULT NULL,
			synced				INTEGER	NOT NULL,
			dial_success		INTEGER NOT NULL,
			total				INTEGER NOT NULL
		) STRICT;

		INSERT INTO stats.crawled_nodes
		SELECT
			timestamp,
			client_name,
			'' client_user_data,
			client_version,
			client_os,
			client_arch,
			network_id,
			fork_id,
			next_fork_id,
			country,
			synced,
			dial_success,
			total
		FROM stats.crawled_nodes_old;

		DROP TABLE stats.crawled_nodes_old;
	`)
	if err != nil {
		return fmt.Errorf("error migrating table: %w", err)
	}

	return nil
}
