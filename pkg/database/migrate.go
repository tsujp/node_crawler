package database

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"time"

	"github.com/ethereum/go-ethereum/log"
)

type migrationFn func(*sql.Tx) error

func (db *DB) migrate(
	database string,
	migrations []migrationFn,
	staticObjects migrationFn,
) error {
	tx, err := db.db.BeginTx(context.Background(), nil)
	if err != nil {
		return fmt.Errorf("begin transaction failed: %w", err)
	}
	defer tx.Rollback()

	_, err = tx.Exec(fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.schema_versions (
			version		INTEGER	PRIMARY KEY,
			timestamp	INTEGER	NOT NULL
		);
	`, database))
	if err != nil {
		return fmt.Errorf("creating schema version table failed: %w", err)
	}

	schemaVersions, err := schemaVersions(tx, database)
	if err != nil {
		return fmt.Errorf("selecting schema version failed: %w", err)
	}

	migrationRan := false

	for version, migration := range migrations {
		if schemaVersions.Exists(version) {
			continue
		}

		migrationRan = true

		start := time.Now()
		log.Info("running migration", "database", database, "version", version)

		err := migration(tx)
		if err != nil {
			return fmt.Errorf("migration (%d) failed: %w", version, err)
		}

		tx.Exec(
			fmt.Sprintf(`
				INSERT INTO %s.schema_versions (
					version, timestamp
				) VALUES (
					?, unixepoch()
				)
			`, database),
			version,
		)

		log.Info(
			"migration complete",
			"database", database,
			"version", version,
			"duration", time.Since(start),
		)
	}

	err = staticObjects(tx)
	if err != nil {
		return fmt.Errorf("create indexes failed: %w", err)
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("commit transaction failed: %w", err)
	}

	if migrationRan {
		start := time.Now()
		log.Info("running vacuum", "database", database)

		_, err = db.db.Exec(fmt.Sprintf("VACUUM %s", database))
		if err != nil {
			return fmt.Errorf("vacuum failed: %w", err)
		}

		log.Info(
			"vacuum done",
			"database", database,
			"duration", time.Since(start),
		)
	}

	return nil
}

type schemaVersion struct {
	Version   int
	Timestamp int64
}

type schemaVersionSlice []schemaVersion

func (s schemaVersionSlice) Exists(i int) bool {
	return slices.ContainsFunc(s, func(sv schemaVersion) bool {
		return sv.Version == i
	})
}

func schemaVersions(tx *sql.Tx, database string) (schemaVersionSlice, error) {
	rows, err := tx.Query(fmt.Sprintf(
		"SELECT version, timestamp FROM %s.schema_versions",
		database,
	))
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	versions := []schemaVersion{}

	for rows.Next() {
		//nolint:exhaustruct  // Empty struct for scan
		sv := schemaVersion{}

		err := rows.Scan(&sv.Version, &sv.Timestamp)
		if err != nil {
			return nil, fmt.Errorf("scan row failed: %w", err)
		}

		versions = append(versions, sv)
	}

	err = rows.Err()
	if err != nil {
		return nil, fmt.Errorf("rows failed: %w", err)
	}

	return versions, nil
}
