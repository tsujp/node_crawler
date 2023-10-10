package main

import (
	"database/sql"
	"fmt"
)

func setPragma(db *sql.DB, query string, expected string) error {
	row := db.QueryRow(query)
	if row.Err() != nil {
		return fmt.Errorf("quering row failed: %w", row.Err())
	}

	if expected == "" {
		return nil
	}

	var result string

	err := row.Scan(&result)
	if err != nil {
		return fmt.Errorf("scan row failed: %w", err)
	}

	if result != expected {
		return fmt.Errorf("result not expected. result: %s, expected: %s", result, expected)
	}

	return nil
}

func openSQLiteDB(
	name,
	autovacuum string,
	busyTimeout uint64,
) (*sql.DB, error) {
	db, err := sql.Open("sqlite", name)
	if err != nil {
		return nil, fmt.Errorf("error opening database: %w", err)
	}

	err = setPragma(db, "PRAGMA auto_vacuum = "+autovacuum, "")
	if err != nil {
		return nil, fmt.Errorf("setting auto_vacuum failed: %w", err)
	}

	err = setPragma(db, fmt.Sprintf("PRAGMA busy_timeout = %d", busyTimeout), fmt.Sprintf("%d", busyTimeout))
	if err != nil {
		return nil, fmt.Errorf("error setting busy_timeout: %w", err)
	}

	err = setPragma(db, "PRAGMA journal_mode = WAL", "wal")
	if err != nil {
		return nil, fmt.Errorf("error setting journal_mode = WAL: %w", err)
	}

	return db, nil
}
