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
	name string,
	busyTimeout uint64,
	autovacuum string,
	journalMode string,
) (*sql.DB, error) {
	db, err := sql.Open("sqlite", name)
	if err != nil {
		return nil, fmt.Errorf("opening database failed: %w", err)
	}

	err = setPragma(db, fmt.Sprintf("PRAGMA busy_timeout = %d", busyTimeout), fmt.Sprintf("%d", busyTimeout))
	if err != nil {
		return nil, fmt.Errorf("setting busy_timeout failed: %w", err)
	}

	if autovacuum != "" {
		err = setPragma(db, "PRAGMA auto_vacuum = "+autovacuum, "")
		if err != nil {
			return nil, fmt.Errorf("setting auto_vacuum failed: %w", err)
		}
	}

	if journalMode != "" {
		err = setPragma(db, "PRAGMA journal_mode = "+journalMode, journalMode)
		if err != nil {
			return nil, fmt.Errorf("setting journal_mode = %s failed: %w", journalMode, err)
		}

		err = setPragma(db, "PRAGMA synchronous = NORMAL", "")
		if err != nil {
			return nil, fmt.Errorf("setting synchronous = NORMAL failed: %w", err)
		}
	}

	return db, nil
}
