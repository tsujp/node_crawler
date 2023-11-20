package database

import (
	"context"
	"database/sql"
	"fmt"
)

func setPragma(conn *sql.Conn, query string, expected string) error {
	row := conn.QueryRowContext(context.Background(), query)
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

func (db *DB) SetPragmas(
	conn *sql.Conn,
	database string,
	busyTimeout uint64,
	autovacuum string,
	journalMode string,
) error {
	err := setPragma(conn, fmt.Sprintf("PRAGMA %s.busy_timeout = %d", database, busyTimeout), fmt.Sprintf("%d", busyTimeout))
	if err != nil {
		return fmt.Errorf("setting busy_timeout failed: %w", err)
	}

	if autovacuum != "" {
		err = setPragma(conn, fmt.Sprintf("PRAGMA %s.auto_vacuum = %s", database, autovacuum), "")
		if err != nil {
			return fmt.Errorf("setting auto_vacuum failed: %w", err)
		}
	}

	if journalMode != "" {
		err = setPragma(conn, fmt.Sprintf("PRAGMA %s.journal_mode = %s", database, journalMode), journalMode)
		if err != nil {
			return fmt.Errorf("setting journal_mode = %s failed: %w", journalMode, err)
		}

		err = setPragma(conn, fmt.Sprintf("PRAGMA %s.synchronous = NORMAL", database), "")
		if err != nil {
			return fmt.Errorf("setting synchronous = NORMAL failed: %w", err)
		}
	}

	return nil
}
