package database

import (
	"database/sql"
	"encoding/hex"
	"fmt"
	"strings"
)

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
