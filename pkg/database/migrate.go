package database

import (
	"fmt"

	"github.com/ethereum/go-ethereum/log"
)

func (db *DB) migrateCrawledNodes() error {
	tx, err := db.db.Begin()
	if err != nil {
		return fmt.Errorf("error starting transaction: %w", err)
	}
	defer tx.Rollback()

	_, err = tx.Exec("ALTER TABLE crawled_nodes RENAME TO crawled_nodes_old")
	if err != nil {
		return fmt.Errorf("error renaming table: %w", err)
	}

	_, err = tx.Exec(schema)
	if err != nil {
		return fmt.Errorf("create schema failed: %w", err)
	}

	rows, err := tx.Query(`
		SELECT
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
		FROM crawled_nodes_old
	`)
	if err != nil {
		return fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	stmt, err := tx.Prepare(`
		INSERT INTO crawled_nodes (
			node_id,
			updated_at,
			client_identifier,
			client_name,
			client_user_data,
			client_version,
			client_build,
			client_os,
			client_arch,
			client_language,
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
			nullif(?, ''),
			nullif(?, ''),
			nullif(?, ''),
			nullif(?, ''),
			nullif(?, ''),
			nullif(?, ''),
			nullif(?, ''),
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
		var id, headHash []byte
		var forkID *uint32
		var clientID, caps, ipAddress, connectionType, country, city *string
		var updatedAt int64
		var rlpxVersion, networkID, nextForkID *int64
		var latitude, longitude *float64

		rows.Scan(
			&id,
			&updatedAt,
			&clientID,
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

		clientPtr := parseClientID(clientID)
		if clientPtr == nil {
			log.Error("parsing client id failed", "id", clientID)
		}

		client := clientPtr.Deref()

		_, err = stmt.Exec(
			id,
			updatedAt,
			clientID,
			client.Name,
			client.UserData,
			client.Version,
			client.Build,
			client.OS,
			client.Arch,
			client.Language,
			rlpxVersion,
			caps,
			networkID,
			forkID,
			nextForkID,
			headHash,
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

	_, err = tx.Exec("DROP TABLE crawled_nodes_old")
	if err != nil {
		return fmt.Errorf("drop old table failed: %w", err)
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("commit tx failed: %w", err)
	}

	_, err = db.db.Exec("VACUUM")
	if err != nil {
		return fmt.Errorf("vacuum failed: %w", err)
	}

	return nil
}

func (db *DB) Migrate() error {
	err := db.migrateCrawledNodes()
	if err != nil {
		return fmt.Errorf("migrate crawled_nodes failed: %w", err)
	}

	return nil
}
