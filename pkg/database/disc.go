package database

import (
	"crypto/ecdsa"
	"database/sql"
	"encoding/hex"
	"fmt"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/p2p/enode"
)

type DiscDB struct {
	db *sql.DB
}

func NewDiscDB(db *sql.DB) *DiscDB {
	return &DiscDB{
		db: db,
	}
}

func (d *DiscDB) NodeKey() (*ecdsa.PrivateKey, error) {
	row := d.db.QueryRow("SELECT key FROM node_key LIMIT 1")
	if row.Err() != nil {
		return nil, fmt.Errorf("selecting node key failed: %w", row.Err())
	}

	keyHex := ""
	err := row.Scan(&keyHex)
	if err != nil {
		return nil, fmt.Errorf("scanning key failed: %w", err)
	}

	keyBytes, err := hex.DecodeString(keyHex)
	if err != nil {
		return nil, fmt.Errorf("decoding hex key failed: %w", err)
	}

	key, err := crypto.ToECDSA(keyBytes)
	if err != nil {
		return nil, fmt.Errorf("parsing key failed: %w", err)
	}

	return key, nil
}

func (d *DiscDB) UpsertNode(node *enode.Node) error {
	_, err := d.db.Exec(
		`
			INSERT INTO discovered_nodes
				(id, node, first_seen, last_seen)
			VALUES
				(?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
			ON CONFLICT (id) DO UPDATE
			SET
				node = excluded.node,
				last_seen = CURRENT_TIMESTAMP
		`,
		node.ID().String(),
		node.String(),
	)
	if err != nil {
		return fmt.Errorf("upserting node failed: %w", err)
	}

	return nil
}

func (d *DiscDB) CreateTables() error {
	_, err := d.db.Exec(`
		CREATE TABLE IF NOT EXISTS discovered_nodes (
			id			TEXT		PRIMARY KEY,
			node		TEXT		NOT NULL,
			first_seen	TIMESTAMPTZ	NOT NULL DEFAULT CURRENT_TIMESTAMP,
			last_seen	TIMESTAMPTZ	NOT NULL DEFAULT CURRENT_TIMESTAMP
		);

		CREATE TABLE IF NOT EXISTS node_key (
			key	TEXT	NOT NULL
		);
	`)
	if err != nil {
		return fmt.Errorf("creating table discovered_nodes failed: %w", err)
	}

	key, _ := crypto.GenerateKey()
	keyStr := hex.EncodeToString(crypto.FromECDSA(key))
	_, err = d.db.Exec(
		`
			INSERT INTO node_key (key)
			VALUES (?)
		`,
		keyStr,
	)
	if err != nil {
		return fmt.Errorf("inserting private key failed: %w", err)
	}

	return nil
}
