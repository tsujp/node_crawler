package database

import (
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/node-crawler/pkg/common"
	"github.com/ethereum/node-crawler/pkg/metrics"
)

func (d *DB) UpsertNode(node *enode.Node) error {
	var err error

	start := time.Now()
	defer metrics.ObserveDBQuery("disc_upsert_node", time.Since(start), err)

	_, err = d.ExecRetryBusy(
		`
			INSERT INTO discovered_nodes (
				id,
				node,
				ip_address,
				first_found,
				last_found,
				next_crawl
			) VALUES (
				?,
				?,
				?,
				CURRENT_TIMESTAMP,
				CURRENT_TIMESTAMP,
				CURRENT_TIMESTAMP
			)
			ON CONFLICT (id) DO UPDATE
			SET
				node = excluded.node,
				ip_address = excluded.ip_address,
				last_found = CURRENT_TIMESTAMP
			WHERE
				node != excluded.node
		`,
		node.ID().String(),
		node.String(),
		node.IP().String(),
	)
	if err != nil {
		return fmt.Errorf("exec failed: %w", err)
	}

	return nil
}

func (d *DB) SelectDiscoveredNodeSlice(limit int) ([]*enode.Node, error) {
	var err error

	start := time.Now()
	defer metrics.ObserveDBQuery("select_disc_node_slice", time.Since(start), err)

	rows, err := d.db.Query(
		`
			SELECT
				node
			FROM discovered_nodes
			WHERE
				next_crawl < CURRENT_TIMESTAMP
			ORDER BY next_crawl ASC
			LIMIT ?
		`,
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	out := make([]*enode.Node, 0, limit)
	for rows.Next() {
		enr := ""

		err = rows.Scan(&enr)
		if err != nil {
			return nil, fmt.Errorf("scanning row failed: %w", err)
		}

		node, err := common.ParseNode(enr)
		if err != nil {
			return nil, fmt.Errorf("parsing enr failed: %w, %s", err, enr)
		}

		out = append(out, node)
	}

	err = rows.Err()
	if err != nil {
		return nil, fmt.Errorf("rows iteration failed: %w", err)
	}

	return out, nil
}
