package database

import (
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/node-crawler/pkg/common"
	"github.com/ethereum/node-crawler/pkg/metrics"
)

func (d *DB) UpsertNode(node *enode.Node) error {
	d.wLock.Lock()
	defer d.wLock.Unlock()

	var err error

	start := time.Now()
	defer metrics.ObserveDBQuery("disc_upsert_node", start, err)

	_, err = d.ExecRetryBusy(
		`
			INSERT INTO discovered_nodes (
				node_id,
				network_address,
				ip_address,
				first_found,
				last_found,
				next_crawl
			) VALUES (
				?,
				?,
				?,
				unixepoch(),
				unixepoch(),
				unixepoch()
			)
			ON CONFLICT (node_id) DO UPDATE
			SET
				network_address = excluded.network_address,
				ip_address = excluded.ip_address,
				last_found = unixepoch()
			WHERE
				network_address != excluded.network_address
		`,
		node.ID().Bytes(),
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
	defer metrics.ObserveDBQuery("select_disc_node_slice", start, err)

	rows, err := d.db.Query(
		`
			SELECT
				network_address
			FROM discovered_nodes
			WHERE
				next_crawl < unixepoch()
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
		var enr string

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
