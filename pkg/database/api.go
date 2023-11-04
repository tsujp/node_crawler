package database

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/ethereum/node-crawler/pkg/metrics"
)

func BytesToUnit32(b []byte) uint32 {
	return binary.BigEndian.Uint32(b)
}

func (db *DB) GetNodeTable(ctx context.Context, nodeID string) (*NodeTable, error) {
	var err error

	start := time.Now()
	defer metrics.ObserveDBQuery("get_node_table", start, err)

	nodeIDBytes, err := hex.DecodeString(nodeID)
	if err != nil {
		return nil, fmt.Errorf("decoding node id failed: %w", err)
	}

	row := db.db.QueryRowContext(
		ctx,
		`
			SELECT
				disc.node_id,
				crawled.updated_at,
				network_address,
				client_name,
				rlpx_version,
				capabilities,
				crawled.network_id,
				fork_id,
				next_fork_id,
				head_hash,
				blocks.timestamp,
				disc.ip_address,
				connection_type,
				country,
				city,
				latitude,
				longitude,
				next_crawl
			FROM discovered_nodes AS disc
			LEFT JOIN crawled_nodes AS crawled ON (disc.node_id = crawled.node_id)
			LEFT JOIN blocks ON (
				crawled.head_hash = blocks.block_hash
				AND crawled.network_id = blocks.network_id
			)
			WHERE disc.node_id = ?;
		`,
		nodeIDBytes,
	)

	nodePage := new(NodeTable)

	var updatedAtInt, headHashTimeInt, nextCrawlInt *int64
	var forkIDInt *uint32

	err = row.Scan(
		&nodePage.nodeID,
		&updatedAtInt,
		&nodePage.Enode,
		&nodePage.ClientName,
		&nodePage.RlpxVersion,
		&nodePage.Capabilities,
		&nodePage.networkID,
		&forkIDInt,
		&nodePage.NextForkID,
		&nodePage.HeadHash,
		&headHashTimeInt,
		&nodePage.IP,
		&nodePage.ConnectionType,
		&nodePage.Country,
		&nodePage.City,
		&nodePage.Latitude,
		&nodePage.Longitude,
		&nextCrawlInt,
	)
	if err != nil {
		return nil, fmt.Errorf("row scan failed: %w", err)
	}

	nodePage.updatedAt = int64PrtToTimePtr(updatedAtInt)
	nodePage.HeadHashTime = int64PrtToTimePtr(headHashTimeInt)
	nodePage.nextCrawl = int64PrtToTimePtr(nextCrawlInt)

	if forkIDInt != nil {
		fid := Uint32ToForkID(*forkIDInt)
		nodePage.ForkID = &fid
	}

	rows, err := db.db.QueryContext(
		ctx,
		`
			SELECT
				crawled_at,
				direction,
				coalesce(error, '')
			FROM crawl_history
			WHERE
				node_id = ?
			LIMIT 10
		`,
		nodeIDBytes,
	)
	if err != nil {
		return nil, fmt.Errorf("history query failed: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		history := NodeTableHistory{}
		var crawledAtInt int64

		err = rows.Scan(
			&crawledAtInt,
			&history.Direction,
			&history.Error,
		)
		if err != nil {
			return nil, fmt.Errorf("history row scan failed: %w", err)
		}

		history.CrawledAt = time.Unix(crawledAtInt, 0)

		nodePage.History = append(nodePage.History, history)
	}

	err = rows.Err()
	if err != nil {
		return nil, fmt.Errorf("history rows iteration failed: %w", err)
	}

	return nodePage, nil
}

func (db *DB) GetNodeList(
	ctx context.Context,
	pageNumber int,
	networkID int64,
	synced int,
	query string,
) (*NodeList, error) {
	var err error

	start := time.Now()
	defer metrics.ObserveDBQuery("get_node_list", start, err)

	pageSize := 10
	offset := (pageNumber - 1) * pageSize

	rows, err := db.db.QueryContext(
		ctx,
		`
			SELECT
				node_id,
				updated_at,
				client_name,
				country,
				blocks.timestamp,
				COUNT(*) OVER () AS total
			FROM crawled_nodes AS crawled
			LEFT JOIN blocks ON (
				crawled.head_hash = blocks.block_hash
				AND crawled.network_id = blocks.network_id
			)
			WHERE
				(      -- Network ID filter
					crawled.network_id = ?1
					OR ?1 = -1
				)
				AND (  -- Synced filter
					?2 = -1  -- All
					OR (     -- Not synced
						(
							abs(crawled.updated_at - blocks.timestamp) >= 60
							OR blocks.timestamp IS NULL
						)
						AND ?2 = 0
					)
					OR (     -- Synced
						abs(crawled.updated_at - blocks.timestamp) < 60
						AND ?2 = 1
					)
				)
				AND (  -- Query filter
					?3 = ''
					OR ip_address = ?3
					OR hex(node_id) LIKE upper(?3 || '%')
				)
			ORDER BY node_id
			LIMIT ?4
			OFFSET ?5
		`,
		networkID,
		synced,
		query,
		pageSize,
		offset,
	)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	out := NodeList{
		PageSize:      pageSize,
		PageNumber:    pageNumber,
		Synced:        synced,
		Offset:        offset,
		Total:         0,
		List:          []NodeListRow{},
		Networks:      []int64{},
		NetworkFilter: networkID,
		Query:         query,
	}

	for rows.Next() {
		row := NodeListRow{}
		var updatedAtInt, headHashTimeInt *int64

		err = rows.Scan(
			&row.nodeID,
			&updatedAtInt,
			&row.ClientName,
			&row.Country,
			&headHashTimeInt,
			&out.Total,
		)
		if err != nil {
			return nil, fmt.Errorf("scan row failed: %w", err)
		}

		row.UpdatedAt = int64PrtToTimePtr(updatedAtInt)
		row.HeadHashTimestamp = int64PrtToTimePtr(headHashTimeInt)

		out.List = append(out.List, row)
	}
	err = rows.Err()
	if err != nil {
		return nil, fmt.Errorf("rows failed: %w", err)
	}

	rows.Close()

	rows, err = db.db.QueryContext(
		ctx,
		`
			SELECT
				DISTINCT network_id
			FROM crawled_nodes
			WHERE
				network_id IS NOT NULL
			ORDER BY network_id
		`,
	)
	if err != nil {
		return nil, fmt.Errorf("networks query failed: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var networkID int64

		err = rows.Scan(&networkID)
		if err != nil {
			return nil, fmt.Errorf("networks scan failed: %w", err)
		}

		out.Networks = append(out.Networks, networkID)
	}

	err = rows.Err()
	if err != nil {
		return nil, fmt.Errorf("networks rows failed: %w", err)
	}

	rows.Close()

	return &out, nil
}

func (db *DB) GetStats(ctx context.Context) (AllStats, error) {
	var err error

	start := time.Now()
	defer metrics.ObserveDBQuery("get_stats", start, err)

	rows, err := db.db.QueryContext(
		ctx,
		`
			SELECT
				client_name,
				crawled.network_id,
				country,
				updated_at,
				blocks.timestamp
			FROM crawled_nodes AS crawled
			LEFT JOIN blocks ON (
				crawled.head_hash = blocks.block_hash
				AND crawled.network_id = blocks.network_id
			)
		`,
	)
	if err != nil {
		return AllStats{}, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	allStats := make([]Stats, 0, 1024)

	for rows.Next() {
		stats := Stats{}
		var name *string
		var updatedAtInt, blockTimestampInt *int64

		err := rows.Scan(
			&name,
			&stats.NetworkID,
			&stats.Country,
			&updatedAtInt,
			&blockTimestampInt,
		)
		if err != nil {
			return AllStats{}, fmt.Errorf("scan failed: %w", err)
		}

		updatedAt := int64PrtToTimePtr(updatedAtInt)
		blockTimestamp := int64PrtToTimePtr(blockTimestampInt)

		client := parseClientName(name)
		if client != nil {
			stats.Synced = isSynced(updatedAt, blockTimestamp)
			stats.Client = *client
			allStats = append(allStats, stats)
		}
	}

	return AllStats(allStats), nil
}
