package database

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"slices"
	"time"

	"github.com/ethereum/node-crawler/pkg/common"
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
				disc.last_found,
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

	var lastFound int64
	var updatedAtInt, headHashTimeInt, nextCrawlInt *int64
	var forkIDInt *uint32

	err = row.Scan(
		&nodePage.nodeID,
		&lastFound,
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

	nodePage.lastFound = time.Unix(lastFound, 0)
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
				error
			FROM (
				SELECT
					crawled_at,
					direction,
					coalesce(error, '') AS error,
					row_number() OVER (PARTITION BY direction) AS row
				FROM crawl_history
				WHERE
					node_id = ?
				ORDER BY crawled_at DESC
			)
			WHERE row <= 10
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

		if history.Direction == common.DirectionAccept {
			nodePage.HistoryAccept = append(nodePage.HistoryAccept, history)
		} else {
			nodePage.HistoryDial = append(nodePage.HistoryDial, history)
		}
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

	pageSize := 20
	offset := (pageNumber - 1) * pageSize

	rows, err := db.db.QueryContext(
		ctx,
		`
			SELECT
				disc.node_id,
				crawled.updated_at,
				crawled.client_name,
				crawled.country,
				blocks.timestamp
			FROM discovered_nodes AS disc
			LEFT JOIN crawled_nodes AS crawled ON (
				disc.node_id = crawled.node_id
			)
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
					OR disc.ip_address = ?3
					OR hex(disc.node_id) LIKE upper(?3 || '%')
				)
				AND (  -- If query is empty, crawled can't be NULL
					?3 != ''
					OR crawled.node_id IS NOT NULL
				)
			ORDER BY disc.node_id
			LIMIT ?4 + 1
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
		HasNextPage:   false,
		Synced:        synced,
		Offset:        offset,
		List:          []NodeListRow{},
		NetworkFilter: networkID,
		Query:         query,
	}

	rowNumber := 0
	for rows.Next() {
		rowNumber++

		// We added 1 to the LIMIT to see if there were any more rows for
		// a next page. This is where we test for that.
		if rowNumber > pageSize {
			out.HasNextPage = true

			break
		}

		row := NodeListRow{}
		var updatedAtInt, headHashTimeInt *int64

		err = rows.Scan(
			&row.nodeID,
			&updatedAtInt,
			&row.ClientName,
			&row.Country,
			&headHashTimeInt,
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
				crawled.client_name,
				crawled.network_id,
				crawled.fork_id,
				crawled.next_fork_id,
				crawled.country,
				crawled.updated_at,
				blocks.timestamp
			FROM discovered_nodes AS disc
			LEFT JOIN crawled_nodes AS crawled ON (
				disc.node_id = crawled.node_id
			)
			LEFT JOIN blocks ON (
				crawled.head_hash = blocks.block_hash
				AND crawled.network_id = blocks.network_id
			)
			WHERE disc.last_found > unixepoch('now', '-24 hours')
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
			&stats.ForkID,
			&stats.NextForkID,
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

type HistoryListRow struct {
	NodeID     string
	ClientName *string
	NetworkID  *int64
	CrawledAt  time.Time
	Direction  string
	Error      *string
}

func (r HistoryListRow) CrawledAtStr() string {
	return r.CrawledAt.UTC().Format(time.RFC3339)
}

func (r HistoryListRow) SinceCrawled() string {
	return sinceUpdate(&r.CrawledAt)
}

func (r HistoryListRow) NetworkIDStr() string {
	if r.NetworkID == nil {
		return ""
	}

	return fmt.Sprintf("%s (%d)", NetworkName(r.NetworkID), *r.NetworkID)
}

type HistoryList struct {
	Rows      []HistoryListRow
	NetworkID int64
	IsError   int
	Before    *time.Time
	After     *time.Time
	LastTime  *time.Time
	FirstTime *time.Time
}

var DateTimeLocal = "2006-01-02T15:04:05"

func (l HistoryList) BeforeStr() string {
	if l.Before == nil {
		return ""
	}

	return l.Before.UTC().Format(DateTimeLocal)
}

func (l HistoryList) AfterStr() string {
	if l.After == nil {
		return ""
	}

	return l.After.UTC().Format(DateTimeLocal)
}

func (l HistoryList) FirstTimeStr() string {
	if l.FirstTime == nil {
		return l.AfterStr()
	}

	return l.FirstTime.UTC().Format(DateTimeLocal)
}

func (l HistoryList) LastTimeStr() string {
	if l.FirstTime == nil {
		return l.BeforeStr()
	}

	return l.LastTime.UTC().Format(DateTimeLocal)
}

func timePtrToUnixPtr(t *time.Time) *int64 {
	if t == nil {
		return nil
	}

	u := t.Unix()
	return &u
}

func (db *DB) GetHistoryList(
	ctx context.Context,
	before *time.Time,
	after *time.Time,
	networkID int64,
	isError int,
) (*HistoryList, error) {
	var err error

	start := time.Now()
	defer metrics.ObserveDBQuery("get_history_list", start, err)

	queryOrderDirection := "ASC"
	if before == nil {
		queryOrderDirection = "DESC"
	}

	rows, err := db.db.QueryContext(
		ctx,
		// Don't ever do this, but we have no other choice because I could not
		// find another way to conditionally set the order direction. :(
		fmt.Sprintf(`
			SELECT
				history.node_id,
				crawled.client_name,
				crawled.network_id,
				history.crawled_at,
				history.direction,
				history.error
			FROM crawl_history AS history
			LEFT JOIN crawled_nodes AS crawled ON (history.node_id = crawled.node_id)
			WHERE
				(
					?1 IS NULL
					OR history.crawled_at >= ?1
				)
				AND (
					?2 IS NULL
					OR history.crawled_at <= ?2
				)
				AND (
					?3 = -1
					OR crawled.network_id = ?3
				)
				AND (
					?4 = -1
					OR (
						?4 = 0
						AND history.error IS NULL
					)
					OR (
						?4 = 1
						AND history.error IS NOT NULL
					)
				)
			ORDER BY history.crawled_at %s
			LIMIT 50
		`, queryOrderDirection),
		timePtrToUnixPtr(before),
		timePtrToUnixPtr(after),
		networkID,
		isError,
	)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	historyList := HistoryList{
		Rows:      []HistoryListRow{},
		NetworkID: networkID,
		IsError:   isError,
		Before:    before,
		After:     after,
		FirstTime: nil,
		LastTime:  nil,
	}

	for rows.Next() {
		row := HistoryListRow{}
		nodeIDBytes := make([]byte, 32)
		var crawledAtInt int64

		err := rows.Scan(
			&nodeIDBytes,
			&row.ClientName,
			&row.NetworkID,
			&crawledAtInt,
			&row.Direction,
			&row.Error,
		)
		if err != nil {
			return nil, fmt.Errorf("scan failed: %w", err)
		}

		row.NodeID = hex.EncodeToString(nodeIDBytes[:])
		row.CrawledAt = time.Unix(crawledAtInt, 0)

		historyList.Rows = append(historyList.Rows, row)
	}

	if len(historyList.Rows) > 0 {
		slices.SortStableFunc(historyList.Rows, func(a, b HistoryListRow) int {
			if a.CrawledAt.Equal(b.CrawledAt) {
				return 0
			}
			if a.CrawledAt.After(b.CrawledAt) {
				return -1
			}
			return 1
		})

		historyList.FirstTime = &historyList.Rows[0].CrawledAt
		historyList.LastTime = &historyList.Rows[len(historyList.Rows)-1].CrawledAt
	}

	return &historyList, nil
}
