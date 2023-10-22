package database

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/ethereum/go-ethereum/params"
)

type NodeTableHistory struct {
	CrawledAt string
	Direction string
	Error     string
}

type NodeTable struct {
	ID             string
	updatedAt      string
	Enode          string
	ClientName     string
	RlpxVersion    string
	Capabilities   string
	NetworkID      int
	ForkID         string
	NextForkID     string
	BlockHeight    string
	HeadHash       string
	IP             string
	ConnectionType string
	Country        string
	City           string
	Latitude       float64
	Longitude      float64
	Sequence       string
	nextCrawl      string

	History []NodeTableHistory
}

func (n NodeTable) YOffsetPercent() int {
	return 100 - int((n.Latitude+90)/180*100)
}

func (n NodeTable) XOffsetPercent() int {
	return int((n.Longitude + 180) / 360 * 100)
}

func sinceUpdate(updatedAt string) string {
	t, err := time.ParseInLocation(time.DateTime, updatedAt, time.UTC)
	if err != nil {
		return updatedAt
	}

	since := time.Since(t)
	if since < 0 {
		return "In " + since.Truncate(time.Second).String()
	}

	return since.Truncate(time.Second).String() + " ago"
}

func (n NodeTable) UpdatedAt() string {
	if n.updatedAt == "" {
		return "Never crawled"
	}

	since := sinceUpdate(n.updatedAt)
	return fmt.Sprintf("%s (%s)", since, n.updatedAt)
}

func (n NodeTable) NextCrawl() string {
	if n.nextCrawl == "" {
		return "Never"
	}

	return fmt.Sprintf("%s (%s)", sinceUpdate(n.nextCrawl), n.nextCrawl)
}

func networkName(networkID int) string {
	switch networkID {
	case int(params.MainnetChainConfig.ChainID.Int64()):
		return "Mainnet"
	case int(params.HoleskyChainConfig.ChainID.Int64()):
		return "Holesky"
	case int(params.SepoliaChainConfig.ChainID.Int64()):
		return "Sepolia"
	case int(params.GoerliChainConfig.ChainID.Int64()):
		return "Goerli"
	default:
		return "Unknown"
	}
}

func (n NodeTable) NetworkName() string {
	return networkName(n.NetworkID)
}

func (db *DB) GetNodeTable(ctx context.Context, nodeID string) (*NodeTable, error) {
	row := db.db.QueryRowContext(
		ctx,
		`
			SELECT
				disc.id,
				coalesce(crawled.updated_at, ''),
				node,
				coalesce(client_name, ''),
				coalesce(CAST(rlpx_version AS TEXT), ''),
				coalesce(capabilities, ''),
				coalesce(network_id, -1),
				coalesce(fork_id, ''),
				coalesce(CAST(next_fork_id AS TEXT), ''),
				coalesce(block_height, ''),
				coalesce(head_hash, ''),
				coalesce(disc.ip_address, ''),
				coalesce(connection_type, ''),
				coalesce(country, ''),
				coalesce(city, ''),
				coalesce(latitude, 0),
				coalesce(longitude, 0),
				coalesce(CAST(sequence AS TEXT), ''),
				coalesce(next_crawl, '')
			FROM discovered_nodes AS disc
			LEFT JOIN crawled_nodes AS crawled ON (disc.id = crawled.id)
			WHERE disc.id = ?;
		`,
		nodeID,
	)

	nodePage := new(NodeTable)

	err := row.Scan(
		&nodePage.ID,
		&nodePage.updatedAt,
		&nodePage.Enode,
		&nodePage.ClientName,
		&nodePage.RlpxVersion,
		&nodePage.Capabilities,
		&nodePage.NetworkID,
		&nodePage.ForkID,
		&nodePage.NextForkID,
		&nodePage.BlockHeight,
		&nodePage.HeadHash,
		&nodePage.IP,
		&nodePage.ConnectionType,
		&nodePage.Country,
		&nodePage.City,
		&nodePage.Latitude,
		&nodePage.Longitude,
		&nodePage.Sequence,
		&nodePage.nextCrawl,
	)
	if err != nil {
		return nil, fmt.Errorf("row scan failed: %w", err)
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
				id = ?
			LIMIT 10
		`,
		nodeID,
	)
	if err != nil {
		return nil, fmt.Errorf("history query failed: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		history := NodeTableHistory{}

		err = rows.Scan(
			&history.CrawledAt,
			&history.Direction,
			&history.Error,
		)
		if err != nil {
			return nil, fmt.Errorf("history row scan failed: %w", err)
		}

		nodePage.History = append(nodePage.History, history)
	}

	err = rows.Err()
	if err != nil {
		return nil, fmt.Errorf("history rows iteration failed: %w", err)
	}

	return nodePage, nil
}

type NodeListRow struct {
	ID         string
	UpdatedAt  string
	ClientName string
	Country    string
}

func (n NodeListRow) SinceUpdate() string {
	return sinceUpdate(n.UpdatedAt)
}

type NodeList struct {
	PageNumber    int
	PageSize      int
	Offset        int
	Total         int
	NetworkFilter int
	List          []NodeListRow

	Networks []int
}

func (_ NodeList) NetworkName(networkID int) string {
	return fmt.Sprintf("%s (%d)", networkName(networkID), networkID)
}

func (l NodeList) NPages() int {
	return int(math.Ceil(float64(l.Total) / float64(l.PageSize)))
}

func (db *DB) GetNodeList(ctx context.Context, pageNumber int, networkID int) (*NodeList, error) {
	pageSize := 10
	offset := (pageNumber - 1) * pageSize

	rows, err := db.db.QueryContext(
		ctx,
		`
			SELECT
				id,
				updated_at,
				coalesce(client_name, ''),
				coalesce(country, ''),
				COUNT(*) OVER () AS total
			FROM crawled_nodes
			WHERE
				(
					network_id = ?1
					OR -1 = ?1
				)
			ORDER BY id
			LIMIT ?2
			OFFSET ?3
		`,
		networkID,
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
		Offset:        offset,
		Total:         0,
		List:          []NodeListRow{},
		Networks:      []int{},
		NetworkFilter: networkID,
	}

	for rows.Next() {
		row := NodeListRow{}
		err = rows.Scan(
			&row.ID,
			&row.UpdatedAt,
			&row.ClientName,
			&row.Country,
			&out.Total,
		)
		if err != nil {
			return nil, fmt.Errorf("scan row failed: %w", err)
		}

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
		var networkID int

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
