package database

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/node-crawler/pkg/metrics"
	"golang.org/x/exp/slices"
)

type NodeTableHistory struct {
	CrawledAt string
	Direction string
	Error     string
}

type NodeTable struct {
	ID                string
	updatedAt         string
	Enode             string
	ClientName        string
	RlpxVersion       string
	Capabilities      string
	NetworkID         int
	ForkID            string
	NextForkID        string
	BlockHeight       string
	HeadHash          string
	HeadHashTimestamp string
	IP                string
	ConnectionType    string
	Country           string
	City              string
	Latitude          float64
	Longitude         float64
	Sequence          string
	nextCrawl         string

	History []NodeTableHistory
}

func (n NodeTable) YOffsetPercent() int {
	return 100 - int((n.Latitude+90)/180*100)
}

func (n NodeTable) XOffsetPercent() int {
	return int((n.Longitude + 180) / 360 * 100)
}

func (n NodeTable) HeadHashLine() string {
	if n.HeadHashTimestamp == "" {
		return n.HeadHash
	}

	return fmt.Sprintf("%s (%s)", n.HeadHash, n.HeadHashTimestamp)
}

func isSynced(ts1 string, ts2 string) string {
	t1, err := time.ParseInLocation(time.DateTime, ts1, time.UTC)
	if err != nil {
		return "Unknown"
	}

	t2, err := time.ParseInLocation(time.DateTime, ts2, time.UTC)
	if err != nil {
		return "Unknown"
	}

	// If head hash is within one minute of the crawl time,
	// we can consider the node in sync
	if t1.Sub(t2).Abs() < time.Minute {
		return "Yes"
	}

	return "No"
}

func (n NodeTable) IsSynced() string {
	return isSynced(n.updatedAt, n.HeadHashTimestamp)
}

func sinceUpdate(updatedAt string) string {
	t, err := time.ParseInLocation(time.DateTime, updatedAt, time.UTC)
	if err != nil {
		return updatedAt
	}

	since := time.Since(t)
	if since < 0 {
		return "In " + (-since).Truncate(time.Second).String()
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

func NetworkName(networkID int) string {
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
	return NetworkName(n.NetworkID)
}

func (db *DB) GetNodeTable(ctx context.Context, nodeID string) (*NodeTable, error) {
	var err error

	start := time.Now()
	defer metrics.ObserveDBQuery("get_node_table", time.Since(start), err)

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
				coalesce(crawled.network_id, -1),
				coalesce(fork_id, ''),
				coalesce(CAST(next_fork_id AS TEXT), ''),
				coalesce(block_height, ''),
				coalesce(head_hash, ''),
				coalesce(blocks.timestamp, ''),
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
			LEFT JOIN blocks ON (
				crawled.head_hash = blocks.block_hash
				AND crawled.network_id = blocks.network_id
			)
			WHERE disc.id = ?;
		`,
		nodeID,
	)

	nodePage := new(NodeTable)

	err = row.Scan(
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
		&nodePage.HeadHashTimestamp,
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
	ID                string
	UpdatedAt         string
	ClientName        string
	Country           string
	HeadHashTimestamp string
}

func (n NodeListRow) SinceUpdate() string {
	return sinceUpdate(n.UpdatedAt)
}

func (n NodeListRow) IsSynced() string {
	return isSynced(n.UpdatedAt, n.HeadHashTimestamp)
}

type NodeList struct {
	PageNumber    int
	PageSize      int
	Synced        int
	Offset        int
	Total         int
	NetworkFilter int
	List          []NodeListRow

	Networks []int
}

func (_ NodeList) NetworkName(networkID int) string {
	return fmt.Sprintf("%s (%d)", NetworkName(networkID), networkID)
}

func (l NodeList) NPages() int {
	return int(math.Ceil(float64(l.Total) / float64(l.PageSize)))
}

func (db *DB) GetNodeList(ctx context.Context, pageNumber int, networkID int, synced int) (*NodeList, error) {
	var err error

	start := time.Now()
	defer metrics.ObserveDBQuery("get_node_list", time.Since(start), err)

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
				coalesce(blocks.timestamp, ''),
				COUNT(*) OVER () AS total
			FROM crawled_nodes AS crawled
			LEFT JOIN blocks ON (
				crawled.head_hash = blocks.block_hash
				AND crawled.network_id = blocks.network_id
			)
			WHERE
				(
					crawled.network_id = ?1
					OR ?1 = -1
				)
				AND (
					?2 = -1  -- All
					OR (     -- Not synced
						(
							abs(unixepoch(crawled.updated_at) - unixepoch(blocks.timestamp)) >= 60
							OR blocks.timestamp IS NULL
						)
						AND ?2 = 0
					)
					OR (     -- Synced
						abs(unixepoch(crawled.updated_at) - unixepoch(blocks.timestamp)) < 60
						AND ?2 = 1
					)
				)
			ORDER BY id
			LIMIT ?3
			OFFSET ?4
		`,
		networkID,
		synced,
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
			&row.HeadHashTimestamp,
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

type Stats struct {
	Client    Client
	NetworkID int
	Country   string
	Synced    string
}

type AllStats []Stats

type StatsFilterFn func(Stats) bool

type Count struct {
	Key   string
	Count int
}

func (s AllStats) CountClientName(filters ...StatsFilterFn) []Count {
	count := map[string]int{}

	for _, stat := range s {
		skip := false
		for _, filter := range filters {
			if !filter(stat) {
				skip = true
				break
			}
		}

		if skip {
			continue
		}

		v, ok := count[stat.Client.Name]
		if !ok {
			v = 0
		}

		v += 1
		count[stat.Client.Name] = v
	}

	out := make([]Count, 0, len(count))

	for key, value := range count {
		out = append(out, Count{
			Key:   key,
			Count: value,
		})
	}

	slices.SortFunc(out, func(a, b Count) int {
		if a.Count == b.Count {
			return strings.Compare(a.Key, b.Key)
		}

		return a.Count - b.Count
	})

	return out
}

type Client struct {
	Name     string
	Version  string
	OS       string
	Language string
}

func parseClientName(clientName string) *Client {
	clientName = strings.ToLower(clientName)

	if clientName == "" {
		return nil
	}

	if clientName == "server" {
		return nil
	}

	if strings.HasPrefix(clientName, "nimbus-eth1") {
		newClientName := make([]rune, 0, len(clientName))
		for _, c := range clientName {
			switch c {
			case '[', ']', ':', ',':
				// NOOP
			default:
				newClientName = append(newClientName, c)
			}
		}

		parts := strings.Split(string(newClientName), " ")

		if len(parts) != 7 {
			log.Error("nimbus-eth1 not valid", "client_name", clientName)
		}

		return &Client{
			Name:     parts[0],
			Version:  parts[1],
			OS:       parts[2],
			Language: "nim",
		}
	}

	parts := strings.Split(strings.ToLower(clientName), "/")

	if parts[0] == "" {
		return nil
	}

	switch len(parts) {
	case 1:
		return &Client{
			Name: parts[0],
		}
	case 2:
		return &Client{
			Name:    parts[0],
			Version: parts[1],
		}
	case 3:
		lang := ""

		if parts[0] == "reth" {
			lang = "rust"
		} else if parts[0] == "geth" {
			lang = "go"
		} else {
			log.Error("not reth or geth", "client_name", clientName)
		}

		return &Client{
			Name:     parts[0],
			Version:  parts[1],
			OS:       parts[2],
			Language: lang,
		}
	case 4:
		return &Client{
			Name:     parts[0],
			Version:  parts[1],
			OS:       parts[2],
			Language: parts[3],
		}
	case 5:
		return &Client{
			// Name:     parts[0] + "/" + parts[1],
			Name:     parts[0],
			Version:  parts[2],
			OS:       parts[3],
			Language: parts[4],
		}
	case 6:
		if parts[0] == "q-client" {
			return &Client{
				Name:     parts[0],
				Version:  parts[1],
				OS:       parts[4],
				Language: parts[5],
			}
		}
	case 7:
		return &Client{
			Name:     parts[0],
			Version:  parts[4],
			OS:       parts[5],
			Language: parts[6],
		}
	}

	log.Error("could not parse client", "client_name", clientName)

	return nil
}

func (db *DB) GetStats(ctx context.Context) (AllStats, error) {
	rows, err := db.db.QueryContext(
		ctx,
		`
			SELECT
				client_name,
				crawled.network_id,
				coalesce(country, ''),
				updated_at,
				coalesce(blocks.timestamp, '')
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

	allStats := make([]Stats, 1024)

	for rows.Next() {
		stats := Stats{}
		name := ""
		updatedAt := ""
		blockTimestamp := ""

		err = rows.Scan(
			&name,
			&stats.NetworkID,
			&stats.Country,
			&updatedAt,
			&blockTimestamp,
		)

		client := parseClientName(name)
		if client != nil {
			stats.Synced = isSynced(updatedAt, blockTimestamp)
			stats.Client = *client
			allStats = append(allStats, stats)
		}
	}

	return AllStats(allStats), nil
}
