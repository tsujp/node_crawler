package database

import (
	"context"
	_ "embed"
	"fmt"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/node-crawler/pkg/metrics"
	"golang.org/x/exp/slices"
)

func (db *DB) AttachStatsDB(filename string) error {
	statsConn, err := db.db.Conn(context.Background())
	if err != nil {
		return fmt.Errorf("getting connection failed: %w", err)
	}

	_, err = statsConn.ExecContext(
		context.Background(),
		fmt.Sprintf(`ATTACH DATABASE '%s' AS stats`, filename),
	)
	if err != nil {
		return fmt.Errorf("exec failed: %w", err)
	}

	db.statsConn = statsConn

	return nil
}

//go:embed sql/stats.sql
var statsSchema string

func (db *DB) CreateStatsTables() error {
	_, err := db.statsConn.ExecContext(context.Background(), statsSchema)
	if err != nil {
		return fmt.Errorf("creating schema failed: %w", err)
	}

	return nil
}

// Meant to be run as a goroutine.
//
// Copies the stats into the stats table every `frequency` duration.
func (db *DB) CopyStatsDaemon(frequency time.Duration) {
	for {
		start := time.Now()
		nextRun := start.Truncate(frequency).Add(frequency)

		err := db.CopyStats()
		if err != nil {
			log.Error("Copy stats failed", "err", err)
		}

		time.Sleep(time.Until(nextRun))
	}
}

func (db *DB) CopyStats() error {
	var err error

	start := time.Now()
	defer metrics.ObserveDBQuery("copy_stats", start, err)

	_, err = db.statsConn.ExecContext(
		context.Background(),
		`
			INSERT INTO stats.crawled_nodes
		
			SELECT
				unixepoch() timestamp,
				crawled.client_name,
				crawled.client_version,
				crawled.client_os,
				crawled.client_arch,
				crawled.network_id,
				crawled.fork_id,
				crawled.next_fork_id,
				crawled.country,
				CASE
					WHEN blocks.timestamp IS NULL
					THEN false
					ELSE abs(crawled.updated_at - blocks.timestamp) < 60
				END synced,
				EXISTS (
					SELECT 1
					FROM crawl_history
					WHERE
						node_id = crawled.node_id
						AND (
							error IS NULL
							OR error IN ('too many peers')
						)
				) dial_success,
				COUNT(*) total
			FROM crawled_nodes crawled
			LEFT JOIN discovered_nodes disc ON (
				crawled.node_id = disc.node_id
			)
			LEFT JOIN blocks ON (
				crawled.head_hash = blocks.block_hash
				AND crawled.network_id = blocks.network_id
			)
			WHERE
				disc.last_found > unixepoch('now', '-24 hours')
			GROUP BY
				crawled.client_name,
				crawled.client_version,
				crawled.client_os,
				crawled.client_arch,
				crawled.network_id,
				crawled.fork_id,
				crawled.next_fork_id,
				crawled.country,
				synced
		`,
	)
	if err != nil {
		return fmt.Errorf("exec failed: %w", err)
	}

	return nil
}

type Stats struct {
	Client     Client
	NetworkID  *int64
	ForkID     *uint32
	NextForkID *uint64
	Country    *string
	Synced     string
}

func (s Stats) CountryStr() string {
	if s.Country == nil {
		return Unknown
	}

	return *s.Country
}

type AllStats []Stats

type KeyFn func(Stats) string
type StatsFilterFn func(int, Stats) bool

func (s AllStats) Filter(filters ...StatsFilterFn) AllStats {
	out := make(AllStats, 0, len(s))

	for i, stat := range s {
		skip := false
		for _, filter := range filters {
			if !filter(i, stat) {
				skip = true
				break
			}
		}

		if !skip {
			out = append(out, stat)
		}
	}

	return out
}

func (s AllStats) GroupBy(keyFn KeyFn, filters ...StatsFilterFn) CountTotal {
	count := map[string]int{}

	for _, stat := range s.Filter(filters...) {
		key := keyFn(stat)

		v, ok := count[key]
		if !ok {
			v = 0
		}

		v += 1
		count[key] = v
	}

	out := make([]Count, 0, len(count))
	total := 0

	for key, value := range count {
		out = append(out, Count{
			Key:   key,
			Count: value,
		})

		total += value
	}

	slices.SortFunc(out, func(a, b Count) int {
		if a.Count == b.Count {
			return strings.Compare(b.Key, a.Key)
		}

		return b.Count - a.Count
	})

	return CountTotal{
		Values: out,
		Total:  total,
	}
}

type Count struct {
	Key   string
	Count int
}

type CountTotal struct {
	Values []Count
	Total  int
}

func (t CountTotal) Limit(limit int) CountTotal {
	return CountTotal{
		Values: t.Values[:min(limit, len(t.Values))],
		Total:  t.Total,
	}
}

type CountTotalOrderFn func(a, b Count) int

func (t CountTotal) OrderBy(orderByFn CountTotalOrderFn) CountTotal {
	slices.SortStableFunc(t.Values, orderByFn)

	return CountTotal{
		Values: t.Values,
		Total:  t.Total,
	}
}

func (s AllStats) CountClientName(filters ...StatsFilterFn) CountTotal {
	return s.GroupBy(
		func(s Stats) string {
			return s.Client.Name
		},
		filters...,
	)
}

func (s AllStats) GroupCountries(filters ...StatsFilterFn) CountTotal {
	return s.GroupBy(
		func(s Stats) string {
			return s.CountryStr()
		},
		filters...,
	)
}

func (s AllStats) GroupOS(filters ...StatsFilterFn) CountTotal {
	return s.GroupBy(
		func(s Stats) string {
			return s.Client.OS + " / " + s.Client.Arch
		},
		filters...,
	)
}

func (s AllStats) GroupArch(filters ...StatsFilterFn) CountTotal {
	return s.GroupBy(
		func(s Stats) string {
			return s.Client.Arch
		},
		filters...,
	)
}

func (s AllStats) GroupLanguage(filters ...StatsFilterFn) CountTotal {
	return s.GroupBy(
		func(s Stats) string {
			return s.Client.Language
		},
		filters...,
	)
}
