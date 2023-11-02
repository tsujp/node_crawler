package database

import (
	"database/sql"
	"fmt"
	"net"
	"time"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/node-crawler/pkg/common"
	"github.com/ethereum/node-crawler/pkg/metrics"
)

type location struct {
	country   string
	city      string
	latitude  float64
	longitude float64
}

func (db *DB) IPToLocation(ip net.IP) (location, error) {
	if db.geoipDB == nil {
		return location{}, nil
	}

	ipRecord, err := db.geoipDB.City(ip)
	if err != nil {
		return location{}, fmt.Errorf("getting geoip failed: %w", err)
	}

	return location{
		country:   ipRecord.Country.Names["en"],
		city:      ipRecord.City.Names["en"],
		latitude:  ipRecord.Location.Latitude,
		longitude: ipRecord.Location.Longitude,
	}, nil
}

func (db *DB) UpdateCrawledNodeFail(node common.NodeJSON) error {
	var err error

	start := time.Now()
	defer metrics.ObserveDBQuery("update_crawled_node_fail", start, err)

	_, err = db.ExecRetryBusy(
		`
			INSERT INTO discovered_nodes (
				node_id,
				network_address,
				ip_address,
				first_found,
				last_found,
				next_crawl
			) VALUES (
				?1,
				?2,
				?3,
				unixepoch(),
				unixepoch(),
				unixepoch() + ?6
			)
			ON CONFLICT (node_id) DO UPDATE
			SET
				last_found = unixepoch(),
				next_crawl = excluded.next_crawl;

			INSERT INTO crawl_history (
				node_id,
				crawled_at,
				direction,
				error
			) VALUES (
				?1,
				unixepoch(),
				?4,
				?5
			);
		`,
		node.ID(),
		node.N.String(),
		node.N.IP().String(),
		node.Direction,
		node.Error,
		db.nextCrawlFail,
	)
	if err != nil {
		return fmt.Errorf("exec failed: %w", err)
	}

	return nil
}

func (db *DB) UpdateNotEthNode(node common.NodeJSON) error {
	var err error

	start := time.Now()
	defer metrics.ObserveDBQuery("update_crawled_node_not_eth", start, err)

	_, err = db.ExecRetryBusy(
		`
			INSERT INTO discovered_nodes (
				node_id,
				network_address,
				ip_address,
				first_found,
				last_found,
				next_crawl
			) VALUES (
				?1,
				?2,
				?3,
				unixepoch(),
				unixepoch(),
				unixepoch() + ?4
			)
			ON CONFLICT (node_id) DO UPDATE
			SET
				last_found = unixepoch(),
				next_crawl = excluded.next_crawl
		`,
		node.ID(),
		node.N.String(),
		node.N.IP().String(),
		db.nextCrawlNotEth,
	)
	if err != nil {
		return fmt.Errorf("exec failed: %w", err)
	}

	return nil
}

func (db *DB) UpdateCrawledNodeSuccess(node common.NodeJSON) error {
	var err error

	start := time.Now()
	defer metrics.ObserveDBQuery("update_crawled_node_success", start, err)

	info := node.GetInfo()
	ip := node.N.IP()

	location, err := db.IPToLocation(ip)
	if err != nil {
		return fmt.Errorf("geoip failed: %w", err)
	}

	if len(node.BlockHeaders) != 0 {
		err = db.InsertBlocks(node.BlockHeaders, node.Info.NetworkID)
		if err != nil {
			return fmt.Errorf("inserting blocks failed: %w", err)
		}
	}

	_, err = db.ExecRetryBusy(
		`
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
				?1,					-- node_id
				unixepoch(),		-- updated_at
				nullif(?2, ''),		-- client_name
				nullif(?3, 0),		-- rlpx_version
				nullif(?4, ''),		-- capabilities
				nullif(?5, 0),		-- network_id
				nullif(?6, 0),		-- fork_id
				nullif(?7, 0),		-- next_fork_id
				nullif(?8, X''),	-- head_hash
				nullif(?9, ''),		-- ip_address
				nullif(?10, ''),	-- connection_type
				nullif(?11, ''),	-- country
				nullif(?12, ''),	-- city
				nullif(?13, 0.0),	-- latitude
				nullif(?14, 0.0)	-- longitude
			)
			ON CONFLICT (node_id) DO UPDATE
			SET
				updated_at = unixepoch(),
				client_name = coalesce(excluded.client_name, client_name),
				rlpx_version = coalesce(excluded.rlpx_version, rlpx_version),
				capabilities = coalesce(excluded.capabilities, capabilities),
				network_id = coalesce(excluded.network_id, network_id),
				fork_id = coalesce(excluded.fork_id, fork_id),
				next_fork_id = excluded.next_fork_id,			-- Not coalesce because no next fork is valid
				head_hash = coalesce(excluded.head_hash, head_hash),
				ip_address = coalesce(excluded.ip_address, ip_address),
				connection_type = coalesce(excluded.connection_type, connection_type),
				country = coalesce(excluded.country, country),
				city = coalesce(excluded.city, city),
				latitude = coalesce(excluded.latitude, latitude),
				longitude = coalesce(excluded.longitude, longitude);

			INSERT INTO discovered_nodes (
				node_id,
				network_address,
				ip_address,
				first_found,
				last_found,
				next_crawl
			) VALUES (
				?1,
				?15,
				?9,
				unixepoch(),
				unixepoch(),
				unixepoch() + ?17
			)
			ON CONFLICT (node_id) DO UPDATE
			SET
				last_found = unixepoch(),
				next_crawl = excluded.next_crawl;

			INSERT INTO crawl_history (
				node_id,
				crawled_at,
				direction,
				error
			) VALUES (
				?1,
				unixepoch(),
				?16,
				NULL
			);
		`,
		node.ID(),
		info.ClientName,
		info.RLPxVersion,
		node.CapsString(),
		info.NetworkID,
		BytesToUnit32(info.ForkID.Hash[:]),
		info.ForkID.Next,
		info.HeadHash[:],
		node.N.IP().String(),
		node.ConnectionType(),
		location.country,
		location.city,
		location.latitude,
		location.longitude,
		node.N.String(),
		node.Direction,
		db.nextCrawlSucces,
	)
	if err != nil {
		return fmt.Errorf("exec failed: %w", err)
	}

	return nil
}

func (db *DB) InsertBlocks(blocks []*types.Header, networkID uint64) error {
	// tx, err := db.db.Begin()
	// if err != nil {
	// 	return fmt.Errorf("starting tx failed: %w", err)
	// }
	// defer tx.Rollback()

	stmt, err := db.db.Prepare(`
		INSERT INTO blocks (
			block_hash,
			network_id,
			timestamp,
			block_number,
			parent_hash
		) VALUES (
			?,
			?,
			?,
			?,
			?
		)
		ON CONFLICT (block_hash, network_id)
		DO NOTHING
	`)
	if err != nil {
		return fmt.Errorf("preparing statement failed: %w", err)
	}
	defer stmt.Close()

	for _, block := range blocks {
		start := time.Now()
		_, err = retryBusy(func() (sql.Result, error) {
			return stmt.Exec(
				block.Hash().Bytes(),
				networkID,
				block.Time,
				block.Number.Uint64(),
				block.ParentHash.Bytes(),
			)
		})
		metrics.ObserveDBQuery("insert_block", start, err)

		if err != nil {
			log.Error("upsert block failed", "err", err)
		}
	}

	// err = tx.Commit()
	// if err != nil {
	// 	return fmt.Errorf("commit failed: %w", err)
	// }

	return nil
}

func (db *DB) UpsertCrawledNode(node common.NodeJSON) error {
	if !node.EthNode {
		err := db.UpdateNotEthNode(node)
		if err != nil {
			return fmt.Errorf("update not eth node failed: %w", err)
		}

		return nil
	}

	if node.Error != "" {
		err := db.UpdateCrawledNodeFail(node)
		if err != nil {
			return fmt.Errorf("update failed crawl failed: %w", err)
		}

		return nil
	}

	return db.UpdateCrawledNodeSuccess(node)
}
