package database

import (
	"encoding/hex"
	"fmt"
	"net"
	"time"

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
	defer metrics.ObserveDBQuery("update_crawled_node_fail", time.Since(start), err)

	_, err = db.ExecRetryBusy(
		`
			INSERT INTO discovered_nodes (
				id,
				node,
				ip_address,
				first_found,
				last_found,
				next_crawl
			) VALUES (
				?1,
				?2,
				?3,
				CURRENT_TIMESTAMP,
				CURRENT_TIMESTAMP,
				datetime(CURRENT_TIMESTAMP, '+48 hours')
			)
			ON CONFLICT (id) DO UPDATE
			SET
				last_found = CURRENT_TIMESTAMP,
				next_crawl = excluded.next_crawl;

			INSERT INTO crawl_history (
				id,
				crawled_at,
				direction,
				error
			) VALUES (
				?1,
				CURRENT_TIMESTAMP,
				?4,
				?5
			);
		`,
		node.ID(),
		node.N.String(),
		node.N.IP().String(),
		node.Direction,
		node.Error,
	)
	if err != nil {
		return fmt.Errorf("exec failed: %w", err)
	}

	return nil
}

func (db *DB) UpdateNotEthNode(node common.NodeJSON) error {
	var err error

	start := time.Now()
	defer metrics.ObserveDBQuery("update_crawled_node_not_eth", time.Since(start), err)

	_, err = db.ExecRetryBusy(
		`
			INSERT INTO discovered_nodes (
				id,
				node,
				ip_address,
				first_found,
				last_found,
				next_crawl
			) VALUES (
				?1,
				?2,
				?3,
				CURRENT_TIMESTAMP,
				CURRENT_TIMESTAMP,
				datetime(CURRENT_TIMESTAMP, '+14 days')
			)
			ON CONFLICT (id) DO UPDATE
			SET
				last_found = CURRENT_TIMESTAMP,
				next_crawl = excluded.next_crawl
		`,
		node.ID(),
		node.N.String(),
		node.N.IP().String(),
	)
	if err != nil {
		return fmt.Errorf("exec failed: %w", err)
	}

	return nil
}

func (db *DB) UpdateCrawledNodeSuccess(node common.NodeJSON) error {
	var err error

	start := time.Now()
	defer metrics.ObserveDBQuery("update_crawled_node_success", time.Since(start), err)

	info := node.GetInfo()
	ip := node.N.IP()

	location, err := db.IPToLocation(ip)
	if err != nil {
		return fmt.Errorf("geoip failed: %w", err)
	}

	_, err = db.ExecRetryBusy(
		`
			INSERT INTO crawled_nodes (
				id,
				updated_at,
				client_name,
				rlpx_version,
				capabilities,
				network_id,
				fork_id,
				next_fork_id,
				block_height,
				head_hash,
				ip,
				connection_type,
				country,
				city,
				latitude,
				longitude,
				sequence
			) VALUES (
				?1,					-- id
				CURRENT_TIMESTAMP,	-- last_seen
				nullif(?2, ''),		-- client_name
				nullif(?3, 0),		-- rlpx_version
				nullif(?4, ''),		-- capabilities
				nullif(?5, 0),		-- network_id
				nullif(?6, ''),		-- fork_id
				nullif(?7, 0),		-- next_fork_id
				nullif(?8, ''),		-- block_height
				nullif(?9, ''),		-- head_hash
				nullif(?10, ''),	-- ip
				nullif(?11, ''),	-- connection_type
				nullif(?12, ''),	-- country
				nullif(?13, ''),	-- city
				nullif(?14, 0),		-- latitude
				nullif(?15, 0),		-- longitude
				nullif(?16, 0)		-- sequence
			)
			ON CONFLICT (id) DO UPDATE
			SET
				updated_at = CURRENT_TIMESTAMP,
				client_name = coalesce(excluded.client_name, client_name),
				rlpx_version = coalesce(excluded.rlpx_version, rlpx_version),
				capabilities = coalesce(excluded.capabilities, capabilities),
				network_id = coalesce(excluded.network_id, network_id),
				fork_id = coalesce(excluded.fork_id, fork_id),
				next_fork_id = excluded.next_fork_id,			-- Not coalesce because no next fork is valid
				block_height = coalesce(excluded.block_height, block_height),
				head_hash = coalesce(excluded.head_hash, head_hash),
				ip = coalesce(excluded.ip, ip),
				connection_type = coalesce(excluded.connection_type, connection_type),
				country = coalesce(excluded.country, country),
				city = coalesce(excluded.city, city),
				latitude = coalesce(excluded.latitude, latitude),
				longitude = coalesce(excluded.longitude, longitude),
				sequence = coalesce(excluded.sequence, sequence)
			RETURNING
				id;

			INSERT INTO discovered_nodes (
				id,
				node,
				ip_address,
				first_found,
				last_found,
				next_crawl
			) VALUES (
				?1,
				?17,
				?10,
				CURRENT_TIMESTAMP,
				CURRENT_TIMESTAMP,
				datetime(CURRENT_TIMESTAMP, '+12 hours')
			)
			ON CONFLICT (id) DO UPDATE
			SET
				last_found = CURRENT_TIMESTAMP,
				next_crawl = excluded.next_crawl;

			INSERT INTO crawl_history (
				id,
				crawled_at,
				direction,
				error
			) VALUES (
				?1,
				CURRENT_TIMESTAMP,
				?18,
				NULL
			);
		`,
		node.ID(),
		info.ClientName,
		info.RLPxVersion,
		node.CapsString(),
		info.NetworkID,
		hex.EncodeToString(info.ForkID.Hash[:]),
		info.ForkID.Next,
		info.Blockheight,
		info.HeadHash.String(),
		node.N.IP().String(),
		node.ConnectionType(),
		location.country,
		location.city,
		location.latitude,
		location.longitude,
		node.N.Seq(),
		node.N.String(),
		node.Direction,
	)
	if err != nil {
		return fmt.Errorf("exec failed: %w", err)
	}

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
