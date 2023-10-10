package database

import (
	"encoding/hex"
	"fmt"
	"net"

	"github.com/ethereum/node-crawler/pkg/common"
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
	_, err := db.ExecRetryBusy(
		0,
		`
			UPDATE discovered_nodes
			SET
				next_crawl = datetime(CURRENT_TIMESTAMP, '+6 hours')
			WHERE
				id = ?1;

			INSERT INTO crawl_history (
				id,
				crawled_at,
				error
			) VALUES (
				?1,
				CURRENT_TIMESTAMP,
				?2
			);
		`,
		node.ID(),
		node.Error,
	)
	if err != nil {
		return fmt.Errorf("exec failed: %w", err)
	}

	return nil
}

func (db *DB) UpdateNotEthNode(node common.NodeJSON) error {
	_, err := db.ExecRetryBusy(
		0,
		`
			UPDATE discovered_nodes
			SET
				next_crawl = datetime(CURRENT_TIMESTAMP, '+14 days')
			WHERE
				id = ?1;
		`,
		node.ID(),
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

	info := node.GetInfo()
	ip := node.N.IP()

	location, err := db.IPToLocation(ip)
	if err != nil {
		return fmt.Errorf("geoip failed: %w", err)
	}

	_, err = db.ExecRetryBusy(
		0,
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

			UPDATE discovered_nodes
			SET
				next_crawl = datetime(CURRENT_TIMESTAMP, '+1 hour')
			WHERE
				id = ?1;

			INSERT INTO crawl_history (
				id,
				crawled_at,
				error
			) VALUES (
				?1,
				CURRENT_TIMESTAMP,
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
	)
	if err != nil {
		return fmt.Errorf("exec failed: %w", err)
	}

	return nil
}
