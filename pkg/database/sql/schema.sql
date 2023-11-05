CREATE TABLE IF NOT EXISTS discovered_nodes (
	node_id			BLOB	PRIMARY KEY,
	network_address	TEXT	NOT NULL,
	ip_address		TEXT	NOT NULL,
	first_found		INTEGER	NOT NULL,
	last_found		INTEGER	NOT NULL,
	next_crawl		INTEGER	NOT NULL
) STRICT;

CREATE INDEX IF NOT EXISTS discovered_nodes_node_id_next_crawl
	ON discovered_nodes (node_id, next_crawl);
CREATE INDEX IF NOT EXISTS discovered_nodes_next_crawl_network_address
	ON discovered_nodes (next_crawl, network_address);

CREATE TABLE IF NOT EXISTS crawled_nodes (
	node_id			BLOB	PRIMARY KEY,
	updated_at		INTEGER	NOT NULL,
	client_name		TEXT	DEFAULT NULL,
	rlpx_version	INTEGER	DEFAULT NULL,
	capabilities	TEXT	DEFAULT NULL,
	network_id		INTEGER	DEFAULT NULL,
	fork_id			INTEGER	DEFAULT NULL,
	next_fork_id	INTEGER	DEFAULT NULL,
	head_hash		BLOB	DEFAULT NULL,
	ip_address		TEXT	DEFAULT NULL,
	connection_type	TEXT	DEFAULT NULL,
	country			TEXT	DEFAULT NULL,
	city			TEXT	DEFAULT NULL,
	latitude		REAL	DEFAULT NULL,
	longitude		REAL	DEFAULT NULL
) STRICT;

CREATE INDEX IF NOT EXISTS crawled_nodes_node_id_last_seen
	ON crawled_nodes (node_id, updated_at);
CREATE INDEX IF NOT EXISTS crawled_nodes_network_id
	ON crawled_nodes (network_id);
CREATE INDEX IF NOT EXISTS crawled_nodes_hex_node_id
	ON crawled_nodes (hex(node_id));
CREATE INDEX IF NOT EXISTS crawled_nodes_ip_address
	ON crawled_nodes (ip_address);

CREATE TABLE IF NOT EXISTS crawl_history (
	node_id		BLOB	NOT NULL,
	crawled_at	INTEGER	NOT NULL,
	direction	TEXT	NOT NULL,
	error		TEXT	DEFAULT NULL,

	PRIMARY KEY (node_id, crawled_at)
) STRICT;

CREATE INDEX IF NOT EXISTS crawl_history_crawled_at
	ON crawl_history (crawled_at);

CREATE TABLE IF NOT EXISTS blocks (
	block_hash		BLOB	NOT NULL,
	network_id		INTEGER NOT NULL,
	timestamp		INTEGER	NOT NULL,
	block_number	INTEGER	NOT NULL,
	parent_hash		BLOB	DEFAULT NULL,

	PRIMARY KEY (block_hash, network_id)
) STRICT;

CREATE INDEX IF NOT EXISTS blocks_block_hash_timestamp
	ON blocks (block_hash, timestamp);

