CREATE TABLE IF NOT EXISTS stats.crawled_nodes (
	timestamp		INTEGER	NOT NULL,
	client_name		TEXT	DEFAULT NULL,
	client_version	TEXT	DEFAULT NULL,
	client_os		TEXT	DEFAULT NULL,
	client_arch		TEXT	DEFAULT NULL,
	network_id		INTEGER	DEFAULT NULL,
	fork_id			INTEGER	DEFAULT NULL,
	next_fork_id	INTEGER	DEFAULT NULL,
	country			TEXT	DEFAULT NULL,
	synced			INTEGER	NOT NULL,
	dial_success	INTEGER NOT NULL,
	total			INTEGER NOT NULL
) STRICT;

CREATE INDEX IF NOT EXISTS stats.crawled_nodes_timestamp
	ON crawled_nodes (timestamp);
