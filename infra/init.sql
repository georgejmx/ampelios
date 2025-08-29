CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS events (
	_id SERIAL PRIMARY KEY,
	timestamp BIGINT NOT NULL,
	visitorid INT NOT NULL,
	event Varchar(20) NOT NULL,
	itemid INT NOT NULL,
	transactionid INT,
	sessionnumber INT,
	processed BOOLEAN
);

CREATE TABLE IF NOT EXISTS cluster (
	site_id INT NOT NULL,
	cluster_id INT NOT NULL,
	centroid vector(6),
	PRIMARY KEY (site_id, cluster_id)
);

CREATE TABLE IF NOT EXISTS user_journey (
	_id SERIAL PRIMARY KEY,
	user_id INT NOT NULL UNIQUE,
	site_id INT NOT NULL,
	state vector(3) NOT NULL,
	paths vector[],
	cluster_id INT,
	FOREIGN KEY (site_id, cluster_id) REFERENCES cluster(site_id, cluster_id)
);

-- speed up acessing the first unprocessed event per session
CREATE INDEX idx_events_unprocessed_sessiontime
    ON events (visitorid, sessionnumber, timestamp)
    WHERE processed IS NOT TRUE;
