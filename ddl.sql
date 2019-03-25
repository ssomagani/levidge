DROP PROCEDURE select_datagram IF EXISTS;
DROP PROCEDURE insert_from_dummy_data IF EXISTS;
DROP PROCEDURE select_timestamp IF EXISTS;
DROP PROCEDURE insert_to_dummy_data IF EXISTS;
DROP TABLE v_dummy_data_last_n_export_table IF EXISTS;
DROP TABLE dummy_data_export_table IF EXISTS;
DROP TABLE dummy_data IF EXISTS;

file -inlinebatch END_OF_BATCH

CREATE TABLE dummy_data (
  start_time BIGINT NOT NULL,
  id BIGINT NOT NULL,
  val VARCHAR(1000) NOT NULL,
  created BIGINT NOT NULL,
);
PARTITION TABLE dummy_data ON COLUMN id;
CREATE INDEX dummy_data_idx1 ON dummy_data (id);

CREATE PROCEDURE select_datagram PARTITION ON TABLE dummy_data COLUMN id as
SELECT * FROM dummy_data limit ? offset ?;

CREATE PROCEDURE select_timestamp PARTITION ON TABLE dummy_data COLUMN id as
SELECT id, start_time, created FROM dummy_data WHERE id = ?;

CREATE PROCEDURE insert_to_dummy_data PARTITION ON TABLE dummy_data COLUMN id PARAMETER 1 as
INSERT INTO dummy_data (start_time, id, val, created) VALUES (?, ?, ?, SINCE_EPOCH(MILLISECOND, NOW));

CREATE PROCEDURE insert_from_dummy_data as
INSERT INTO dummy_data_export_table SELECT * FROM v_dummy_data_last_n_export_table WHERE id > ? AND id < ? ;

CREATE STREAM dummy_data_export_table PARTITION ON COLUMN id
  EXPORT TO TARGET kafka_export_topic (
        id BIGINT NOT NULL,
        val VARCHAR(1000)
);

CREATE VIEW v_dummy_data_last_n_export_table (
	id,
	val

) AS
SELECT id, val FROM dummy_data GROUP BY id, val;

END_OF_BATCH
