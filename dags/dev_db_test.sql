CREATE OR REPLACE TRANSIENT TABLE dev_test (name VARCHAR(250), id INT, load_utc_ts datetime);

INSERT INTO dev_test VALUES ('name', 5, sysdate());
