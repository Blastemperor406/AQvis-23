CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : '1' };

USE test;

CREATE TABLE IF NOT EXISTS test_aqvis (
node text PRIMARY KEY,
pm1 int,
pm2_5 int,
pm10 int,
last_update_timestamp timestamp
);