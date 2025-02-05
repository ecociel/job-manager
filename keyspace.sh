#!/usr/bin/env bash

until printf "" 2>>/dev/null >>/dev/tcp/cassandra/9042; do
    sleep 5;
    echo "Waiting for cassandra...";
done

echo "Creating keyspace.."

cqlsh cassandra -u cassandra -p cassandra -e "
CREATE KEYSPACE IF NOT EXISTS job
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
"
echo "Keyspace created successfully!"


cqlsh cassandra -u cassandra -p cassandra -e "
CREATE TABLE IF NOT EXISTS job.jobs (
              name text PRIMARY KEY,
    					Owner TEXT,
              lock_ttl int,
					    lock_status TEXT,
					    lock_timestamp TIMESTAMP,
 					    check_interval bigint,
              last_run timestamp,
 					    state blob,
              retry_attempts int,
              max_retries int,
              backoff_duration bigint,
              status text,
					    schedule text,
)"
echo "jobs table created successfully!"