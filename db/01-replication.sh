#!/bin/sh
# Primary only: a role and a slot for the streaming replica, and a pg_hba line that lets it connect.
set -e
psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" <<'SQL'
CREATE ROLE replicator WITH REPLICATION LOGIN PASSWORD 'replicator';
SELECT pg_create_physical_replication_slot('replica1');
SQL
echo "host replication replicator all scram-sha-256" >> "$PGDATA/pg_hba.conf"
