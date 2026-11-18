#!/bin/sh
# Replica: on an empty volume copy the primary with pg_basebackup, then start as a hot standby.
# application_name=replica1 is what synchronous_standby_names on the primary waits for.
set -e
if [ ! -s "$PGDATA/PG_VERSION" ]; then
  mkdir -p "$PGDATA" && chown postgres:postgres "$PGDATA" && chmod 700 "$PGDATA"
  gosu postgres env PGPASSWORD=replicator pg_basebackup -h pg-primary -U replicator -D "$PGDATA" -X stream -S replica1 -R
  cat >> "$PGDATA/postgresql.auto.conf" <<'CONF'
primary_conninfo = 'host=pg-primary port=5432 user=replicator password=replicator application_name=replica1'
primary_slot_name = 'replica1'
hot_standby_feedback = on
CONF
fi
exec docker-entrypoint.sh postgres -c shared_preload_libraries=timescaledb -c hot_standby=on
