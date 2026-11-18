-- Transactions homework: vest bindings, optimistic lock on pets, GDPR deletion requests.
-- Idempotent: runs as an init script on an empty volume and as a migration on an existing database
-- (api and saga-worker apply it at startup under an advisory lock).

-- Optimistic lock: an update must name the version it read; 0 rows updated = somebody changed the profile first.
ALTER TABLE pets ADD COLUMN IF NOT EXISTS version int NOT NULL DEFAULT 1;

-- Which pet wore which vest and when. Needed to know whose telemetry is whose (GDPR deletes by periods).
CREATE TABLE IF NOT EXISTS vest_bindings (
  id         bigserial PRIMARY KEY,
  device_id  text NOT NULL REFERENCES vests(device_id) ON DELETE CASCADE,
  pet_id     int  REFERENCES pets(id) ON DELETE SET NULL,
  bound_at   timestamptz NOT NULL DEFAULT now(),
  unbound_at timestamptz
);
-- At most one active binding per vest: a second concurrent "bind" fails instead of creating two owners.
CREATE UNIQUE INDEX IF NOT EXISTS vest_bindings_active ON vest_bindings (device_id) WHERE unbound_at IS NULL;
CREATE INDEX IF NOT EXISTS vest_bindings_pet ON vest_bindings (pet_id);
INSERT INTO vest_bindings (device_id, pet_id, bound_at)
  SELECT v.device_id, v.pet_id, v.created_at FROM vests v
   WHERE v.pet_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM vest_bindings b WHERE b.device_id = v.device_id AND b.unbound_at IS NULL);

-- One row per GDPR deletion saga run: the orchestrator's own view of progress (Temporal keeps the full history).
CREATE TABLE IF NOT EXISTS deletion_requests (
  id          bigserial PRIMARY KEY,
  owner_id    int  NOT NULL,                   -- no FK: the owner is gone at the end
  workflow_id text NOT NULL,
  run_id      text NOT NULL UNIQUE,            -- a retried "mark" step does not create a second request
  status      text NOT NULL DEFAULT 'running', -- running | done
  step        text NOT NULL DEFAULT 'mark',
  devices     text[],                          -- the owner's vests, for the UI
  periods     jsonb,                           -- [{device_id, from, to}]: whose telemetry to delete
  created_at  timestamptz NOT NULL DEFAULT now(),
  updated_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS deletion_requests_owner ON deletion_requests (owner_id, created_at DESC);

-- The saga worker's role. Relational part waits for the replica like the api (PC); a deletion is confirmed only
-- when the replica has it too.
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'saga') THEN CREATE ROLE saga LOGIN PASSWORD 'saga'; END IF;
END $$;
ALTER ROLE saga SET synchronous_commit = 'remote_apply';
GRANT SELECT ON ALL TABLES IN SCHEMA public TO saga, writer, detection, api, reader;
GRANT SELECT, INSERT, UPDATE ON deletion_requests TO saga;
GRANT USAGE ON SEQUENCE deletion_requests_id_seq TO saga;
GRANT DELETE ON owners, pets, metrics, alerts TO saga;
GRANT UPDATE ON vests, vest_bindings TO saga;
-- The api binds vests (reset of the demo restores deleted owners) and changes profiles.
GRANT INSERT, UPDATE ON vest_bindings TO api;
GRANT USAGE ON SEQUENCE vest_bindings_id_seq, owners_id_seq, pets_id_seq TO api;
-- Continuous aggregates keep materialized rows after the raw rows are gone: the saga deletes them per vest.
DO $$ DECLARE r record; BEGIN
  FOR r IN SELECT materialization_hypertable_schema AS s, materialization_hypertable_name AS t FROM timescaledb_information.continuous_aggregates LOOP
    EXECUTE format('GRANT USAGE ON SCHEMA %I TO saga', r.s);
    EXECUTE format('GRANT SELECT, DELETE ON %I.%I TO saga', r.s, r.t);
  END LOOP;
END $$;
