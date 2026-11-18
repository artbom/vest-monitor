-- Runs once on an empty primary volume; the replica gets everything through streaming replication.
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Relational data with personal data. Each pet carries its own alert thresholds (its baseline).
CREATE TABLE owners (id serial PRIMARY KEY, name text NOT NULL, email text NOT NULL);
CREATE TABLE vets   (id serial PRIMARY KEY, name text NOT NULL, email text NOT NULL);
CREATE TABLE pets (
  id         serial PRIMARY KEY,
  owner_id   int  NOT NULL REFERENCES owners(id) ON DELETE CASCADE,
  vet_id     int  REFERENCES vets(id),
  name       text NOT NULL,
  species    text NOT NULL,
  hr_max     smallint NOT NULL DEFAULT 160,   -- tachycardia: heart rate at or above this for 10 s
  temp_max   real     NOT NULL DEFAULT 40,    -- fever: temperature at or above this for 30 s
  updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE vests (
  device_id  text PRIMARY KEY,
  pet_id     int REFERENCES pets(id) ON DELETE SET NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

-- Telemetry: one row per vest per second; only device_id links it to a person.
CREATE TABLE metrics (
  device_id   text        NOT NULL,
  ts          timestamptz NOT NULL,
  heart_rate  smallint,
  resp_rate   smallint,
  temperature real,
  activity    smallint,
  steps       int,
  battery     smallint,
  PRIMARY KEY (device_id, ts)          -- idempotent writes: a packet read twice from Kafka adds nothing
);
-- Hourly chunks: the active chunk's index (~10 000 vests x 3 600 s) stays in memory, so inserts do not hit the disk.
SELECT create_hypertable('metrics', by_range('ts', INTERVAL '1 hour'));

-- Chunks older than 7 days become columnar and compressed. Retention follows the requirements:
-- raw seconds 1 year, hourly aggregates 5 years, daily aggregates 10 years.
ALTER TABLE metrics SET (timescaledb.compress, timescaledb.compress_segmentby = 'device_id', timescaledb.compress_orderby = 'ts');
SELECT add_compression_policy('metrics', INTERVAL '7 days');
SELECT add_retention_policy('metrics', INTERVAL '1 year');

-- Continuous aggregates: the database keeps them up to date itself.
CREATE MATERIALIZED VIEW metrics_1m WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
SELECT device_id, time_bucket('1 minute', ts) AS bucket,
  avg(heart_rate)::real AS heart_rate_avg,   min(heart_rate) AS heart_rate_min,   max(heart_rate) AS heart_rate_max,
  avg(resp_rate)::real AS resp_rate_avg,     min(resp_rate) AS resp_rate_min,     max(resp_rate) AS resp_rate_max,
  avg(temperature)::real AS temperature_avg, min(temperature) AS temperature_min, max(temperature) AS temperature_max,
  avg(activity)::real AS activity_avg,       min(activity) AS activity_min,       max(activity) AS activity_max,
  avg(steps)::real AS steps_avg,             min(steps) AS steps_min,             max(steps) AS steps_max,
  avg(battery)::real AS battery_avg,         min(battery) AS battery_min,         max(battery) AS battery_max
FROM metrics GROUP BY device_id, bucket WITH NO DATA;
SELECT add_continuous_aggregate_policy('metrics_1m', start_offset => INTERVAL '1 day', end_offset => INTERVAL '1 minute', schedule_interval => INTERVAL '1 minute');
SELECT add_retention_policy('metrics_1m', INTERVAL '1 year');

CREATE MATERIALIZED VIEW metrics_1h WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
SELECT device_id, time_bucket('1 hour', bucket) AS bucket,
  avg(heart_rate_avg)::real AS heart_rate_avg,   min(heart_rate_min) AS heart_rate_min,   max(heart_rate_max) AS heart_rate_max,
  avg(resp_rate_avg)::real AS resp_rate_avg,     min(resp_rate_min) AS resp_rate_min,     max(resp_rate_max) AS resp_rate_max,
  avg(temperature_avg)::real AS temperature_avg, min(temperature_min) AS temperature_min, max(temperature_max) AS temperature_max,
  avg(activity_avg)::real AS activity_avg,       min(activity_min) AS activity_min,       max(activity_max) AS activity_max,
  avg(steps_avg)::real AS steps_avg,             min(steps_min) AS steps_min,             max(steps_max) AS steps_max,
  avg(battery_avg)::real AS battery_avg,         min(battery_min) AS battery_min,         max(battery_max) AS battery_max
FROM metrics_1m GROUP BY device_id, time_bucket('1 hour', bucket) WITH NO DATA;
SELECT add_continuous_aggregate_policy('metrics_1h', start_offset => INTERVAL '3 days', end_offset => INTERVAL '1 hour', schedule_interval => INTERVAL '15 minutes');
SELECT add_retention_policy('metrics_1h', INTERVAL '5 years');

CREATE MATERIALIZED VIEW metrics_1d WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
SELECT device_id, time_bucket('1 day', bucket) AS bucket,
  avg(heart_rate_avg)::real AS heart_rate_avg,   min(heart_rate_min) AS heart_rate_min,   max(heart_rate_max) AS heart_rate_max,
  avg(resp_rate_avg)::real AS resp_rate_avg,     min(resp_rate_min) AS resp_rate_min,     max(resp_rate_max) AS resp_rate_max,
  avg(temperature_avg)::real AS temperature_avg, min(temperature_min) AS temperature_min, max(temperature_max) AS temperature_max,
  avg(activity_avg)::real AS activity_avg,       min(activity_min) AS activity_min,       max(activity_max) AS activity_max,
  avg(steps_avg)::real AS steps_avg,             min(steps_min) AS steps_min,             max(steps_max) AS steps_max,
  avg(battery_avg)::real AS battery_avg,         min(battery_min) AS battery_min,         max(battery_max) AS battery_max
FROM metrics_1h GROUP BY device_id, time_bucket('1 day', bucket) WITH NO DATA;
SELECT add_continuous_aggregate_policy('metrics_1d', start_offset => INTERVAL '7 days', end_offset => INTERVAL '1 day', schedule_interval => INTERVAL '1 hour');
SELECT add_retention_policy('metrics_1d', INTERVAL '10 years');

CREATE TABLE alerts (
  id         bigserial PRIMARY KEY,
  device_id  text        NOT NULL,
  ts         timestamptz NOT NULL,
  type       text        NOT NULL,
  value      real,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ON alerts (device_id, ts DESC);
CREATE UNIQUE INDEX ON alerts (device_id, type, ts);   -- a retried alert is not stored twice

-- One role per service. PACELC per data type, set where it is enforced:
-- writer/detection commit locally (PA/EL), api waits until the replica has applied the change (PC/EC).
CREATE ROLE writer    LOGIN PASSWORD 'writer';
CREATE ROLE detection LOGIN PASSWORD 'detection';
CREATE ROLE api       LOGIN PASSWORD 'api';
CREATE ROLE reader    LOGIN PASSWORD 'reader';
ALTER ROLE writer    SET synchronous_commit = 'local';
ALTER ROLE detection SET synchronous_commit = 'local';
ALTER ROLE api       SET synchronous_commit = 'remote_apply';
GRANT pg_monitor TO api;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO writer, detection, api, reader;
GRANT INSERT ON metrics, vests TO writer;
GRANT INSERT ON alerts TO detection;
GRANT USAGE ON SEQUENCE alerts_id_seq TO detection;
GRANT INSERT, UPDATE, DELETE ON owners, vets, pets, vests TO api;

-- Demo reference data: 10 owners, 3 vets, 50 pets wearing VST-0001 .. VST-0050 (the simulator's ids).
INSERT INTO owners (name, email) SELECT 'Owner ' || i, 'owner' || i || '@example.test' FROM generate_series(1, 10) i;
INSERT INTO vets (name, email) SELECT 'Vet ' || i, 'vet' || i || '@example.test' FROM generate_series(1, 3) i;
INSERT INTO pets (owner_id, vet_id, name, species, hr_max, temp_max)
  SELECT (i - 1) % 10 + 1, (i - 1) % 3 + 1,
         (ARRAY['Rex', 'Luna', 'Bella', 'Max', 'Milo', 'Daisy', 'Charlie', 'Lucy', 'Rocky', 'Nala'])[(i - 1) % 10 + 1] || ' ' || i,
         CASE WHEN i % 4 = 0 THEN 'cat' ELSE 'dog' END,
         CASE WHEN i % 4 = 0 THEN 220 ELSE 160 END, 40
  FROM generate_series(1, 50) i;
INSERT INTO vests (device_id, pet_id) SELECT 'VST-' || lpad(i::text, 4, '0'), i FROM generate_series(1, 50) i;
