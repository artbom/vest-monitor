// HTTP API for the owner app, the vet cabinet and the demo dashboard (served at /).
// Two database pools, two PACELC choices:
//   ro: the replica, role "reader": charts and lists; stale by a few ms at most, falls back to the primary if the replica is down
//   rw: the primary, role "api" (synchronous_commit = remote_apply): a profile change is confirmed only once the replica
//       has applied it. Replica down -> the write is not confirmed and we answer 503 (PC/EC).
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import type { ServerResponse } from 'node:http';
import Fastify from 'fastify';
import mqtt from 'mqtt';
import { Redis } from 'ioredis';
import pg from 'pg';
import { Client as TemporalClient, Connection, WorkflowExecutionAlreadyStartedError } from '@temporalio/client';
import { channels, config, decodePacket, env, keys, log, METRICS, SCENARIOS, type Scenario, fastRedis, sleep } from './shared.ts';
import { kafka, ensureTopic } from './kafka.ts';
import { migrate } from './migrate.ts';

await migrate('api');

// Profile writes: no query_timeout, so the UPDATE keeps waiting for the replica after we answer 503 (see PUT /profile).
const rwWrite = new pg.Pool({ connectionString: config.db('api'), max: 5, connectionTimeoutMillis: 2000 });
rwWrite.on('error', () => {});
const rw = new pg.Pool({ connectionString: config.db('api'), max: 5, query_timeout: 4000, connectionTimeoutMillis: 2000 });
const ro = new pg.Pool({ connectionString: config.db('reader', true), max: 5, query_timeout: 4000, connectionTimeoutMillis: 1000 });
rw.on('error', () => {}); ro.on('error', () => {});
const redis = new Redis(config.redisUrl, fastRedis);
const app = Fastify({ logger: { level: 'warn' } });
const NAMES = new Set<string>(METRICS.map(m => m.name));
const UI = path.join(import.meta.dirname, '../public/index.html');
const CHAOS = env('CHAOS_URL', 'http://chaos:3001');

let replicaReads = 0, primaryFallbacks = 0;
// After a failed replica read, go straight to the primary for 5 s instead of waiting for a connect timeout on every read.
let replicaDownUntil = 0;
async function read<T extends pg.QueryResultRow>(sql: string, params: unknown[] = []): Promise<pg.QueryResult<T>> {
  if (Date.now() > replicaDownUntil) {
    try { const r = await ro.query<T>(sql, params); replicaReads++; return r; }
    catch { replicaDownUntil = Date.now() + 5000; }
  }
  primaryFallbacks++;
  return rw.query<T>(sql, params);
}
const withTimeout = <T>(p: Promise<T>, ms = 1500): Promise<T> =>
  Promise.race([p, new Promise<T>((_, rej) => setTimeout(() => rej(new Error('timeout')), ms))]);

app.get('/', async (_req, reply) => reply.type('text/html; charset=utf-8').send(fs.readFileSync(UI)));
app.get('/health', async () => ({ ok: true }));

// ---- Latest readings, from the live Kafka feed (no database round trip) ----
const latest = new Map<string, { ts: number; heart_rate?: number; temperature?: number; battery?: number }>();

app.get('/vests', async () => {
  const { rows } = await read<{ device_id: string; pet: string | null; species: string | null; owner: string | null; vet: string | null }>(
    `SELECT v.device_id, p.name AS pet, p.species, o.name AS owner, vt.name AS vet FROM vests v
       LEFT JOIN pets p ON p.id = v.pet_id LEFT JOIN owners o ON o.id = p.owner_id LEFT JOIN vets vt ON vt.id = p.vet_id
      ORDER BY v.device_id LIMIT 300`);
  const scenarios = await redis.hgetall(keys.scenarios);
  return rows.map(r => ({ ...r, scenario: scenarios[r.device_id] ?? 'normal', latest: latest.get(r.device_id) ?? null }));
});

// Chart data. Resolution follows the range so a response stays around a thousand points.
const readLatency: number[] = [];
app.get<{ Params: { id: string }; Querystring: { metric?: string; from?: string; to?: string } }>('/vests/:id/metrics', async (req, reply) => {
  const started = performance.now();
  const metric = req.query.metric ?? 'heart_rate';
  if (!NAMES.has(metric)) return reply.code(400).send({ error: `metric must be one of ${[...NAMES].join(', ')}` });
  const to = req.query.to ? new Date(req.query.to) : new Date();
  const from = req.query.from ? new Date(req.query.from) : new Date(+to - 3600_000);
  if (isNaN(+from) || isNaN(+to) || from >= to) return reply.code(400).send({ error: 'from/to must be ISO times, from < to' });
  const hours = (+to - +from) / 3600_000;
  const [table, step, t] = hours <= 2 ? ['metrics', 1, 'ts'] : hours <= 72 ? ['metrics_1m', 60, 'bucket']
    : hours <= 24 * 60 ? ['metrics_1h', 3600, 'bucket'] : ['metrics_1d', 86400, 'bucket'];
  const select = table === 'metrics' ? `${metric} AS avg, ${metric} AS min, ${metric} AS max` : `${metric}_avg AS avg, ${metric}_min AS min, ${metric}_max AS max`;
  const { rows } = await read(`SELECT ${t} AS t, ${select} FROM ${table} WHERE device_id = $1 AND ${t} >= $2 AND ${t} < $3 ORDER BY ${t}`, [req.params.id, from, to]);
  readLatency.push(performance.now() - started);
  // Column arrays instead of [{t, value}]: field names are not repeated for every point.
  return { device_id: req.params.id, metric, from, to, step_s: step, source: table,
    t: rows.map(r => r.t), avg: rows.map(r => r.avg), min: rows.map(r => r.min), max: rows.map(r => r.max) };
});

// ---- Pet profile: the relational, PC/EC part ----
const PROFILE_SQL = `SELECT v.device_id, p.id AS pet_id, p.name AS pet, p.species, p.hr_max, p.temp_max, p.updated_at, p.version, o.id AS owner_id, o.name AS owner, vt.name AS vet
  FROM vests v LEFT JOIN pets p ON p.id = v.pet_id LEFT JOIN owners o ON o.id = p.owner_id LEFT JOIN vets vt ON vt.id = p.vet_id WHERE v.device_id = $1`;
app.get<{ Params: { id: string } }>('/vests/:id/profile', async (req, reply) => {
  const { rows: [p] } = await read(PROFILE_SQL, [req.params.id]);
  const cached = await redis.get(keys.profile(req.params.id)).catch(() => undefined);
  const ttl = await redis.ttl(keys.profile(req.params.id)).catch(() => null);
  return p ? { ...p, cache: cached ? JSON.parse(cached) : cached === undefined ? 'redis-down' : null, cache_ttl_s: ttl } : reply.code(404).send({ error: 'unknown vest' });
});

app.put<{ Params: { id: string }; Body: { hr_max?: number; temp_max?: number; version?: number } }>('/vests/:id/profile', async (req, reply) => {
  const { hr_max, temp_max, version } = req.body ?? {};
  if (version !== undefined && !Number.isInteger(version)) return reply.code(400).send({ error: 'version: integer' });
  if (hr_max !== undefined && !(Number.isInteger(hr_max) && hr_max >= 60 && hr_max <= 300)) return reply.code(400).send({ error: 'hr_max: 60..300' });
  if (temp_max !== undefined && !(temp_max >= 36 && temp_max <= 43)) return reply.code(400).send({ error: 'temp_max: 36..43' });
  const started = performance.now();
  const key = keys.profile(req.params.id);
  // The UPDATE commits on the primary and then waits until the replica applies it (remote_apply). Nobody sees the change
  // during that wait. If the replica is down we stop waiting after 4 s and answer 503 "not confirmed", but the
  // transaction keeps waiting and becomes visible once the replica is back, so the cache is cleared when it really
  // lands, twice: a detection reload that raced the first DEL cannot keep the old profile.
  // Optimistic lock: the owner and the vet can both edit the thresholds. The UPDATE only matches the version the client
  // read; if somebody saved in between, 0 rows change and the client gets 409 with the current profile.
  const write = rwWrite.query(
    `UPDATE pets SET hr_max = coalesce($2, hr_max), temp_max = coalesce($3, temp_max), updated_at = now(), version = version + 1
      WHERE id = (SELECT pet_id FROM vests WHERE device_id = $1) AND ($4::int IS NULL OR version = $4)`, [req.params.id, hr_max ?? null, temp_max ?? null, version ?? null])
    .then(r => { redis.del(key).catch(() => {}); setTimeout(() => redis.del(key).catch(() => {}), 1000); return r; });
  write.catch(() => {});
  const r = await Promise.race([write, sleep(4000).then(() => null)]).catch(e => e as Error);
  if (r === null) return reply.code(503).send({ error: 'синхронна репліка не підтвердила запис за 4 с. Зміна не підтверджена і нікому не видна; вона застосується, коли репліка повернеться (PC)',
    waited_ms: Math.round(performance.now() - started) });
  if (r instanceof Error) return reply.code(500).send({ error: r.message });
  if (!r.rowCount) {
    const { rows: [cur] } = await rw.query(PROFILE_SQL, [req.params.id]);
    if (!cur?.pet_id) return reply.code(404).send({ error: 'this vest is not linked to a pet' });
    return reply.code(409).send({ error: `профіль уже змінив хтось інший (версія ${cur.version}, а ви редагували ${version}). Перечитайте і збережіть ще раз`, current: cur });
  }
  const commitMs = Math.round(performance.now() - started);
  const { rows: [p] } = await read(PROFILE_SQL, [req.params.id]);   // read from the replica: remote_apply guarantees it is there
  return { ...p, commit_ms: commitMs };
});

app.get<{ Querystring: { device_id?: string } }>('/alerts', async req => (await read(
  `SELECT a.device_id, a.ts, a.type, a.value, a.created_at, p.name AS pet, o.name AS owner, vt.name AS vet FROM alerts a
     LEFT JOIN vests v ON v.device_id = a.device_id LEFT JOIN pets p ON p.id = v.pet_id
     LEFT JOIN owners o ON o.id = p.owner_id LEFT JOIN vets vt ON vt.id = p.vet_id
    WHERE ($1::text IS NULL OR a.device_id = $1) ORDER BY a.ts DESC LIMIT 50`, [req.query.device_id ?? null])).rows);

// ---- Simulators and scenarios ----
async function sims() {
  // A simulator's status key expires 5 s after it stops, so the list only has running simulators.
  const names = (await redis.keys(keys.simStatus('*')).catch(() => [] as string[])).map(k => k.split(':')[1]);
  const list = await Promise.all(names.map(async name => ({ name, config: await redis.hgetall(keys.simConfig(name)), status: await redis.hgetall(keys.simStatus(name)) })));
  return list.filter(s => Object.keys(s.status).length).sort((a, b) => a.name.localeCompare(b.name));
}
app.get('/sims', async () => ({ sims: await sims(), scenarios: SCENARIOS }));

app.post<{ Params: { name: string }; Body: { vests?: number; speed?: number; network?: 'up' | 'down' } }>('/sims/:name', async (req, reply) => {
  const { vests, speed, network } = req.body ?? {};
  if (vests !== undefined && !(Number.isInteger(vests) && vests >= 0 && vests <= 20000)) return reply.code(400).send({ error: 'vests: 0..20000' });
  if (speed !== undefined && !(speed >= 0.1 && speed <= 100)) return reply.code(400).send({ error: 'speed: 0.1..100' });
  if (network !== undefined && network !== 'up' && network !== 'down') return reply.code(400).send({ error: 'network: up | down' });
  await redis.hset(keys.simConfig(req.params.name), { ...(vests !== undefined && { vests }), ...(speed !== undefined && { speed }), ...(network && { network }) });
  await redis.publish(channels.simControl, req.params.name);
  return redis.hgetall(keys.simConfig(req.params.name));
});

app.post<{ Params: { id: string }; Body: { scenario: Scenario } }>('/sim/vests/:id/scenario', async (req, reply) => {
  const scenario = req.body?.scenario;
  if (!SCENARIOS.includes(scenario)) return reply.code(400).send({ error: `scenario must be one of ${SCENARIOS.join(', ')}` });
  if (scenario === 'normal') await redis.hdel(keys.scenarios, req.params.id);
  else await redis.hset(keys.scenarios, req.params.id, scenario);
  await redis.publish(channels.simControl, '*');
  return { device_id: req.params.id, scenario };
});

// ---- GDPR deletion: a saga orchestrated by Temporal, steps run by saga-worker ----
const temporal = new TemporalClient({ connection: Connection.lazy({ address: env('TEMPORAL_ADDRESS', 'localhost:17233') }) });
const TEMPORAL_UI = env('TEMPORAL_UI', 'http://localhost:18233');
const workflowId = (ownerId: number) => `gdpr-owner-${ownerId}`;
const uiUrl = (wid: string, rid?: string) => `${TEMPORAL_UI}/namespaces/default/workflows/${encodeURIComponent(wid)}${rid ? `/${rid}/history` : ''}`;

app.post<{ Params: { id: string } }>('/owners/:id/gdpr-delete', async (req, reply) => {
  const ownerId = Number(req.params.id);
  if (!Number.isInteger(ownerId)) return reply.code(400).send({ error: 'owner id: integer' });
  const { rowCount } = await rw.query('SELECT 1 FROM owners WHERE id = $1', [ownerId]);
  if (!rowCount) return reply.code(404).send({ error: 'немає такого власника (вже видалений?)' });
  try {
    // One running deletion per owner: the workflow id is the owner, a second start while it runs is refused.
    const h = await temporal.workflow.start('gdprDelete', { taskQueue: 'gdpr', workflowId: workflowId(ownerId), args: [ownerId],
      workflowIdConflictPolicy: 'FAIL', workflowIdReusePolicy: 'ALLOW_DUPLICATE' });
    return { workflow_id: h.workflowId, run_id: h.firstExecutionRunId, ui_url: uiUrl(h.workflowId, h.firstExecutionRunId) };
  } catch (e) {
    if (e instanceof WorkflowExecutionAlreadyStartedError) return reply.code(409).send({ error: 'видалення цього власника вже йде', workflow_id: workflowId(ownerId), ui_url: uiUrl(workflowId(ownerId)) });
    return reply.code(503).send({ error: `Temporal недоступний: ${(e as Error).message}` });
  }
});

const ACTIVITY_STATES = ['UNSPECIFIED', 'SCHEDULED', 'STARTED', 'CANCEL_REQUESTED', 'PAUSED', 'PAUSE_REQUESTED'];
// Progress: our deletion_requests row (current step) + Temporal's view (status, retries of the pending step).
app.get<{ Params: { id: string } }>('/owners/:id/gdpr-delete', async (req, reply) => {
  const ownerId = Number(req.params.id);
  let wf: Record<string, unknown> | null = null;
  try {
    const d = await withTimeout(temporal.workflow.getHandle(workflowId(ownerId)).describe(), 1500);
    const pending = d.raw.pendingActivities?.[0];
    wf = { run_id: d.runId, status: d.status.name, started: d.startTime, closed: d.closeTime ?? null,
      pending: pending ? { activity: pending.activityType?.name, attempt: pending.attempt, state: ACTIVITY_STATES[pending.state ?? 0] ?? String(pending.state),
        last_failure: pending.lastFailure?.message ?? null } : null };
  } catch (e) {
    wf = (e as Error).name === 'WorkflowNotFoundError' ? null : { error: (e as Error).message };
  }
  // The row of this very run: right after a start the "mark" step has not inserted it yet, and an older run's row
  // must not be shown as this one's progress.
  const { rows: [r] } = await rw.query(`SELECT workflow_id, run_id, status, step, devices, created_at, updated_at FROM deletion_requests
    WHERE owner_id = $1 AND ($2::text IS NULL OR run_id = $2) ORDER BY created_at DESC LIMIT 1`, [ownerId, (wf?.run_id as string | undefined) ?? null]);
  if (!r && !wf) return reply.code(404).send({ error: 'видалення не запускалось' });
  const runId = (wf?.run_id as string | undefined) ?? r?.run_id;
  return { request: r ?? null, workflow: wf, ui_url: uiUrl(workflowId(ownerId), runId) };
});

// Demo data back after a GDPR deletion: owners 1..10, a pet on every vest VST-0001..0050, active bindings.
async function restoreDemoData() {
  const c = await rw.connect();
  try {
    await c.query('BEGIN');
    await c.query(`INSERT INTO owners (id, name, email) SELECT i, 'Owner ' || i, 'owner' || i || '@example.test' FROM generate_series(1, 10) i ON CONFLICT (id) DO NOTHING`);
    const { rows } = await c.query<{ device_id: string }>(`
      WITH missing AS (SELECT i FROM generate_series(1, 50) i JOIN vests v ON v.device_id = 'VST-' || lpad(i::text, 4, '0') WHERE v.pet_id IS NULL),
      ins AS (INSERT INTO pets (owner_id, vet_id, name, species, hr_max, temp_max)
        SELECT (i - 1) % 10 + 1, (i - 1) % 3 + 1,
               (ARRAY['Rex', 'Luna', 'Bella', 'Max', 'Milo', 'Daisy', 'Charlie', 'Lucy', 'Rocky', 'Nala'])[(i - 1) % 10 + 1] || ' ' || i,
               CASE WHEN i % 4 = 0 THEN 'cat' ELSE 'dog' END, CASE WHEN i % 4 = 0 THEN 220 ELSE 160 END, 40
          FROM missing RETURNING id, split_part(name, ' ', 2)::int AS i),
      upd AS (UPDATE vests v SET pet_id = ins.id FROM ins WHERE v.device_id = 'VST-' || lpad(ins.i::text, 4, '0') RETURNING v.device_id, v.pet_id)
      INSERT INTO vest_bindings (device_id, pet_id) SELECT device_id, pet_id FROM upd
        ON CONFLICT (device_id) WHERE unbound_at IS NULL DO NOTHING RETURNING device_id`);
    await c.query('COMMIT');
    if (rows.length) await redis.del(...rows.map(r => keys.profile(r.device_id))).catch(() => {});
    return rows.length;
  } catch (e) {
    await c.query('ROLLBACK').catch(() => {});
    throw e;
  } finally {
    c.release();
  }
}

// "Everybody back to normal" for the demo.
app.post('/sim/reset', async () => {
  const restored = await restoreDemoData().catch(e => { log('api', 'restore failed', { error: (e as Error).message }); return 0; });
  await redis.del(keys.scenarios, keys.accepted, keys.stored);
  const pauses = await redis.keys(keys.alertPause('*', '*'));     // so the alert demo step works again right away
  if (pauses.length) await redis.del(...pauses);
  for (const s of await sims()) await redis.hset(keys.simConfig(s.name), { network: 'up' });
  await redis.publish(channels.simControl, '*');
  return { ok: true, restored_pets: restored };
});

// ---- Fault injection (proxied to the chaos service, which talks to Docker) ----
app.post<{ Params: { service: string; action: string } }>('/chaos/:service/:action', async (req, reply) => {
  const r = await fetch(`${CHAOS}/services/${req.params.service}/${req.params.action}`, { method: 'POST' }).catch(() => null);
  if (!r) return reply.code(502).send({ error: 'chaos service is not running' });
  return reply.code(r.status).send(await r.json());
});

// ---- Live feed: our own consumer group per api instance, so every instance sees every packet ----
const watchers = new Set<{ res: ServerResponse; vest?: string }>();
function broadcast(event: string, data: unknown, vest?: string) {
  const msg = `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;
  for (const c of watchers) if (!vest || c.vest === vest) c.res.write(msg);
}

await ensureTopic();
const liveConsumer = kafka.consumer({ groupId: `api-live-${os.hostname()}`, maxWaitTimeInMs: 100 });
await liveConsumer.connect();
await liveConsumer.subscribe({ topic: config.topic, fromBeginning: false });
await liveConsumer.run({
  eachMessage: async ({ message }) => {
    const p = decodePacket(message.value!);
    const last = p.rows.at(-1)!;
    latest.set(p.deviceId, { ts: +last.ts, heart_rate: last.heart_rate, temperature: last.temperature, battery: last.battery });
    if ([...watchers].some(c => c.vest === p.deviceId))
      broadcast('live', { device_id: p.deviceId, start: p.startUtc, metrics: METRICS.map(m => m.name), rows: p.rows.map(r => METRICS.map(m => r[m.name] ?? null)) }, p.deviceId);
  },
});

const sub = new Redis(config.redisUrl);
const stats = new Map<string, Record<string, number>>();
await sub.subscribe(channels.alerts, channels.stats);
sub.on('message', (channel: string, message: string) => {
  const data = JSON.parse(message);
  if (channel === channels.stats) stats.set(data.service, data);
  broadcast(channel, data);
});

// MQTT broker queue: messages it holds (for ingest's sessions, in flight to it), from mosquitto's $SYS topics.
let brokerQueue: number | null = null;
const sys = mqtt.connect(config.mqttUrl, { clientId: `api-sys-${os.hostname()}`, reconnectPeriod: 2000 });
sys.on('connect', () => sys.subscribe('$SYS/broker/store/messages/count'));
sys.on('message', (_t, m) => { brokerQueue = Number(m.toString()); });
sys.on('error', () => {});
sys.on('offline', () => { brokerQueue = null; });

app.get<{ Querystring: { vest?: string } }>('/live', (req, reply) => {
  reply.hijack();
  const res = reply.raw;
  res.writeHead(200, { 'content-type': 'text/event-stream', 'cache-control': 'no-cache', connection: 'keep-alive' });
  res.write(': connected\n\n');
  const client = { res, vest: req.query.vest };
  watchers.add(client);
  const ping = setInterval(() => res.write(': ping\n\n'), 15000);
  req.raw.on('close', () => { clearInterval(ping); watchers.delete(client); });
});

// ---- Ops snapshot every 2 s: Kafka cluster and lag, replication, SLIs, simulators, container states ----
let admin = kafka.admin();
let adminUp = false;
async function kafkaState() {
  if (!adminUp) { await withTimeout(admin.connect(), 3000); adminUp = true; }
  const [cluster, meta, high, writer, detection] = await withTimeout(Promise.all([
    admin.describeCluster(),
    admin.fetchTopicMetadata({ topics: [config.topic] }),
    admin.fetchTopicOffsets(config.topic),
    admin.fetchOffsets({ groupId: 'writer', topics: [config.topic] }),
    admin.fetchOffsets({ groupId: 'detection', topics: [config.topic] }),
  ]), 2500);
  const end = new Map(high.map(h => [h.partition, Number(h.high)]));
  const lag = (g: typeof writer) => new Map(g[0].partitions.map(p => [p.partition, Math.max(0, (end.get(p.partition) ?? 0) - Math.max(0, Number(p.offset)))]));
  const lw = lag(writer), ld = lag(detection);
  const partitions = meta.topics[0].partitions.sort((a, b) => a.partitionId - b.partitionId).map(p => ({
    id: p.partitionId, leader: p.leader, replicas: p.replicas, isr: p.isr, end: end.get(p.partitionId) ?? 0,
    lag_writer: lw.get(p.partitionId) ?? 0, lag_detection: ld.get(p.partitionId) ?? 0,
  }));
  return { brokers: cluster.brokers.map(b => b.nodeId).sort(), partitions };
}

async function pgState() {
  const primary = await withTimeout(rw.query<{ state: string; sync_state: string; replay_lag_ms: number | null }>(
    `SELECT state, sync_state, (extract(epoch FROM replay_lag) * 1000)::int AS replay_lag_ms FROM pg_stat_replication WHERE application_name = 'replica1'`), 2000)
    .then(r => ({ up: true, replica: r.rows[0] ?? null })).catch(() => ({ up: false, replica: null }));
  const replicaUp = await withTimeout(ro.query('SELECT 1'), 1500).then(() => true).catch(() => false);
  return { ...primary, replica_up: replicaUp };
}

let storage: Record<string, unknown> = {};
async function refreshStorage() {
  storage = (await read(`SELECT approximate_row_count('metrics') AS rows, hypertable_size('metrics') AS bytes,
    (SELECT count(*) FROM timescaledb_information.chunks WHERE hypertable_name = 'metrics') AS chunks,
    (SELECT count(*) FROM pets) AS pets, (SELECT count(*) FROM vests) AS vests, (SELECT count(*) FROM alerts) AS alerts`).catch(() => ({ rows: [{}] }))).rows[0];
}
setInterval(() => refreshStorage().catch(() => {}), 10_000);
refreshStorage().catch(() => {});

let ops: Record<string, unknown> = {};
async function refreshOps() {
  const [k, p, s, counts, services, resources] = await Promise.all([
    kafkaState().catch(e => { adminUp = false; admin.disconnect().catch(() => {}); admin = kafka.admin(); return { brokers: [], partitions: [], error: (e as Error).message }; }),
    pgState(),
    sims(),
    redis.mget(keys.accepted, keys.stored).catch(() => [null, null]),
    withTimeout(fetch(`${CHAOS}/services`).then(r => r.json()), 1500).catch(() => null),
    // Limits and live usage of every container of the project, cached by the chaos service.
    withTimeout(fetch(`${CHAOS}/resources`).then(r => r.json()), 1500).catch(() => null),
  ]);
  const sorted = readLatency.splice(0).sort((a, b) => a - b);
  ops = {
    at: Date.now(), kafka: k, pg: p, sims: s, services, resources, storage,
    integrity: { accepted: Number(counts[0] ?? 0), stored: Number(counts[1] ?? 0) },
    api: { read_p95_ms: sorted.length ? Math.round(sorted[Math.floor(sorted.length * 0.95)]) : null, replica_reads: replicaReads, primary_fallbacks: primaryFallbacks },
    mqtt: { queued: brokerQueue },
    // A stopped service stops publishing: show nothing rather than its last numbers.
    stats: Object.fromEntries([...stats].filter(([, v]) => Date.now() - v.at < 6000)),
  };
  replicaReads = primaryFallbacks = 0;
  broadcast('ops', ops);
}
setInterval(() => refreshOps().catch(e => log('api', 'ops failed', { error: (e as Error).message })), 2000);
// Container usage refreshes every second on its own event: cheap to read from chaos, unlike the rest of ops.
setInterval(async () => {
  const r = await withTimeout(fetch(`${CHAOS}/resources`).then(x => x.json()), 900).catch(() => null);
  if (r) { ops.resources = r; broadcast('resources', r); }
}, 1000);
app.get('/ops', async () => ops);

await app.listen({ host: '0.0.0.0', port: Number(env('PORT', '3000')) });
log('api', 'listening', { port: env('PORT', '3000') });
