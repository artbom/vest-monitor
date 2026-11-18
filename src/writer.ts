// Kafka (group "writer") -> one row per vest per second in the TimescaleDB hypertable `metrics` on the primary.
// Commits as role "writer" (synchronous_commit = local): does not wait for the replica (PA/EL).
// The offset is committed after the INSERT, and ON CONFLICT DO NOTHING makes a re-read batch harmless.
import { Redis } from 'ioredis';
import pg from 'pg';
import { config, decodePacket, keys, METRICS, type MetricName, fastRedis } from './shared.ts';
import { consume } from './kafka.ts';
import { Latency, reportStats } from './stats.ts';

const db = new pg.Pool({ connectionString: config.db('writer'), max: 3 });
db.on('error', () => {});
const redis = new Redis(config.redisUrl, fastRedis);
const COLS = METRICS.map(m => m.name);
const TYPES: Record<MetricName, string> = { heart_rate: 'smallint', resp_rate: 'smallint', temperature: 'real', activity: 'smallint', steps: 'int', battery: 'smallint' };
// unnest() turns column arrays into rows: one INSERT for the whole batch.
const SQL = `INSERT INTO metrics (device_id, ts, ${COLS.join(', ')})
  SELECT * FROM unnest($1::text[], $2::timestamptz[], ${COLS.map((c, i) => `$${i + 3}::${TYPES[c]}[]`).join(', ')})
  ON CONFLICT DO NOTHING`;
const seen = new Set<string>();
const counters = { packets: 0, rows: 0, duplicates: 0 };
const e2e = new Latency();     // vest packet received by ingest -> row committed

reportStats(redis, 'writer', () => {
  const c = { ...counters, e2e_p99_ms: e2e.take(0.99) };
  counters.packets = counters.rows = counters.duplicates = 0;
  return c;
});

await consume('writer', async packets => {
  const deviceIds: string[] = [], ts: Date[] = [];
  const values = Object.fromEntries(COLS.map(c => [c, [] as (number | null)[]])) as Record<MetricName, (number | null)[]>;
  for (const m of packets) {
    const p = decodePacket(m.data);
    for (const r of p.rows) {
      deviceIds.push(p.deviceId); ts.push(r.ts);
      for (const c of COLS) values[c].push(r[c] ?? null);
    }
  }
  // A vest the database does not know yet (another simulator, a new device) gets a row without a pet.
  const fresh = [...new Set(deviceIds)].filter(d => !seen.has(d));
  if (fresh.length) {
    await db.query('INSERT INTO vests (device_id) SELECT unnest($1::text[]) ON CONFLICT DO NOTHING', [fresh]);
    fresh.forEach(d => seen.add(d));
  }
  const { rowCount } = await db.query(SQL, [deviceIds, ts, ...COLS.map(c => values[c])]);
  const now = Date.now();
  for (const m of packets) e2e.add(now - m.receivedAt);
  counters.rows += rowCount ?? 0; counters.packets += packets.length; counters.duplicates += ts.length - (rowCount ?? 0);
  redis.incrby(keys.stored, packets.length).catch(() => {});   // a counter for the dashboard: never block writes on it
});
