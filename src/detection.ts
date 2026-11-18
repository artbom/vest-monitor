// Kafka (group "detection") -> per-pet rules -> alert row + notification (mocked: a log line and a live event for the UI).
// Works independently of the writer: its own consumer group and offsets, so a slow or failed database write
// does not delay alerts. Partitions are keyed by device_id, so each vest's streaks live in exactly one instance.
import { Redis } from 'ioredis';
import pg from 'pg';
import { channels, config, decodePacket, keys, log, type Row, fastRedis } from './shared.ts';
import { consume } from './kafka.ts';
import { Latency, reportStats } from './stats.ts';

const db = new pg.Pool({ connectionString: config.db('detection'), max: 3 });
db.on('error', () => {});
const redis = new Redis(config.redisUrl, fastRedis);
const PAUSE_S = 600;                     // one notification per vest and rule per 10 minutes
const PROFILE_TTL_S = 3600;              // safety net; the api deletes the key when the profile changes

interface Profile { pet: string | null; owner: string | null; owner_email: string | null; vet: string | null; vet_email: string | null; hr_max: number; temp_max: number }
interface Rule { type: string; test: (r: Row, p: Profile) => boolean; seconds: number; value: (r: Row) => number | undefined }
// The condition has to hold for `seconds` consecutive samples; streaks survive packet boundaries.
const RULES: Rule[] = [
  { type: 'tachycardia', test: (r, p) => (r.heart_rate ?? 0) >= p.hr_max, seconds: 10, value: r => r.heart_rate },
  { type: 'fever', test: (r, p) => (r.temperature ?? 0) >= p.temp_max, seconds: 30, value: r => r.temperature },
  { type: 'low_battery', test: r => (r.battery ?? 100) <= 10, seconds: 1, value: r => r.battery },
];
const streaks = new Map<string, Record<string, number>>();
const counters = { packets: 0, alerts: 0, suppressed: 0, cache_hits: 0, cache_misses: 0 };
const alertLatency = new Latency();      // packet received by ingest -> notification sent

// Cache-aside with a read-only copy of the profile in Redis. Concurrent misses for one vest (an offline buffer
// flushing hundreds of packets) share a single database query: single-flight. TTL gets jitter so keys written
// together after a Redis restart do not expire together.
const inflight = new Map<string, Promise<Profile>>();
async function profile(deviceId: string): Promise<Profile> {
  const cached = await redis.get(keys.profile(deviceId)).catch(() => null);
  if (cached) { counters.cache_hits++; return JSON.parse(cached); }
  counters.cache_misses++;
  let p = inflight.get(deviceId);
  if (!p) {
    p = loadProfile(deviceId).finally(() => inflight.delete(deviceId));
    inflight.set(deviceId, p);
  }
  return p;
}
async function loadProfile(deviceId: string): Promise<Profile> {
  const { rows: [r] } = await db.query<Profile>(
    `SELECT p.name AS pet, o.name AS owner, o.email AS owner_email, vt.name AS vet, vt.email AS vet_email,
            coalesce(p.hr_max, 160) AS hr_max, coalesce(p.temp_max, 40) AS temp_max
       FROM vests v LEFT JOIN pets p ON p.id = v.pet_id LEFT JOIN owners o ON o.id = p.owner_id LEFT JOIN vets vt ON vt.id = p.vet_id
      WHERE v.device_id = $1`, [deviceId]);
  const profile: Profile = r ?? { pet: null, owner: null, owner_email: null, vet: null, vet_email: null, hr_max: 160, temp_max: 40 };
  const ttl = PROFILE_TTL_S + Math.floor(Math.random() * 600);
  await redis.set(keys.profile(deviceId), JSON.stringify(profile), 'EX', ttl).catch(() => {});   // Redis down: work without the copy
  return profile;
}

// Order matters for at-least-once: check the pause, write the alert idempotently, notify, and only then set the pause.
// A crash anywhere before the pause is set means the retry raises the alert again (the unique key drops the duplicate row)
// instead of a pause silently swallowing an alert that was never sent.
async function raise(deviceId: string, rule: Rule, row: Row, p: Profile, receivedAt: number) {
  const pauseKey = keys.alertPause(deviceId, rule.type);
  // The pause lives in Redis (shared by instances, survives a restart) with a local copy for when Redis is down.
  const paused = await redis.exists(pauseKey).then(n => n > 0).catch(() => (localPause.get(pauseKey) ?? 0) > Date.now());
  if (paused) { counters.suppressed++; return; }
  const value = rule.value(row) ?? null;
  await db.query('INSERT INTO alerts (device_id, ts, type, value) VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING', [deviceId, row.ts, rule.type, value]);
  const latency = Date.now() - receivedAt;
  alertLatency.add(latency);
  const event = { device_id: deviceId, type: rule.type, value, ts: row.ts.toISOString(), pet: p.pet, owner: p.owner, vet: p.vet,
    threshold: rule.type === 'tachycardia' ? p.hr_max : rule.type === 'fever' ? p.temp_max : 10, latency_ms: latency };
  await redis.publish(channels.alerts, JSON.stringify(event)).catch(() => {});
  counters.alerts++;
  log('detection', 'NOTIFY', { ...event, to: [p.owner_email, p.vet_email].filter(Boolean) });
  localPause.set(pauseKey, Date.now() + PAUSE_S * 1000);
  await redis.set(pauseKey, '1', 'EX', PAUSE_S).catch(() => {});
}
const localPause = new Map<string, number>();

reportStats(redis, 'detection', () => {
  const c = { ...counters, alert_p99_ms: alertLatency.take(0.99) };
  for (const k in counters) counters[k as keyof typeof counters] = 0;
  return c;
});

// Streaks count consecutive seconds above the threshold. Rows at or before the last seen second are replays
// (at-least-once delivery, a vest resending its buffer) and are skipped; a gap in time breaks the streak.
const lastTs = new Map<string, number>();
await consume('detection', async packets => {
  for (const m of packets) {
    const pk = decodePacket(m.data);
    const p = await profile(pk.deviceId);
    const s = { ...(streaks.get(pk.deviceId) ?? {}) };
    let last = lastTs.get(pk.deviceId) ?? 0;
    for (const row of pk.rows) {
      const t = +row.ts;
      if (t <= last) continue;
      if (last && t - last > 1000) for (const k in s) s[k] = 0;
      last = t;
      for (const rule of RULES) {
        s[rule.type] = rule.test(row, p) ? (s[rule.type] ?? 0) + 1 : 0;
        // >= : a condition that lasts longer than the pause notifies again when the pause expires.
        if (s[rule.type] >= rule.seconds) await raise(pk.deviceId, rule, row, p, m.receivedAt);
      }
    }
    streaks.set(pk.deviceId, s);
    lastTs.set(pk.deviceId, last);
    counters.packets++;
  }
});
