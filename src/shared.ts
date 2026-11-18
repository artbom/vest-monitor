// Shared pieces: config, protobuf schema, metric registry, packet <-> readings, Redis keys.
import path from 'node:path';
import protobuf from 'protobufjs';

export const env = (name: string, fallback: string): string => process.env[name] ?? fallback;

const pgUrl = (role: string, host: string) => `postgres://${role}:${role}@${host}/vest`;
export const config = {
  mqttUrl: env('MQTT_URL', 'mqtt://localhost:11883'),
  redisUrl: env('REDIS_URL', 'redis://localhost:16379'),
  kafkaBrokers: env('KAFKA_BROKERS', 'localhost:19092').split(','),
  topic: 'telemetry',
  partitions: 12,
  // One database role per service: the role decides whether a commit waits for the replica (see db/init.sql).
  db: (role: 'writer' | 'detection' | 'api' | 'reader', replica = false) =>
    pgUrl(role, replica ? env('PG_REPLICA', 'localhost:15433') : env('PG_PRIMARY', 'localhost:15432')),
};

// Redis pub/sub channels and keys shared by the services, the simulators and the UI.
export const channels = {
  alerts: 'alerts',          // detection -> api: every raised alert
  stats: 'stats',            // every service -> api: rates and latencies every 2 s
  simControl: 'sim:control', // api -> simulators: config or scenarios changed
} as const;
export const keys = {
  simConfig: (name: string) => `sim:${name}:config`,
  simStatus: (name: string) => `sim:${name}:status`,
  scenarios: 'sim:scenarios',                     // deviceId -> scenario, shared by all simulators
  profile: (deviceId: string) => `profile:${deviceId}`,   // detection's read-only copy of the pet profile
  alertPause: (deviceId: string, type: string) => `alert:${deviceId}:${type}`,
  accepted: 'sli:accepted',                       // packets ingest got into Kafka
  stored: 'sli:stored',                           // packets writer committed to the database
} as const;

// Simulator scenarios a vest can be switched to from the UI.
export const SCENARIOS = ['normal', 'tachycardia', 'fever', 'low_battery', 'offline'] as const;
export type Scenario = (typeof SCENARIOS)[number];

export const METRICS = [
  { id: 1, name: 'heart_rate', scale: 1 },
  { id: 2, name: 'resp_rate', scale: 1 },
  { id: 3, name: 'temperature', scale: 10 },
  { id: 4, name: 'activity', scale: 1 },
  { id: 5, name: 'steps', scale: 1 },
  { id: 6, name: 'battery', scale: 1 },
] as const;
export type MetricName = (typeof METRICS)[number]['name'];
export type Sample = Record<MetricName, number>;
export type Row = Partial<Sample> & { ts: Date };
export interface DecodedPacket { deviceId: string; seq: number; startUtc: number; rows: Row[] }

interface SeriesMessage { id: number; scale: number; deltas: number[] }
interface PacketMessage { deviceId: string; tzOffsetMin: number; seq: number; startUtc: number; stepS: number; series: SeriesMessage[] }

const root = protobuf.loadSync(path.join(import.meta.dirname, '../proto/vest.proto'));
const SeriesPacket = root.lookupType('vest.SeriesPacket');
const BY_ID = new Map<number, (typeof METRICS)[number]>(METRICS.map(m => [m.id, m]));

// samples: one per second starting at startUtc.
export function encodePacket(p: { deviceId: string; seq: number; startUtc: number; tzOffsetMin?: number; samples: Sample[] }): Uint8Array {
  const series = METRICS.map(m => {
    const ints = p.samples.map(s => Math.round(s[m.name] * m.scale));
    return { id: m.id, scale: m.scale, deltas: ints.map((v, i) => (i ? v - ints[i - 1] : v)) };
  });
  return SeriesPacket.encode({ deviceId: p.deviceId, seq: p.seq, startUtc: p.startUtc, tzOffsetMin: p.tzOffsetMin ?? 180, stepS: 1, series }).finish();
}

// Throws on a malformed packet. Metrics this service does not know are skipped, not rejected.
export function decodePacket(bytes: Uint8Array): DecodedPacket {
  const p = SeriesPacket.decode(bytes) as unknown as PacketMessage;
  if (!p.deviceId || !p.startUtc || !p.series.length) throw new Error('packet without device_id, start_utc or series');
  const length = p.series[0].deltas.length;
  const rows: Row[] = Array.from({ length }, (_, i) => ({ ts: new Date((p.startUtc + i * (p.stepS || 1)) * 1000) }));
  for (const s of p.series) {
    const m = BY_ID.get(s.id);
    if (!m) continue;
    if (s.deltas.length !== length) throw new Error(`series ${m.name} has ${s.deltas.length} samples, expected ${length}`);
    let v = 0;
    s.deltas.forEach((d, i) => { v += d; rows[i][m.name] = v / (s.scale || 1); });
  }
  return { deviceId: p.deviceId, seq: p.seq, startUtc: p.startUtc, rows };
}

export const log = (service: string, msg: string, extra?: object): void =>
  console.log(JSON.stringify({ t: new Date().toISOString(), service, msg, ...extra }));

// Redis client for the request path: while Redis is down commands fail in ~300 ms instead of queueing for a minute,
// so callers fall back (detection reads the profile from the database, stats are skipped).
export const fastRedis = { enableOfflineQueue: false, maxRetriesPerRequest: 1, commandTimeout: 300 };

export const sleep = (ms: number) => new Promise(r => setTimeout(r, ms));
