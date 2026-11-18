// MQTT -> validate -> Kafka (acks=all) -> only then PUBACK to the broker.
// The broker already answered the vest; it keeps the message in this persistent session until we acknowledge,
// so a packet is never dropped between the broker and Kafka. While Kafka is unavailable we stop acknowledging,
// the broker queues, and receiveMaximum caps how many unacknowledged messages we hold in memory.
import mqtt from 'mqtt';
import { Redis } from 'ioredis';
import { config, decodePacket, keys, log, sleep, fastRedis } from './shared.ts';
import { ensureTopic, kafka } from './kafka.ts';
import { Latency, reportStats } from './stats.ts';

interface Pending { deviceId: string; payload: Buffer; receivedAt: number; ack: () => void }

const redis = new Redis(config.redisUrl, fastRedis);
const producer = kafka.producer({ allowAutoTopicCreation: false });
const counters = { received: 0, accepted: 0, invalid: 0, kafka_errors: 0, bytes: 0 };
const ackLatency = new Latency();
let queue: Pending[] = [];
let flushing = false;
let flushStartedAt = 0;

await ensureTopic();
await producer.connect();

// Micro-batching: packets that arrived together go to Kafka in one request, each waits for acks from all in-sync replicas.
async function flush() {
  if (flushing || !queue.length) return;
  flushing = true;
  flushStartedAt = Date.now();
  const batch = queue.splice(0, 500);
  for (;;) {
    try {
      await producer.send({
        topic: config.topic, acks: -1,
        messages: batch.map(p => ({ key: p.deviceId, value: p.payload, timestamp: String(p.receivedAt) })),
      });
      break;
    } catch (e) {
      counters.kafka_errors++;
      log('ingest', 'kafka write failed, holding the batch (broker keeps the messages unacknowledged)', { packets: batch.length, error: (e as Error).message });
      await sleep(1000);
    }
  }
  const now = Date.now();
  for (const p of batch) { p.ack(); ackLatency.add(now - p.receivedAt); }
  counters.accepted += batch.length;
  redis.incrby(keys.accepted, batch.length).catch(() => {});
  flushing = false;
  if (queue.length) flush();
}
setInterval(flush, 5);

// mqtt.js handles the incoming packets of one connection strictly one after another and takes the next only after the
// PUBACK. Acknowledging after Kafka therefore caps one session at ~1 / Kafka latency (~80 packets/s). That is the price
// of "ACK only after a durable write"; the fix is more sessions, all in one shared subscription (and more instances).
const SESSIONS = Number(process.env.INGEST_SESSIONS ?? 32);
for (let i = 0; i < SESSIONS; i++) session(i);

function session(i: number) {
const client = mqtt.connect(config.mqttUrl, {
  // Stable client ids: after a restart the instance resumes its sessions and gets what the broker queued for them.
  // A second instance (docker compose --scale) needs its own INGEST_ID.
  clientId: `ingest-${process.env.INGEST_ID ?? '1'}-${i}`, protocolVersion: 5, clean: false, reconnectPeriod: 1000,
  // No keepalive: while a PUBLISH waits for Kafka, the PINGRESP queued behind it would time the session out and cause redeliveries.
  keepalive: 0,
  properties: { sessionExpiryInterval: 3600, receiveMaximum: 100 },
  customHandleAcks: (topic, message, _packet, done) => {
    const receivedAt = Date.now();
    counters.received++; counters.bytes += message.length;
    try {
      const p = decodePacket(message);
      if (topic !== `vests/${p.deviceId}/telemetry`) throw new Error(`topic ${topic} does not match device ${p.deviceId}`);
      queue.push({ deviceId: p.deviceId, payload: Buffer.from(message), receivedAt, ack: () => done(0) });
      if (queue.length >= 500) flush();
    } catch (e) {
      counters.invalid++;
      log('ingest', 'invalid packet', { topic, error: (e as Error).message });
      done(131);   // "implementation specific error": acknowledged, so the broker drops it instead of resending forever
    }
  },
});
// A shared subscription spreads vests over all ingest instances: `docker compose up --scale ingest=3`.
client.on('connect', () => {
  client.subscribe('$share/ingest/vests/+/telemetry', { qos: 1 });
  if (i === 0) log('ingest', 'subscribed', { topic: '$share/ingest/vests/+/telemetry', sessions: SESSIONS });
});
client.on('error', e => log('ingest', 'mqtt error', { error: e.message }));
}

reportStats(redis, 'ingest', () => {
  const c = { ...counters, ack_p99_ms: ackLatency.take(0.99), pending_now: queue.length,
    kafka_wait_now: flushing ? Date.now() - flushStartedAt : 0 };
  for (const k in counters) counters[k as keyof typeof counters] = 0;
  return c;
});
