// Kafka plumbing: client, topic setup and a consumer-group loop that commits offsets only after the handler succeeded.
import os from 'node:os';
import { Kafka, logLevel, type KafkaMessage } from 'kafkajs';
import { config, log, sleep } from './shared.ts';

export const kafka = new Kafka({
  clientId: `${process.argv[1]?.split('/').pop()?.replace('.ts', '')}-${os.hostname()}`,
  brokers: config.kafkaBrokers,
  logLevel: logLevel.ERROR,
  retry: { initialRetryTime: 300, maxRetryTime: 2000, retries: 8 },   // recover within ~2 s once brokers are back
});

// 12 partitions keyed by device_id: one vest's packets stay in order; replication factor 3 and min.insync.replicas 2
// mean a write is confirmed only when two brokers have it, and the topic survives one broker going down.
export async function ensureTopic(): Promise<void> {
  const admin = kafka.admin();
  for (;;) {
    try {
      await admin.connect();
      await admin.createTopics({
        waitForLeaders: true,
        topics: [{ topic: config.topic, numPartitions: config.partitions, replicationFactor: 3,
          configEntries: [{ name: 'min.insync.replicas', value: '2' }, { name: 'retention.ms', value: String(7 * 24 * 3600_000) }] }],
      });
      await admin.disconnect();
      return;
    } catch (e) {
      log('kafka', 'waiting for the cluster', { error: (e as Error).message });
      await admin.disconnect().catch(() => {});
      await sleep(2000);
    }
  }
}

export interface Packet { data: Buffer; receivedAt: number }
const toPacket = (m: KafkaMessage): Packet => ({ data: m.value!, receivedAt: Number(m.timestamp) });

// A fetched partition batch is handled in chunks of CHUNK packets. A failed chunk is retried (heartbeating, so the group
// keeps the partition); after each successful chunk its offset is committed: at-least-once, so handlers must be idempotent.
// Chunks keep every step short: a 100 000-row INSERT under load would outlive the session and get the consumer kicked out.
const CHUNK = 500;
export async function consume(groupId: string, handle: (packets: Packet[], partition: number) => Promise<void>,
  { fromBeginning = true, concurrency = 3 } = {}): Promise<void> {
  await ensureTopic();
  const consumer = kafka.consumer({ groupId, sessionTimeout: 30_000, heartbeatInterval: 3000, maxWaitTimeInMs: 100, maxBytesPerPartition: 512 * 1024 });
  // If the consumer gives up (e.g. it was fenced out of the group), exit: Docker restarts us from the committed offsets.
  consumer.on(consumer.events.CRASH, e => { log(groupId, 'consumer crashed, exiting', { error: e.payload.error.message }); process.exit(1); });
  await consumer.connect();
  await consumer.subscribe({ topic: config.topic, fromBeginning });
  await consumer.run({
    autoCommit: false,
    partitionsConsumedConcurrently: concurrency,
    eachBatchAutoResolve: false,
    eachBatch: async ({ batch, resolveOffset, heartbeat, commitOffsetsIfNecessary, isRunning, isStale }) => {
      for (let i = 0; i < batch.messages.length; i += CHUNK) {
        const chunk = batch.messages.slice(i, i + CHUNK);
        for (let attempt = 1; ; attempt++) {
          if (!isRunning() || isStale()) return;
          try {
            await handle(chunk.map(toPacket), batch.partition);
            break;
          } catch (e) {
            if (attempt === 1 || attempt % 10 === 0) log(groupId, 'chunk failed, retrying', { partition: batch.partition, attempt, error: (e as Error).message });
            await heartbeat().catch(() => {});
            await sleep(1000);
          }
        }
        const last = chunk.at(-1)!.offset;
        resolveOffset(last);
        // The batch payload's commit goes straight to the coordinator, without consumer.commitOffsets' own retries. When
        // the group coordinator moves (a broker came back and took the leadership of __consumer_offsets), those retries kept
        // hitting the old broker for ~30 s; kafkajs looks the coordinator up again only on re-join. Failing at once lets
        // kafkajs re-join in about a second; the uncommitted chunk is read again and ON CONFLICT drops it.
        await commitOffsetsIfNecessary({ topics: [{ topic: batch.topic, partitions: [{ partition: batch.partition, offset: (BigInt(last) + 1n).toString() }] }] });
        await heartbeat();
      }
    },
  });
}
