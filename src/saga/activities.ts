// Saga steps. Every step is idempotent: running it again after a crash (the step finished, but Temporal never
// heard about it) changes nothing, so the orchestrator can always just retry. Each step is slowed down by STEP_DELAY_MS
// so the progress is visible in the demo.
import { activityInfo, heartbeat, sleep } from '@temporalio/activity';
import { Redis } from 'ioredis';
import pg from 'pg';
import { config, env, keys, log } from '../shared.ts';

// Role "saga": synchronous_commit = remote_apply, the relational deletion is confirmed only once the replica has it.
const db = new pg.Pool({ connectionString: `postgres://saga:saga@${env('PG_PRIMARY', 'localhost:15432')}/vest`, max: 3 });
db.on('error', () => {});
const redis = new Redis(config.redisUrl, { maxRetriesPerRequest: 1 });
redis.on('error', () => {});
const STEP_DELAY_MS = Number(env('STEP_DELAY_MS', '5000'));

const BUCKETS: Record<string, string> = { metrics_1m: '1 minute', metrics_1h: '1 hour', metrics_1d: '1 day' };
interface Period { device_id: string; from: string; to: string | null }

// Heartbeats while the step works: a worker that died stops heartbeating, and Temporal hands the step to another worker.
async function step<T>(runId: string, name: string, work: () => Promise<T>): Promise<T> {
  const { attempt } = activityInfo();
  await db.query(`UPDATE deletion_requests SET step = $2, updated_at = now() WHERE run_id = $1`, [runId, name]);
  log('saga-worker', `step ${name}`, { runId, attempt });
  const beat = setInterval(() => heartbeat(), 1000);
  try {
    await sleep(STEP_DELAY_MS);
    return await work();
  } finally {
    clearInterval(beat);
  }
}

// 1. Record the request and the owner's vests. ON CONFLICT: a retried step does not create a second request.
export async function markRequest(ownerId: number, runId: string): Promise<string[]> {
  const { workflowExecution } = activityInfo();
  await db.query(`INSERT INTO deletion_requests (owner_id, workflow_id, run_id, devices)
    SELECT $1, $2, $3, coalesce(array_agg(DISTINCT b.device_id), '{}') FROM vest_bindings b JOIN pets p ON p.id = b.pet_id WHERE p.owner_id = $1
    ON CONFLICT (run_id) DO NOTHING`, [ownerId, workflowExecution?.workflowId ?? '', runId]);
  return step(runId, 'mark', async () =>
    (await db.query<{ devices: string[] }>(`SELECT devices FROM deletion_requests WHERE run_id = $1`, [runId])).rows[0].devices);
}

// 2. One local transaction: close the owner's bindings, remember the periods (whose telemetry it was), delete the owner.
// Pets go with it (ON DELETE CASCADE), vests stay but point to nobody. The periods are written before the delete
// in the same transaction, so a retry after the owner is gone still knows what to delete.
export async function deletePostgres(ownerId: number, runId: string): Promise<void> {
  return step(runId, 'postgres', async () => {
    const c = await db.connect();
    try {
      await c.query('BEGIN');
      const { rows: [r] } = await c.query<{ periods: Period[] | null }>(`SELECT periods FROM deletion_requests WHERE run_id = $1 FOR UPDATE`, [runId]);
      if (!r.periods) {
        await c.query(`UPDATE vest_bindings b SET unbound_at = now() FROM pets p WHERE p.id = b.pet_id AND p.owner_id = $1 AND b.unbound_at IS NULL`, [ownerId]);
        await c.query(`UPDATE deletion_requests SET periods = (
            SELECT coalesce(jsonb_agg(jsonb_build_object('device_id', b.device_id, 'from', b.bound_at, 'to', b.unbound_at)), '[]')
              FROM vest_bindings b JOIN pets p ON p.id = b.pet_id WHERE p.owner_id = $1) WHERE run_id = $2`, [ownerId, runId]);
      }
      const del = await c.query(`DELETE FROM owners WHERE id = $1`, [ownerId]);
      await c.query('COMMIT');
      log('saga-worker', 'postgres: owner deleted', { ownerId, deleted: del.rowCount });
    } catch (e) {
      await c.query('ROLLBACK').catch(() => {});
      throw e;
    } finally {
      c.release();
    }
  });
}

// 3. Telemetry of the binding periods: raw seconds, continuous aggregates (their materialized rows stay after the raw
// rows are gone) and alerts. Telemetry is PA/EL data: these deletes do not wait for the replica.
export async function deleteTelemetry(runId: string): Promise<Record<string, number>> {
  return step(runId, 'telemetry', async () => {
    const periods = await getPeriods(runId);
    const aggs = (await db.query<{ view: string; t: string }>(`SELECT view_name AS view,
        format('%I.%I', materialization_hypertable_schema, materialization_hypertable_name) AS t FROM timescaledb_information.continuous_aggregates`)).rows;
    const deleted: Record<string, number> = { metrics: 0, alerts: 0 };
    const c = await db.connect();
    try {
      await c.query(`SET synchronous_commit = local`);
      for (const p of periods) {
        const args = [p.device_id, p.from, p.to ?? 'infinity'];
        deleted.metrics += (await c.query(`DELETE FROM metrics WHERE device_id = $1 AND ts >= $2 AND ts < $3`, args)).rowCount ?? 0;
        deleted.alerts += (await c.query(`DELETE FROM alerts WHERE device_id = $1 AND ts >= $2 AND ts < $3`, args)).rowCount ?? 0;
        for (const a of aggs) {
          // Whole buckets that overlap the period: a bucket mixing two owners' data is dropped, not split.
          const width = BUCKETS[a.view] ?? '1 day';
          const n = (await c.query(`DELETE FROM ${a.t} WHERE device_id = $1 AND bucket > $2::timestamptz - $4::interval AND bucket < $3`, [...args, width])).rowCount ?? 0;
          deleted[a.view] = (deleted[a.view] ?? 0) + n;
        }
      }
    } finally {
      await c.query(`RESET synchronous_commit`).catch(() => {});
      c.release();
    }
    log('saga-worker', 'telemetry deleted', { runId, ...deleted });
    return deleted;
  });
}

// 4. Cold archive: monthly Parquet files per vest in object storage would be rewritten without these periods.
// The demo has no archive, so the step only says what it would do.
export async function rewriteParquet(runId: string): Promise<void> {
  return step(runId, 'parquet', async () => {
    for (const p of await getPeriods(runId))
      log('saga-worker', 'parquet: would rewrite archive files (stub)', { device_id: p.device_id, from: p.from, to: p.to });
  });
}

// 5. Detection's read-only copies of the profiles. Last step: marks the request done.
export async function dropRedisCopies(runId: string): Promise<void> {
  return step(runId, 'redis', async () => {
    const devices = [...new Set((await getPeriods(runId)).map(p => p.device_id))];
    if (devices.length) await redis.del(...devices.map(d => keys.profile(d)));
    await db.query(`UPDATE deletion_requests SET status = 'done', step = 'done', updated_at = now() WHERE run_id = $1`, [runId]);
  });
}

async function getPeriods(runId: string): Promise<Period[]> {
  return (await db.query<{ periods: Period[] }>(`SELECT periods FROM deletion_requests WHERE run_id = $1`, [runId])).rows[0]?.periods ?? [];
}
