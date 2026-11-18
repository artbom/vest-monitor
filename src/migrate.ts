// Applies db/saga.sql to a database created before it existed, so nobody has to wipe the volumes.
// The script is idempotent; an advisory lock keeps api and saga-worker from running it at the same time.
import fs from 'node:fs';
import path from 'node:path';
import pg from 'pg';
import { env, log } from './shared.ts';

const SQL = fs.readFileSync(path.join(import.meta.dirname, '../db/saga.sql'), 'utf8');

export async function migrate(service: string): Promise<void> {
  const url = `postgres://vest:vest@${env('PG_PRIMARY', 'localhost:15432')}/vest`;
  for (let attempt = 1; ; attempt++) {
    const c = new pg.Client({ connectionString: url });
    c.on('notice', () => {});
    try {
      await c.connect();
      await c.query('BEGIN');
      await c.query('SELECT pg_advisory_xact_lock(4242)');
      await c.query(SQL);
      await c.query('COMMIT');
      log(service, 'schema migrated');
      return;
    } catch (e) {
      if (attempt >= 30) throw e;
      log(service, 'migration failed, retrying', { error: (e as Error).message });
      await new Promise(r => setTimeout(r, 2000));
    } finally {
      await c.end().catch(() => {});
    }
  }
}
