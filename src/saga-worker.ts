// Temporal worker for the GDPR deletion saga: polls the "gdpr" task queue and runs the workflow and its steps.
// Stop it in the middle of a saga and start it again: the saga continues from the step that was running.
import fs from 'node:fs';
import path from 'node:path';
import { NativeConnection, Worker } from '@temporalio/worker';
import * as activities from './saga/activities.ts';
import { migrate } from './migrate.ts';
import { env, log, sleep } from './shared.ts';

await migrate('saga-worker');

const address = env('TEMPORAL_ADDRESS', 'localhost:17233');
let connection: NativeConnection;
for (;;) {
  try { connection = await NativeConnection.connect({ address }); break; }
  catch (e) { log('saga-worker', 'temporal not reachable, retrying', { address, error: (e as Error).message }); await sleep(2000); }
}

const bundle = path.join(import.meta.dirname, 'saga/workflow-bundle.js');
const worker = await Worker.create({
  connection,
  taskQueue: 'gdpr',
  activities,
  // Prebuilt at image build (src/saga/bundle.ts); locally it is bundled on start.
  ...(fs.existsSync(bundle) ? { workflowBundle: { codePath: bundle } } : { workflowsPath: path.join(import.meta.dirname, 'saga/workflows.ts') }),
});
log('saga-worker', 'polling', { address, taskQueue: 'gdpr' });
await worker.run();
