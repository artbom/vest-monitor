// GDPR deletion saga, orchestrated by Temporal. The workflow is plain code: Temporal records the result of every
// step (activity) in its history, so after a worker crash the function is replayed and continues from the step
// that did not finish, not from the start. No compensations: a deletion only moves forward, failed steps are retried.
import { proxyActivities, workflowInfo } from '@temporalio/workflow';
import type * as activities from './activities.ts';

const act = proxyActivities<typeof activities>({
  startToCloseTimeout: '2 minutes',
  // A dead worker stops heartbeating: after 5 s the step is handed to the next worker that polls the queue.
  heartbeatTimeout: '5 seconds',
  retry: { initialInterval: '1 second', backoffCoefficient: 2, maximumInterval: '30 seconds' },   // unlimited attempts
});

export async function gdprDelete(ownerId: number): Promise<{ devices: string[]; deleted: Record<string, number> }> {
  const { runId } = workflowInfo();
  const devices = await act.markRequest(ownerId, runId);         // 1. deletion_requests: what we are about to delete
  await act.deletePostgres(ownerId, runId);                      // 2. owner, pets (cascade), close vest bindings
  const deleted = await act.deleteTelemetry(runId);              // 3. metrics, aggregates, alerts for the binding periods
  await act.rewriteParquet(runId);                               // 4. cold archive (stub in the demo)
  await act.dropRedisCopies(runId);                              // 5. detection's profile copies
  return { devices, deleted };
}
