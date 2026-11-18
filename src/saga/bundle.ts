// Bundles the workflow code once at image build time, so a restarted worker is polling in a second
// instead of spending ~10 s and a few hundred MB on webpack at every start.
import fs from 'node:fs';
import path from 'node:path';
import { bundleWorkflowCode } from '@temporalio/worker';

const out = path.join(import.meta.dirname, 'workflow-bundle.js');
const { code } = await bundleWorkflowCode({ workflowsPath: path.join(import.meta.dirname, 'workflows.ts') });
fs.writeFileSync(out, code);
console.log(`workflow bundle: ${out}, ${Math.round(code.length / 1024)} KB`);
