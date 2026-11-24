// Fault injection for the demo: stops and starts containers of this compose project through the Docker socket.
// Only the services in ALLOWED can be touched, and the service is reachable only from the api inside the compose network.
// It also reports resource limits and live usage of every container of the project (read-only, cached).
import http from 'node:http';
import { env, log } from './shared.ts';

const PROJECT = env('COMPOSE_PROJECT', 'vest');
// pg-primary has no automatic failover in the demo, api serves the dashboard and chaos is this service: never stopped.
const ALLOWED = new Set(['kafka-1', 'kafka-2', 'kafka-3', 'pg-replica', 'writer', 'detection', 'ingest', 'redis', 'mosquitto',
  'saga-worker', 'temporal', 'simulator']);

function docker<T>(method: string, path: string): Promise<{ status: number; body: T }> {
  return new Promise((resolve, reject) => {
    const req = http.request({ socketPath: '/var/run/docker.sock', method, path }, res => {
      let data = '';
      res.on('data', c => (data += c));
      res.on('end', () => { try { resolve({ status: res.statusCode ?? 0, body: data ? JSON.parse(data) : (null as T) }); } catch (e) { reject(e); } });
    });
    req.on('error', reject);
    req.setTimeout(5000, () => req.destroy(new Error('docker api timeout')));
    req.end();
  });
}

interface Container { Id: string; State: string; Labels: Record<string, string> }
async function all() {
  const filters = encodeURIComponent(JSON.stringify({ label: [`com.docker.compose.project=${PROJECT}`] }));
  const { body } = await docker<Container[]>('GET', `/containers/json?all=1&filters=${filters}`);
  return body.map(c => ({ id: c.Id, service: c.Labels['com.docker.compose.service'], running: c.State === 'running', state: c.State }));
}
const containers = async () => (await all()).filter(c => ALLOWED.has(c.service));

// ---- Resources: limits from the container config, usage from one-shot stats; CPU % from the delta of two polls ----
interface Usage { service: string; running: boolean; state: string; controllable: boolean;
  cpu_limit: number | null; mem_limit: number | null; cpu_pct: number | null; mem_used: number | null }
const limits = new Map<string, { cpu: number | null; mem: number | null }>();
let host = { cpus: 0, mem: 0 };
let resources: { at: number; host: typeof host; containers: Usage[] } = { at: 0, host, containers: [] };

interface CpuStats { cpu_usage: { total_usage: number }; system_cpu_usage?: number; online_cpus?: number }
interface Stats { cpu_stats: CpuStats; precpu_stats?: CpuStats; memory_stats: { usage?: number; stats?: Record<string, number> } }

// One long-lived `stats?stream=true` request per running container: Docker pushes a sample every second, which is
// cheaper than one-shot polls (each of those makes Docker sample for ~1 s) and lets the dashboard refresh every second.
const latest = new Map<string, { cpu: number | null; mem: number }>();
const streams = new Set<string>();
function follow(id: string) {
  if (streams.has(id)) return;
  streams.add(id);
  const req = http.request({ socketPath: '/var/run/docker.sock', method: 'GET', path: `/containers/${id}/stats?stream=true` }, res => {
    let buf = '';
    res.on('data', chunk => {
      buf += chunk;
      let nl;
      while ((nl = buf.indexOf('\n')) >= 0) {
        const line = buf.slice(0, nl); buf = buf.slice(nl + 1);
        if (!line.trim()) continue;
        try {
          const s = JSON.parse(line) as Stats;
          const cur = s.cpu_stats, pre = s.precpu_stats;
          const dTotal = cur.cpu_usage.total_usage - (pre?.cpu_usage.total_usage ?? 0);
          const dSystem = (cur.system_cpu_usage ?? 0) - (pre?.system_cpu_usage ?? 0);
          const cpu = pre?.system_cpu_usage && dSystem > 0 ? Math.max(0, dTotal / dSystem * (cur.online_cpus ?? host.cpus) * 100) : null;
          // Same as `docker stats`: page cache that the kernel can drop is not counted as used.
          const st = s.memory_stats.stats ?? {};
          latest.set(id, { cpu, mem: (s.memory_stats.usage ?? 0) - (st.inactive_file ?? st.total_inactive_file ?? 0) });
        } catch { /* partial or empty sample of a stopping container */ }
      }
    });
    res.on('end', () => { streams.delete(id); latest.delete(id); });
  });
  req.on('error', () => { streams.delete(id); latest.delete(id); });
  req.end();
}

async function usage(c: Awaited<ReturnType<typeof all>>[number]): Promise<Usage> {
  if (!limits.has(c.id)) {
    const { body } = await docker<{ HostConfig: { NanoCpus: number; Memory: number } }>('GET', `/containers/${c.id}/json`);
    limits.set(c.id, { cpu: body.HostConfig.NanoCpus ? body.HostConfig.NanoCpus / 1e9 : null, mem: body.HostConfig.Memory || null });
  }
  const lim = limits.get(c.id)!;
  if (c.running) follow(c.id);
  const u = c.running ? latest.get(c.id) : undefined;
  return { service: c.service, running: c.running, state: c.state, controllable: ALLOWED.has(c.service),
    cpu_limit: lim.cpu, mem_limit: lim.mem, cpu_pct: u?.cpu == null ? null : Math.round(u.cpu * 10) / 10, mem_used: u?.mem ?? null };
}

async function refreshResources() {
  if (!host.cpus) {
    const { body } = await docker<{ NCPU: number; MemTotal: number }>('GET', '/info');
    host = { cpus: body.NCPU, mem: body.MemTotal };
  }
  const list = await all();
  const rows = await Promise.all(list.map(c => usage(c).catch(() => ({ service: c.service, running: c.running, state: c.state,
    controllable: ALLOWED.has(c.service), cpu_limit: null, mem_limit: null, cpu_pct: null, mem_used: null }))));
  resources = { at: Date.now(), host, containers: rows.sort((a, b) => a.service.localeCompare(b.service)) };
}
// The container list is re-read every second (cheap); usage comes from the open stats streams above.
async function loop() { await refreshResources().catch(e => log('chaos', 'stats failed', { error: (e as Error).message })); setTimeout(loop, 1000); }
loop();

http.createServer(async (req, res) => {
  try {
    const url = new URL(req.url ?? '/', 'http://chaos');
    if (req.method === 'GET' && url.pathname === '/services') return res.end(JSON.stringify(await containers()));
    if (req.method === 'GET' && url.pathname === '/resources') return res.end(JSON.stringify(resources));
    const m = url.pathname.match(/^\/services\/([\w-]+)\/(stop|start)$/);
    if (req.method === 'POST' && m && ALLOWED.has(m[1])) {
      const targets = (await containers()).filter(c => c.service === m[1]);
      for (const c of targets) await docker('POST', `/containers/${c.id}/${m[2]}?t=2`);
      log('chaos', `${m[2]} ${m[1]}`, { containers: targets.length });
      refreshResources().catch(() => {});
      return res.end(JSON.stringify({ service: m[1], action: m[2], containers: targets.length }));
    }
    res.statusCode = 404; res.end('{}');
  } catch (e) {
    res.statusCode = 500; res.end(JSON.stringify({ error: (e as Error).message }));
  }
}).listen(3001);
