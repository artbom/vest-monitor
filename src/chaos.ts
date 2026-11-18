// Fault injection for the demo: stops and starts containers of this compose project through the Docker socket.
// Only the services in ALLOWED can be touched, and the service is reachable only from the api inside the compose network.
import http from 'node:http';
import { env, log } from './shared.ts';

const PROJECT = env('COMPOSE_PROJECT', 'vest');
const ALLOWED = new Set(['kafka-1', 'kafka-2', 'kafka-3', 'pg-replica', 'writer', 'detection', 'ingest', 'redis', 'mosquitto', 'saga-worker', 'temporal']);

function docker<T>(method: string, path: string): Promise<{ status: number; body: T }> {
  return new Promise((resolve, reject) => {
    const req = http.request({ socketPath: '/var/run/docker.sock', method, path }, res => {
      let data = '';
      res.on('data', c => (data += c));
      res.on('end', () => resolve({ status: res.statusCode ?? 0, body: data ? JSON.parse(data) : (null as T) }));
    });
    req.on('error', reject);
    req.end();
  });
}

interface Container { Id: string; State: string; Labels: Record<string, string> }
async function containers() {
  const filters = encodeURIComponent(JSON.stringify({ label: [`com.docker.compose.project=${PROJECT}`] }));
  const { body } = await docker<Container[]>('GET', `/containers/json?all=1&filters=${filters}`);
  return body.filter(c => ALLOWED.has(c.Labels['com.docker.compose.service']))
    .map(c => ({ id: c.Id, service: c.Labels['com.docker.compose.service'], running: c.State === 'running' }));
}

http.createServer(async (req, res) => {
  try {
    const url = new URL(req.url ?? '/', 'http://chaos');
    if (req.method === 'GET' && url.pathname === '/services') return res.end(JSON.stringify(await containers()));
    const m = url.pathname.match(/^\/services\/([\w-]+)\/(stop|start)$/);
    if (req.method === 'POST' && m && ALLOWED.has(m[1])) {
      const targets = (await containers()).filter(c => c.service === m[1]);
      for (const c of targets) await docker('POST', `/containers/${c.id}/${m[2]}?t=2`);
      log('chaos', `${m[2]} ${m[1]}`, { containers: targets.length });
      return res.end(JSON.stringify({ service: m[1], action: m[2], containers: targets.length }));
    }
    res.statusCode = 404; res.end('{}');
  } catch (e) {
    res.statusCode = 500; res.end(JSON.stringify({ error: (e as Error).message }));
  }
}).listen(3001);
