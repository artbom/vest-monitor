// Proxy in front of the Temporal Web UI (UI 2.54 in the dev server) on the same port, 18233.
// The UI puts a <meta> CSP into its HTML: script-src 'strict-dynamic' plus the hash of its inline bootstrap script.
// Current Chrome does not extend that trust to the bootstrap's dynamic import() of the app modules, blocks them and
// shows an empty page. The proxy drops that <meta> from HTML responses and passes everything else through unchanged.
import http from 'node:http';
import { env, log } from './shared.ts';

const TARGET = new URL(env('TEMPORAL_UI_TARGET', 'http://temporal:8233'));
const CSP_META = /<meta\s+http-equiv="content-security-policy"[^>]*>/i;

http.createServer((req, res) => {
  // No compression from upstream, so an HTML body can be edited as text. The Host header stays as the browser sent it.
  const headers = { ...req.headers };
  delete headers['accept-encoding'];
  const up = http.request({ hostname: TARGET.hostname, port: TARGET.port, method: req.method, path: req.url, headers }, upRes => {
    const isHtml = String(upRes.headers['content-type'] ?? '').startsWith('text/html');
    if (!isHtml) { res.writeHead(upRes.statusCode ?? 502, upRes.headers); upRes.pipe(res); return; }
    const chunks: Buffer[] = [];
    upRes.on('data', c => chunks.push(c));
    upRes.on('end', () => {
      const body = Buffer.from(Buffer.concat(chunks).toString('utf8').replace(CSP_META, ''));
      const out = { ...upRes.headers, 'content-length': String(body.length) };
      delete out['transfer-encoding'];
      res.writeHead(upRes.statusCode ?? 502, out);
      res.end(body);
    });
  });
  up.on('error', e => { if (!res.headersSent) res.writeHead(502, { 'content-type': 'text/plain' }); res.end(`Temporal UI недоступний: ${e.message}`); });
  req.pipe(up);
}).listen(8233, () => log('temporal-ui', 'proxy listening', { port: 8233, target: TARGET.href }));
