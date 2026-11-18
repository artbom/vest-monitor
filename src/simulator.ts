// Virtual vests. Every simulated second each vest takes one sample; every 20 samples it publishes a SeriesPacket (QoS 1).
// Several simulators can run at once (this laptop, a Raspberry Pi on the same network): each has a name and its own
// config in Redis, sim:<name>:config {vests, speed, network}. Scenarios are per vest and shared.
// Env: SIM_NAME (laptop), ID_OFFSET (0: ids start at VST-0001), VESTS (50), SPEED (1 = real time).
import http from 'node:http';
import os from 'node:os';
import mqtt from 'mqtt';
import { Redis } from 'ioredis';
import { channels, config, encodePacket, env, keys, log, SCENARIOS, type Sample, type Scenario } from './shared.ts';
import { reportStats } from './stats.ts';

const PACKET_S = 20;
const OFFLINE_BUFFER = 1000;                       // packets a vest keeps while it has no connection
const NAME = env('SIM_NAME', os.hostname());
const ID_OFFSET = Number(env('ID_OFFSET', '0'));
const CONTROL_PORT = Number(env('CONTROL_PORT', '8080'));
const CONTROL_URL = env('CONTROL_URL', `http://${os.hostname()}.local:${CONTROL_PORT}`);

interface Vest { id: string; seq: number; phase: number; samples: Sample[]; outbox: Uint8Array[]; scenario: Scenario; s: Sample }

const redis = new Redis(config.redisUrl);
const sub = new Redis(config.redisUrl);
// One MQTT connection for all vests: thousands of sockets from one laptop would only test the laptop.
const client = mqtt.connect(config.mqttUrl, { clientId: `sim-${NAME}`, reconnectPeriod: 1000, connectTimeout: 3000 });
client.on('error', e => log('simulator', 'mqtt error', { error: e.message }));

let clock = Math.floor(Date.now() / 1000);
let speed = 1;
let networkUp = true;                              // "network: down" in the UI = every vest of this simulator loses LTE
let timer: NodeJS.Timeout | undefined;
const vests: Vest[] = [];
const counters = { packets: 0, bytes: 0, flushed: 0, lost: 0 };

const clamp = (v: number, lo: number, hi: number) => Math.min(hi, Math.max(lo, v));
const walk = (v: number, step: number, lo: number, hi: number) => clamp(v + (Math.random() - 0.5) * step, lo, hi);
const deviceId = (i: number) => `VST-${String(ID_OFFSET + i + 1).padStart(4, '0')}`;

function newVest(i: number): Vest {
  return {
    id: deviceId(i), seq: 0, phase: i % PACKET_S, samples: [], outbox: [], scenario: 'normal',
    s: { heart_rate: 80 + Math.random() * 20, resp_rate: 22, temperature: 38.5, activity: 2, steps: Math.floor(Math.random() * 3000), battery: 60 + Math.floor(Math.random() * 40) },
  };
}

function sample(v: Vest, t: number): Sample {
  const s = v.s, sc = v.scenario;
  s.activity = Math.round(walk(s.activity, 1.2, 0, 9));
  s.heart_rate = sc === 'tachycardia' ? walk(Math.max(s.heart_rate + 4, 225), 6, 225, 250)
    : walk(s.heart_rate + (75 + s.activity * 8 - s.heart_rate) * 0.05, 4, 50, 150);
  s.resp_rate = walk(s.resp_rate + (18 + s.activity * 2 - s.resp_rate) * 0.05, 1.5, 10, 60);
  s.temperature = sc === 'fever' ? Math.min(41.5, s.temperature + 0.03) : walk(s.temperature + (38.5 - s.temperature) * 0.02, 0.02, 37.5, 41.5);
  s.steps += s.activity > 3 ? Math.round(Math.random() * 2) : 0;
  if (sc === 'low_battery') s.battery = Math.min(s.battery, 9);
  else if (t % 300 === 0) s.battery = s.battery <= 10 ? 100 : s.battery - 1;   // -1% every 5 min, "recharged" at 10%
  return { heart_rate: Math.round(s.heart_rate), resp_rate: Math.round(s.resp_rate), temperature: +s.temperature.toFixed(1),
    activity: s.activity, steps: s.steps, battery: s.battery };
}

const online = (v: Vest) => networkUp && client.connected && v.scenario !== 'offline';

function send(v: Vest, payload: Uint8Array) {
  client.publish(`vests/${v.id}/telemetry`, Buffer.from(payload), { qos: 1 });
  counters.packets++; counters.bytes += payload.length;
}

function keep(v: Vest, payload: Uint8Array) {
  v.outbox.push(payload);
  if (v.outbox.length > OFFLINE_BUFFER) { v.outbox.shift(); counters.lost++; }   // buffer full: the oldest packet is lost
}

// Back online: every vest sends its whole buffer at once. This is the burst the system has to absorb.
function flushAll() {
  for (const v of vests) {
    if (!online(v) || !v.outbox.length) continue;
    counters.flushed += v.outbox.length;
    for (const p of v.outbox) send(v, p);
    v.outbox = [];
  }
}

function tick() {
  const t = clock++;
  for (const v of vests) {
    v.samples.push(sample(v, t));
    if (v.samples.length < PACKET_S) continue;
    const payload = encodePacket({ deviceId: v.id, seq: ++v.seq, startUtc: t - PACKET_S + 1, samples: v.samples });
    v.samples = [];
    if (online(v)) send(v, payload); else keep(v, payload);
  }
}

function resize(n: number) {
  while (vests.length < n) {
    const v = newVest(vests.length);
    for (let k = 0; k < v.phase; k++) v.samples.push(sample(v, clock - v.phase + k));   // spread publish seconds over 20 s
    vests.push(v);
  }
  for (const v of vests.slice(n)) counters.lost += v.outbox.length;   // removed vests take their buffers with them
  vests.length = n;
}

async function applyConfig() {
  const cfg = await redis.hgetall(keys.simConfig(NAME));
  const scenarios = await redis.hgetall(keys.scenarios);
  // No config in Redis (first start, or Redis restarted empty): keep what we run now, env only on the first start.
  resize(clamp(+(cfg.vests ?? (timer ? vests.length : env('VESTS', '50'))), 0, 20000));
  const newSpeed = clamp(+(cfg.speed ?? (timer ? speed : env('SPEED', '1'))), 0.1, 100);
  networkUp = cfg.network !== 'down';
  for (const v of vests) v.scenario = SCENARIOS.includes(scenarios[v.id] as Scenario) ? scenarios[v.id] as Scenario : 'normal';
  flushAll();
  if (newSpeed !== speed || !timer) {
    speed = newSpeed;
    clearInterval(timer);
    // Simulated time follows the wall clock (x speed) instead of counting timer ticks, which drift behind under load.
    const baseReal = Date.now(), baseClock = clock;
    timer = setInterval(() => { const target = baseClock + Math.floor((Date.now() - baseReal) / 1000 * speed); while (clock < target) tick(); }, 50);
  }
  log('simulator', 'config applied', { name: NAME, vests: vests.length, speed, networkUp });
}

client.on('connect', () => { if (timer) flushAll(); });   // a real reconnect (e.g. the Pi got its Wi-Fi back) also flushes
await applyConfig();
await sub.subscribe(channels.simControl);
sub.on('message', () => { applyConfig().catch(e => log('simulator', 'config error', { error: e.message })); });

// Status for the UI. Expires, so a stopped simulator disappears from the dashboard.
setInterval(() => {
  const buffered = vests.reduce((a, v) => a + v.outbox.length, 0);
  redis.multi()
    .hset(keys.simStatus(NAME), {
      vests: vests.length, speed, network: networkUp ? 'up' : 'down', connected: client.connected ? 1 : 0,
      simulated: new Date(clock * 1000).toISOString(), buffered, first: deviceId(0), host: os.hostname(),
      offline: vests.filter(v => !online(v)).length, control: CONTROL_URL,
    })
    .expire(keys.simStatus(NAME), 5)
    .exec().catch(() => {});
}, 1000);

reportStats(redis, `sim:${NAME}`, () => {
  const c = { ...counters, buffered_now: vests.reduce((a, v) => a + v.outbox.length, 0) };
  counters.packets = counters.bytes = counters.flushed = counters.lost = 0;
  return c;
});

// ---- The fleet's own control panel: the device count is set where the vests "live" (the Raspberry Pi),
// the laptop dashboard only watches. Open http://<this machine>:8080 (CONTROL_PORT) on the Pi or from a phone.
async function setConfig(patch: Record<string, string | number>) {
  await redis.hset(keys.simConfig(NAME), patch);
  await redis.publish(channels.simControl, NAME);
}
const PAGE = `<!doctype html><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Парк жилетів</title>
<style>body{margin:0;font:16px/1.4 system-ui,sans-serif;background:#0d1117;color:#e8eef6;display:grid;place-items:center;min-height:100vh}
main{width:min(92vw,440px);display:grid;gap:18px}h1{font-size:20px;margin:0}.muted{color:#7c8494;font-size:14px}
.big{font:600 44px/1 ui-monospace,Menlo,monospace}.row{display:flex;gap:8px;flex-wrap:wrap}
button{flex:1;padding:12px;border-radius:10px;border:1px solid #2a3442;background:#19202a;color:#e8eef6;font:600 15px system-ui;cursor:pointer}
button.on{background:#244a7a;border-color:#6aa8e8}button.bad{background:#4a1f27;border-color:#e0606f}
input[type=range]{width:100%;accent-color:#6aa8e8}dl{display:grid;grid-template-columns:auto 1fr;gap:4px 14px;margin:0}dt{color:#7c8494}dd{margin:0;font-family:ui-monospace,Menlo,monospace}</style>
<main><h1>Парк жилетів · <span id="name"></span></h1>
<div><div class="muted">жилетів</div><div class="big" id="count">—</div><input id="slider" type="range" min="0" max="100"></div>
<div class="row" id="presets"></div><div class="row" id="speeds"></div><div class="row"><button id="net"></button></div>
<dl><dt>пакетів/с</dt><dd id="rate"></dd><dt>у буферах</dt><dd id="buf"></dd><dt>брокер MQTT</dt><dd id="mqtt"></dd></dl>
<div class="muted">Решта системи — на дашборді ноутбука.</div></main>
<script>
const $=id=>document.getElementById(id), LOG=Math.log10(20001), post=b=>fetch('/config',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(b)}).then(load);
const toPos=v=>Math.round(100*Math.log10(1+v)/LOG), fromPos=p=>Math.round(10**(p/100*LOG)-1);
let dragging=false; $('slider').oninput=()=>{dragging=true;$('count').textContent=fromPos(+$('slider').value).toLocaleString('uk-UA')};
$('slider').onchange=()=>{dragging=false;post({vests:fromPos(+$('slider').value)})};
async function load(){const s=await fetch('/status').then(r=>r.json());$('name').textContent=s.name;
 if(!dragging){$('count').textContent=s.vests.toLocaleString('uk-UA');$('slider').value=toPos(s.vests)}
 $('presets').replaceChildren(...[50,1000,5000,10000,20000].map(v=>{const b=document.createElement('button');b.textContent=v>=1000?v/1000+'k':v;b.className=v===s.vests?'on':'';b.onclick=()=>post({vests:v});return b}));
 $('speeds').replaceChildren(...[1,5,20].map(v=>{const b=document.createElement('button');b.textContent='час ×'+v;b.className=v===s.speed?'on':'';b.onclick=()=>post({speed:v});return b}));
 $('net').textContent=s.network==='up'?'📶 мережа є — вимкнути':'📵 мережі немає — увімкнути';$('net').className=s.network==='up'?'':'bad';$('net').onclick=()=>post({network:s.network==='up'?'down':'up'});
 $('rate').textContent=Math.round(s.vests*s.speed/20)+' (очікувано)';$('buf').textContent=s.buffered.toLocaleString('uk-UA');$('mqtt').textContent=s.connected?'підключено':'немає звʼязку'}
load();setInterval(load,2000);
</script>`;
http.createServer(async (req, res) => {
  try {
    if (req.method === 'GET' && req.url === '/status') {
      return res.end(JSON.stringify({ name: NAME, vests: vests.length, speed, network: networkUp ? 'up' : 'down', connected: client.connected,
        buffered: vests.reduce((a, v) => a + v.outbox.length, 0) }));
    }
    if (req.method === 'POST' && req.url === '/config') {
      let body = '';
      for await (const chunk of req) body += chunk;
      const b = JSON.parse(body || '{}');
      const patch: Record<string, string | number> = {};
      if (Number.isInteger(b.vests) && b.vests >= 0 && b.vests <= 20000) patch.vests = b.vests;
      if ([1, 5, 20].includes(b.speed)) patch.speed = b.speed;
      if (b.network === 'up' || b.network === 'down') patch.network = b.network;
      if (Object.keys(patch).length) await setConfig(patch);
      return res.end('{}');
    }
    res.setHeader('content-type', 'text/html; charset=utf-8');
    res.end(PAGE);
  } catch (e) { res.statusCode = 400; res.end(JSON.stringify({ error: (e as Error).message })); }
}).listen(CONTROL_PORT, () => log('simulator', 'control panel', { url: CONTROL_URL }));
