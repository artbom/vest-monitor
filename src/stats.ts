// Throughput counters: published to Redis every 2 s for the UI, logged every 10 s.
import type { Redis } from 'ioredis';
import { channels, log } from './shared.ts';

// Latency samples of one 2-second window -> percentile. Empty window = no value.
export class Latency {
  private samples: number[] = [];
  add(ms: number) { this.samples.push(ms); }
  take(p: number): number | null {
    if (!this.samples.length) return null;
    const s = this.samples.sort((a, b) => a - b);
    this.samples = [];
    return Math.round(s[Math.min(s.length - 1, Math.floor(s.length * p))]);
  }
}

// read() returns counters for the window; counters become per-second rates, keys ending in _ms or _now pass through unchanged.
export function reportStats(redis: Redis, service: string, read: () => Record<string, number | null>): void {
  let last = Date.now();
  let window: Record<string, number> = {};
  setInterval(() => {
    const now = Date.now(), s = (now - last) / 1000;
    const snapshot = read();
    for (const [k, v] of Object.entries(snapshot)) if (v != null && !k.endsWith('_ms') && !k.endsWith('_now')) window[k] = (window[k] ?? 0) + v;
    const rates = Object.fromEntries(Object.entries(snapshot).map(([k, v]) =>
      [k, v == null || k.endsWith('_ms') || k.endsWith('_now') ? v : +(v / s).toFixed(1)]));
    redis.publish(channels.stats, JSON.stringify({ service, at: now, ...rates })).catch(() => {});
    last = now;
  }, 2000);
  setInterval(() => { log(service, 'stats (10 s totals)', window); window = {}; }, 10_000);
}
