// RunTracker Durable Object (per-clan)
// - Tracks a run initiated by statsSyncCron (runId, expectedCount)
// - Accepts /complete notifications per membershipId and dedupes them
// - When completedCount >= expectedCount it triggers recomputeClanAggregateStats
// - Uses persistent storage and runIndex for alarm cleanup

import type { DurableObjectState } from '../types';
import { recomputeClanAggregateStats } from '../db/aggregateHelpers';

type RunInfo = {
  runId: string;
  clanId: string;
  expectedCount: number;
  createdAt: number;
  completedCount: number;
  seen: Record<string, true>;
  done?: boolean;
};

const RUN_KEY_PREFIX = 'rt:';
const RUN_INDEX_KEY = 'rt_index';
const ALARM_TTL_MS = 10 * 60 * 1000; // 10 minutes

export class RunTracker {
  state: DurableObjectState;
  env: any;

  constructor(state: DurableObjectState, env: any) {
    this.state = state;
    this.env = env;
  }

  async fetch(req: Request): Promise<Response> {
    const url = new URL(req.url);
    const path = url.pathname;

    try {
      if (path === '/init' && req.method === 'POST') {
        const body = await req.json().catch(() => ({} as any));
        const runId = String(body.runId);
        const clanId = String(body.clanId);
        const expectedCount = Number(body.expectedCount) || 0;
        const key = `${RUN_KEY_PREFIX}${runId}`;

        await this.state.blockConcurrencyWhile?.(async () => {
          let info = (await this.state.storage.get(key)) as RunInfo | undefined;
          
          if (!info) {
            info = { 
              runId, 
              clanId, 
              expectedCount, 
              createdAt: Date.now(), 
              completedCount: 0, 
              seen: {}, 
              done: false 
            };
          } else {
            info.expectedCount = expectedCount || info.expectedCount;
          }
          
          await this.state.storage.put(key, info);

          // maintain run index for alarm cleanup
          let idx = (await this.state.storage.get(RUN_INDEX_KEY)) as Array<{ runId: string; createdAt: number }> | undefined;
          if (!idx) {
            idx = [];
          }
          
          if (!idx.find((r) => r.runId === runId)) {
            idx.push({ runId, createdAt: info.createdAt });
            await this.state.storage.put(RUN_INDEX_KEY, idx);
          }

          const alarmTime = Date.now() + ALARM_TTL_MS;
          await this.state.storage.setAlarm(alarmTime);
        });

        return new Response(JSON.stringify({ ok: true }), { headers: { 'Content-Type': 'application/json' } });
      }

      if (path === '/complete' && req.method === 'POST') {
        const body = await req.json().catch(() => ({} as any));
        const runId = String(body.runId);
        const membershipId = String(body.membershipId);
        const key = `${RUN_KEY_PREFIX}${runId}`;

        let doRecompute = false;
        let clanId = '';

        await this.state.blockConcurrencyWhile?.(async () => {
          const info = (await this.state.storage.get(key)) as RunInfo | undefined;
          
          if (!info || info.done || info.seen[membershipId]) {
            return;
          }
          
          clanId = info.clanId;
          info.seen[membershipId] = true;
          info.completedCount = (info.completedCount || 0) + 1;
          
          await this.state.storage.put(key, info);
          
          const alarmTime = Date.now() + ALARM_TTL_MS;
          await this.state.storage.setAlarm(alarmTime);

          if (info.completedCount >= (info.expectedCount || 0)) {
            console.log(`[RunTracker] All members complete for run ${runId}`);
            info.done = true;
            await this.state.storage.put(key, info);
            doRecompute = true;
          }
        });

        if (doRecompute) {
          try {
            await recomputeClanAggregateStats(this.env.DB, clanId);
            console.log(`[RunTracker] Recompute complete for clan ${clanId}`);
          } catch (err) {
            console.error(`[RunTracker] Recompute failed:`, err);
          }
        }

        return new Response(JSON.stringify({ ok: true }), { headers: { 'Content-Type': 'application/json' } });
      }

      return new Response('Not Found', { status: 404 });
      
    } catch (err) {
      console.error(`[RunTracker] ❌ Error:`, err);
      return new Response(JSON.stringify({ error: String(err) }), { 
        status: 500, 
        headers: { 'Content-Type': 'application/json' } 
      });
    }
  }

  async alarm() {
    try {
      const idx = (await this.state.storage.get(RUN_INDEX_KEY)) as Array<{ runId: string; createdAt: number }> | undefined;
      
      if (!idx || idx.length === 0) return;
      
      const now = Date.now();
      const remaining: Array<{ runId: string; createdAt: number }> = [];
      
      for (const entry of idx) {
        const age = now - (entry.createdAt || 0);
        
        if (age > ALARM_TTL_MS) {
          try {
            await this.state.storage.delete(`${RUN_KEY_PREFIX}${entry.runId}`);
          } catch {}
        } else {
          remaining.push(entry);
        }
      }
      
      if (remaining.length === 0) {
        await this.state.storage.delete(RUN_INDEX_KEY);
      } else {
        await this.state.storage.put(RUN_INDEX_KEY, remaining);
      }
    } catch (e) {
      console.error(`[RunTracker] Cleanup error:`, e);
    }
  }
}