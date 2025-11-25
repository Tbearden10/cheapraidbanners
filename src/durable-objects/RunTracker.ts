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
            info = { runId, clanId, expectedCount, createdAt: Date.now(), completedCount: 0, seen: {}, done: false };
          } else {
            info.expectedCount = expectedCount || info.expectedCount;
          }
          await this.state.storage.put(key, info);

          // maintain run index for alarm cleanup
          let idx = (await this.state.storage.get(RUN_INDEX_KEY)) as Array<{ runId: string; createdAt: number }> | undefined;
          if (!idx) idx = [];
          if (!idx.find((r) => r.runId === runId)) {
            idx.push({ runId, createdAt: info.createdAt });
            await this.state.storage.put(RUN_INDEX_KEY, idx);
          }

          await this.state.storage.setAlarm(Date.now() + ALARM_TTL_MS);
        });

        console.log(`[RunTracker] init run=${runId} clan=${clanId} expected=${expectedCount}`);
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
          if (!info || info.done) return;
          clanId = info.clanId;

          if (info.seen[membershipId]) {
            // already recorded
            return;
          }

          info.seen[membershipId] = true;
          info.completedCount = (info.completedCount || 0) + 1;
          await this.state.storage.put(key, info);
          await this.state.storage.setAlarm(Date.now() + ALARM_TTL_MS);

          if (info.completedCount >= (info.expectedCount || 0)) {
            info.done = true;
            await this.state.storage.put(key, info);
            doRecompute = true;
          }
        });

        if (doRecompute) {
          try {
            console.log(`[RunTracker] run ${runId} complete for clan ${clanId} — running recompute`);
            await recomputeClanAggregateStats(this.env.DB, clanId);
            console.log(`[RunTracker] recompute finished for clan ${clanId} run ${runId}`);
          } catch (err) {
            console.error('[RunTracker] recompute failed', err);
            // Optionally mark retry needed or log for alerting
          }
        }

        return new Response(JSON.stringify({ ok: true }), { headers: { 'Content-Type': 'application/json' } });
      }

      return new Response('Not Found', { status: 404 });
    } catch (err) {
      console.error('[RunTracker] Error', err);
      return new Response(JSON.stringify({ error: String(err) }), { status: 500, headers: { 'Content-Type': 'application/json' } });
    }
  }

  async alarm() {
    try {
      const idx = (await this.state.storage.get(RUN_INDEX_KEY)) as Array<{ runId: string; createdAt: number }> | undefined;
      if (!idx || idx.length === 0) return;
      const now = Date.now();
      const remaining: Array<{ runId: string; createdAt: number }> = [];
      for (const entry of idx) {
        if (now - (entry.createdAt || 0) > ALARM_TTL_MS) {
          try {
            await this.state.storage.delete(`${RUN_KEY_PREFIX}${entry.runId}`);
          } catch (e) {
            // ignore
          }
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
      console.warn('[RunTracker] alarm cleanup error', e);
    }
  }
}