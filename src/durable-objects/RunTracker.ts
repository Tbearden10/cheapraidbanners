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
    console.log('[RunTracker] Instance created');
  }

  async fetch(req: Request): Promise<Response> {
    const url = new URL(req.url);
    const path = url.pathname;

    console.log(`[RunTracker] Request: ${req.method} ${path}`);

    try {
      if (path === '/init' && req.method === 'POST') {
        const body = await req.json().catch(() => ({} as any));
        const runId = String(body.runId);
        const clanId = String(body.clanId);
        const expectedCount = Number(body.expectedCount) || 0;
        const key = `${RUN_KEY_PREFIX}${runId}`;

        console.log(`[RunTracker:Init] Starting initialization`);
        console.log(`[RunTracker:Init] - Run ID: ${runId}`);
        console.log(`[RunTracker:Init] - Clan ID: ${clanId}`);
        console.log(`[RunTracker:Init] - Expected Count: ${expectedCount}`);

        await this.state.blockConcurrencyWhile?.(async () => {
          let info = (await this.state.storage.get(key)) as RunInfo | undefined;
          
          if (!info) {
            console.log(`[RunTracker:Init] Creating new run info`);
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
            console.log(`[RunTracker:Init] Run already exists, updating expected count`);
            console.log(`[RunTracker:Init] - Previous expected: ${info.expectedCount}`);
            console.log(`[RunTracker:Init] - Previous completed: ${info.completedCount}`);
            info.expectedCount = expectedCount || info.expectedCount;
          }
          
          await this.state.storage.put(key, info);
          console.log(`[RunTracker:Init] ✓ Run info saved`);

          // maintain run index for alarm cleanup
          let idx = (await this.state.storage.get(RUN_INDEX_KEY)) as Array<{ runId: string; createdAt: number }> | undefined;
          if (!idx) {
            console.log(`[RunTracker:Init] Creating new run index`);
            idx = [];
          }
          
          if (!idx.find((r) => r.runId === runId)) {
            idx.push({ runId, createdAt: info.createdAt });
            await this.state.storage.put(RUN_INDEX_KEY, idx);
            console.log(`[RunTracker:Init] ✓ Run added to index (total runs: ${idx.length})`);
          } else {
            console.log(`[RunTracker:Init] Run already in index`);
          }

          const alarmTime = Date.now() + ALARM_TTL_MS;
          await this.state.storage.setAlarm(alarmTime);
          console.log(`[RunTracker:Init] ✓ Alarm set for ${new Date(alarmTime).toISOString()}`);
        });

        console.log(`[RunTracker:Init] ✅ Initialization complete for run ${runId}`);
        return new Response(JSON.stringify({ ok: true }), { headers: { 'Content-Type': 'application/json' } });
      }

      if (path === '/complete' && req.method === 'POST') {
        const body = await req.json().catch(() => ({} as any));
        const runId = String(body.runId);
        const membershipId = String(body.membershipId);
        const key = `${RUN_KEY_PREFIX}${runId}`;

        console.log(`[RunTracker:Complete] Processing completion`);
        console.log(`[RunTracker:Complete] - Run ID: ${runId}`);
        console.log(`[RunTracker:Complete] - Membership ID: ${membershipId}`);

        let doRecompute = false;
        let clanId = '';

        await this.state.blockConcurrencyWhile?.(async () => {
          const info = (await this.state.storage.get(key)) as RunInfo | undefined;
          
          if (!info) {
            console.log(`[RunTracker:Complete] ⚠️  Run not found: ${runId}`);
            return;
          }
          
          if (info.done) {
            console.log(`[RunTracker:Complete] ⚠️  Run already completed: ${runId}`);
            return;
          }
          
          clanId = info.clanId;

          if (info.seen[membershipId]) {
            console.log(`[RunTracker:Complete] ⚠️  Member already recorded: ${membershipId}`);
            return;
          }

          info.seen[membershipId] = true;
          info.completedCount = (info.completedCount || 0) + 1;
          
          console.log(`[RunTracker:Complete] ✓ Member recorded`);
          console.log(`[RunTracker:Complete] - Progress: ${info.completedCount}/${info.expectedCount}`);
          console.log(`[RunTracker:Complete] - Percentage: ${((info.completedCount / info.expectedCount) * 100).toFixed(1)}%`);
          
          await this.state.storage.put(key, info);
          
          const alarmTime = Date.now() + ALARM_TTL_MS;
          await this.state.storage.setAlarm(alarmTime);
          console.log(`[RunTracker:Complete] ✓ Alarm extended to ${new Date(alarmTime).toISOString()}`);

          if (info.completedCount >= (info.expectedCount || 0)) {
            console.log(`[RunTracker:Complete] 🎉 All members complete! Marking run as done`);
            info.done = true;
            await this.state.storage.put(key, info);
            doRecompute = true;
          }
        });

        if (doRecompute) {
          try {
            console.log(`[RunTracker:Complete] 🔄 Starting clan aggregate recomputation`);
            console.log(`[RunTracker:Complete] - Run ID: ${runId}`);
            console.log(`[RunTracker:Complete] - Clan ID: ${clanId}`);
            
            const recomputeStart = Date.now();
            await recomputeClanAggregateStats(this.env.DB, clanId);
            const recomputeDuration = Date.now() - recomputeStart;
            
            console.log(`[RunTracker:Complete] ✅ Recompute finished for clan ${clanId}`);
            console.log(`[RunTracker:Complete] - Duration: ${recomputeDuration}ms (${(recomputeDuration / 1000).toFixed(2)}s)`);
          } catch (err) {
            console.error(`[RunTracker:Complete] ❌ Recompute failed:`, err);
          }
        }

        return new Response(JSON.stringify({ ok: true }), { headers: { 'Content-Type': 'application/json' } });
      }

      // POST /reset - Clear all runs and reset state
      if (path === '/reset' && req.method === 'POST') {
        console.log(`[RunTracker:Reset] Clearing all runs and state`);
        
        let clearedCount = 0;
        
        await this.state.blockConcurrencyWhile?.(async () => {
          // Get the run index to know what to delete
          const idx = (await this.state.storage.get(RUN_INDEX_KEY)) as Array<{ runId: string; createdAt: number }> | undefined;
          
          if (idx && idx.length > 0) {
            // Delete all run entries
            for (const entry of idx) {
              try {
                await this.state.storage.delete(`${RUN_KEY_PREFIX}${entry.runId}`);
                clearedCount++;
              } catch (e) {
                console.warn(`[RunTracker:Reset] Failed to delete run ${entry.runId}:`, e);
              }
            }
          }
          
          // Delete the index itself
          await this.state.storage.delete(RUN_INDEX_KEY);
          
          // Cancel any pending alarms
          try {
            await this.state.storage.deleteAlarm?.();
          } catch (e) {
            // deleteAlarm might not exist in all environments, ignore
          }
        });
        
        console.log(`[RunTracker:Reset] ✅ Cleared ${clearedCount} run(s) and reset state`);
        return new Response(JSON.stringify({ ok: true, cleared: clearedCount }), { headers: { 'Content-Type': 'application/json' } });
      }

      console.log(`[RunTracker] ❌ 404 Not Found: ${path}`);
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
    console.log(`[RunTracker:Alarm] Alarm triggered - starting cleanup`);
    
    try {
      const idx = (await this.state.storage.get(RUN_INDEX_KEY)) as Array<{ runId: string; createdAt: number }> | undefined;
      
      if (!idx || idx.length === 0) {
        console.log(`[RunTracker:Alarm] No runs to clean up`);
        return;
      }
      
      console.log(`[RunTracker:Alarm] Found ${idx.length} run(s) in index`);
      
      const now = Date.now();
      const remaining: Array<{ runId: string; createdAt: number }> = [];
      let deletedCount = 0;
      
      for (const entry of idx) {
        const age = now - (entry.createdAt || 0);
        
        if (age > ALARM_TTL_MS) {
          console.log(`[RunTracker:Alarm] Deleting stale run: ${entry.runId} (age: ${(age / 1000 / 60).toFixed(1)} minutes)`);
          try {
            await this.state.storage.delete(`${RUN_KEY_PREFIX}${entry.runId}`);
            deletedCount++;
          } catch (e) {
            console.warn(`[RunTracker:Alarm] Failed to delete run ${entry.runId}:`, e);
          }
        } else {
          console.log(`[RunTracker:Alarm] Keeping run: ${entry.runId} (age: ${(age / 1000 / 60).toFixed(1)} minutes)`);
          remaining.push(entry);
        }
      }
      
      if (remaining.length === 0) {
        console.log(`[RunTracker:Alarm] All runs cleaned, deleting index`);
        await this.state.storage.delete(RUN_INDEX_KEY);
      } else {
        console.log(`[RunTracker:Alarm] Updating index with ${remaining.length} remaining run(s)`);
        await this.state.storage.put(RUN_INDEX_KEY, remaining);
      }
      
      console.log(`[RunTracker:Alarm] ✅ Cleanup complete: deleted ${deletedCount}, kept ${remaining.length}`);
      
    } catch (e) {
      console.error(`[RunTracker:Alarm] ❌ Cleanup error:`, e);
    }
  }
}