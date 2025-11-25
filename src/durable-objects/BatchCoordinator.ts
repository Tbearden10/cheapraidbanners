// Durable Object: per-member coordinator
// - Persists run state to Durable Object storage so state survives GC/restarts
// - Dedupes batches persistently (seenIndices per dungeon)
// - Maintains runIndex for alarm cleanup
// - Uses blockConcurrencyWhile for atomic updates when available

import type { DurableObjectState } from '../types';

type DungeonMeta = {
  totalBatches: number;
  received: number;
  fullClears: number;
  playtimeSeconds: number;
  lastActivityDate: string | null;
  seenIndices: number[]; // small array ok for typical batch counts
};

type RunMeta = {
  jobId: string; // e.g. member-<id>
  runId: string;
  dungeons: Record<string, DungeonMeta>;
  createdAt: number;
};

const RUN_INDEX_KEY = 'runIndex';
const RUN_KEY_PREFIX = 'run:';

export class BatchCoordinator {
  private state: DurableObjectState;
  private alarmTTL = 10 * 60 * 1000; // 10 minutes

  constructor(state: DurableObjectState, _env: any) {
    this.state = state;
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const path = url.pathname;

    try {
      // POST /init { jobId, runId, dungeonHash, totalBatches }
      if (path === '/init' && request.method === 'POST') {
        const body = (await request.json()) as any;
        const jobId = String(body.jobId);
        const runId = String(body.runId);
        const dungeonHash = String(body.dungeonHash);
        const totalBatches = Number(body.totalBatches) || 0;
        const runKey = `${RUN_KEY_PREFIX}${runId}`;

        await this.state.blockConcurrencyWhile?.(async () => {
          let meta = (await this.state.storage.get(runKey)) as RunMeta | undefined;
          if (!meta) {
            meta = { jobId, runId, dungeons: {}, createdAt: Date.now() };
          } else {
            // ensure jobId recorded
            meta.jobId = jobId;
          }

          if (!meta.dungeons[dungeonHash]) {
            meta.dungeons[dungeonHash] = {
              totalBatches,
              received: 0,
              fullClears: 0,
              playtimeSeconds: 0,
              lastActivityDate: null,
              seenIndices: [],
            };
          } else {
            // update totalBatches (support re-init)
            meta.dungeons[dungeonHash].totalBatches = totalBatches;
          }

          await this.state.storage.put(runKey, meta);

          // maintain runIndex
          let runIndex = (await this.state.storage.get(RUN_INDEX_KEY)) as Array<{ runId: string; createdAt: number }> | undefined;
          if (!runIndex) runIndex = [];
          if (!runIndex.find((r) => r.runId === runId)) {
            runIndex.push({ runId, createdAt: meta.createdAt });
            await this.state.storage.put(RUN_INDEX_KEY, runIndex);
          }

          // refresh alarm
          await this.state.storage.setAlarm(Date.now() + this.alarmTTL);
        });

        console.log(`[BatchCoordinator] init job=${jobId} run=${runId} dungeon=${dungeonHash} batches=${totalBatches}`);
        return new Response(JSON.stringify({ ok: true }), { headers: { 'Content-Type': 'application/json' } });
      }

      // POST /batch { runId, dungeonHash, batchIndex, fullClears, playtimeSeconds, lastActivityDate }
      if (path === '/batch' && request.method === 'POST') {
        const body = (await request.json()) as any;
        const runId = String(body.runId);
        const dungeonHash = String(body.dungeonHash);
        const batchIndex = Number(body.batchIndex);
        const fullClears = Number(body.fullClears || 0);
        const playtimeSeconds = Number(body.playtimeSeconds || 0);
        const lastActivityDate = body.lastActivityDate ?? null;
        const runKey = `${RUN_KEY_PREFIX}${runId}`;

        let accepted = false;
        await this.state.blockConcurrencyWhile?.(async () => {
          const meta = (await this.state.storage.get(runKey)) as RunMeta | undefined;
          if (!meta) return;
          const dungeon = meta.dungeons[dungeonHash];
          if (!dungeon) return;

          // Dedupe: ignore if seen
          if (Number.isFinite(batchIndex) && dungeon.seenIndices.includes(batchIndex)) {
            accepted = true; // duplicate but OK
            return;
          }

          // Accept the batch
          dungeon.seenIndices.push(batchIndex);
          dungeon.received = (dungeon.received || 0) + 1;
          dungeon.fullClears = (dungeon.fullClears || 0) + fullClears;
          dungeon.playtimeSeconds = (dungeon.playtimeSeconds || 0) + playtimeSeconds;
          if (lastActivityDate && (!dungeon.lastActivityDate || lastActivityDate > dungeon.lastActivityDate)) {
            dungeon.lastActivityDate = lastActivityDate;
          }

          await this.state.storage.put(runKey, meta);
          // refresh alarm
          await this.state.storage.setAlarm(Date.now() + this.alarmTTL);
          accepted = true;
        });

        if (!accepted) {
          return new Response(JSON.stringify({ error: 'Unknown run or dungeon' }), { status: 400, headers: { 'Content-Type': 'application/json' } });
        }
        return new Response(JSON.stringify({ ok: true }), { headers: { 'Content-Type': 'application/json' } });
      }

      // GET /result?runId=...&dungeonHash=...
      if (path === '/result' && request.method === 'GET') {
        const runId = String(url.searchParams.get('runId') ?? '');
        const dungeonHash = String(url.searchParams.get('dungeonHash') ?? '');
        if (!runId || !dungeonHash) {
          return new Response(JSON.stringify({ error: 'runId and dungeonHash required' }), { status: 400, headers: { 'Content-Type': 'application/json' } });
        }
        const runKey = `${RUN_KEY_PREFIX}${runId}`;

        const maxWait = 30_000;
        const start = Date.now();
        while (true) {
          const meta = (await this.state.storage.get(runKey)) as RunMeta | undefined;
          if (!meta) {
            return new Response(JSON.stringify({ error: 'Run not found' }), { status: 404, headers: { 'Content-Type': 'application/json' } });
          }
          const dungeon = meta.dungeons[dungeonHash];
          if (!dungeon) {
            return new Response(JSON.stringify({ error: 'Dungeon not found for run' }), { status: 404, headers: { 'Content-Type': 'application/json' } });
          }

          if (dungeon.received >= dungeon.totalBatches) {
            const aggregated = {
              fullClears: dungeon.fullClears,
              playtimeSeconds: dungeon.playtimeSeconds,
              lastActivityDate: dungeon.lastActivityDate,
            };

            // Remove dungeon from run meta; if run has no dungeons remove run and update runIndex
            await this.state.blockConcurrencyWhile?.(async () => {
              const fresh = (await this.state.storage.get(runKey)) as RunMeta | undefined;
              if (!fresh) return;
              delete fresh.dungeons[dungeonHash];
              if (Object.keys(fresh.dungeons).length === 0) {
                // remove run key
                await this.state.storage.delete(runKey);
                // update runIndex
                const runIndex = (await this.state.storage.get(RUN_INDEX_KEY)) as Array<{ runId: string; createdAt: number }> | undefined;
                if (runIndex) {
                  const updated = runIndex.filter((r) => r.runId !== runId);
                  if (updated.length === 0) {
                    await this.state.storage.delete(RUN_INDEX_KEY);
                  } else {
                    await this.state.storage.put(RUN_INDEX_KEY, updated);
                  }
                }
              } else {
                await this.state.storage.put(runKey, fresh);
              }
            });

            return new Response(JSON.stringify(aggregated), { headers: { 'Content-Type': 'application/json' } });
          }

          if (Date.now() - start > maxWait) {
            return new Response(JSON.stringify({ error: 'Timeout waiting for batches', received: dungeon.received, total: dungeon.totalBatches }), { status: 408, headers: { 'Content-Type': 'application/json' } });
          }
          await new Promise((r) => setTimeout(r, 100));
        }
      }

      return new Response('Not Found', { status: 404 });
    } catch (err) {
      console.error('[BatchCoordinator] Error', err);
      return new Response(JSON.stringify({ error: String(err) }), { status: 500, headers: { 'Content-Type': 'application/json' } });
    }
  }

  // Alarm: cleanup stale runs using the runIndex (no storage.list dependency)
  async alarm() {
    try {
      const runIndex = (await this.state.storage.get(RUN_INDEX_KEY)) as Array<{ runId: string; createdAt: number }> | undefined;
      if (!runIndex || runIndex.length === 0) return;
      const now = Date.now();
      const TTL = this.alarmTTL;
      const remaining: Array<{ runId: string; createdAt: number }> = [];
      for (const entry of runIndex) {
        if (now - (entry.createdAt || 0) > TTL) {
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
      console.warn('[BatchCoordinator] alarm cleanup error', e);
    }
  }
}