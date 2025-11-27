// Durable Object: per-member coordinator (with extensive logging)
// - Persists run state to Durable Object storage so state survives GC/restarts
// - Dedupes batches persistently (seenIndices per dungeon)
// - Maintains runIndex for alarm cleanup
// - Uses blockConcurrencyWhile for atomic updates when available
// - Adds verbose logging for debugging

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
  private idStr: string;

  constructor(state: DurableObjectState, _env: any) {
    this.state = state;
    // try to get a stable id string for logs
    try {
      // state.id may be a DurableObjectId with toString()
      // fallback to JSON if needed
      // @ts-ignore
      this.idStr = state.id && state.id.toString ? state.id.toString() : JSON.stringify(state.id);
    } catch {
      this.idStr = 'unknown-do-id';
    }
  }

  private now() {
    return new Date().toISOString();
  }

  private log(...args: any[]) {
    console.log(`[BatchCoordinator:${this.idStr}]`, this.now(), '-', ...args);
  }

  private warn(...args: any[]) {
    console.warn(`[BatchCoordinator:${this.idStr}]`, this.now(), '- WARN -', ...args);
  }

  private error(...args: any[]) {
    console.error(`[BatchCoordinator:${this.idStr}]`, this.now(), '- ERROR -', ...args);
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

        this.log('INIT request received', { jobId, runId, dungeonHash, totalBatches });

        await this.state.blockConcurrencyWhile?.(async () => {
          try {
            const existing = (await this.state.storage.get(runKey)) as RunMeta | undefined;
            this.log('INIT: existing run meta (before)', { runKey, existing: existing ? 'present' : 'none' });

            let meta = existing;
            if (!meta) {
              meta = { jobId, runId, dungeons: {}, createdAt: Date.now() };
              this.log('INIT: creating new run meta', { runKey, createdAt: meta.createdAt });
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
              this.log('INIT: created dungeon entry', { runKey, dungeonHash, totalBatches });
            } else {
              // update totalBatches (support re-init)
              const prev = meta.dungeons[dungeonHash].totalBatches;
              meta.dungeons[dungeonHash].totalBatches = totalBatches;
              this.log('INIT: updated dungeon totalBatches', { runKey, dungeonHash, prev, totalBatches });
            }

            await this.state.storage.put(runKey, meta);
            this.log('INIT: persisted run meta', { runKey });

            // maintain runIndex
            let runIndex = (await this.state.storage.get(RUN_INDEX_KEY)) as Array<{ runId: string; createdAt: number }> | undefined;
            this.log('INIT: runIndex (before)', { runIndexCount: runIndex ? runIndex.length : 0 });
            if (!runIndex) runIndex = [];
            if (!runIndex.find((r) => r.runId === runId)) {
              runIndex.push({ runId, createdAt: meta.createdAt });
              await this.state.storage.put(RUN_INDEX_KEY, runIndex);
              this.log('INIT: added run to runIndex', { runId, runIndexCount: runIndex.length });
            } else {
              this.log('INIT: run already present in runIndex', { runId });
            }

            // refresh alarm
            const alarmTime = Date.now() + this.alarmTTL;
            await this.state.storage.setAlarm(alarmTime);
            this.log('INIT: alarm refreshed until', { alarmTimeISO: new Date(alarmTime).toISOString(), alarmTTLms: this.alarmTTL });
          } catch (e) {
            this.error('INIT: error inside blockConcurrencyWhile', e);
            throw e;
          }
        });

        this.log(`INIT completed for run=${runId} dungeon=${dungeonHash} totalBatches=${totalBatches}`);
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

        this.log('BATCH received', { runId, dungeonHash, batchIndex, fullClears, playtimeSeconds, lastActivityDate });

        let accepted = false;
        await this.state.blockConcurrencyWhile?.(async () => {
          try {
            const meta = (await this.state.storage.get(runKey)) as RunMeta | undefined;
            if (!meta) {
              this.warn('BATCH: no run meta found for', { runKey });
              return;
            }
            const dungeon = meta.dungeons[dungeonHash];
            if (!dungeon) {
              this.warn('BATCH: dungeon not found on run meta', { runKey, dungeonHash });
              return;
            }

            // Log current dungeon state before applying
            this.log('BATCH: current dungeon state (before apply)', {
              runKey,
              dungeonHash,
              totalBatches: dungeon.totalBatches,
              received: dungeon.received,
              fullClears: dungeon.fullClears,
              playtimeSeconds: dungeon.playtimeSeconds,
              lastActivityDate: dungeon.lastActivityDate,
              seenIndicesCount: dungeon.seenIndices.length,
              seenIndicesSample: dungeon.seenIndices.slice(-5),
            });

            // Dedupe: ignore if seen
            if (Number.isFinite(batchIndex) && dungeon.seenIndices.includes(batchIndex)) {
              accepted = true; // duplicate but OK
              this.log('BATCH: duplicate index ignored', { runKey, dungeonHash, batchIndex });
              return;
            }

            // Accept the batch
            dungeon.seenIndices.push(batchIndex);
            dungeon.received = (dungeon.received || 0) + 1;
            dungeon.fullClears = (dungeon.fullClears || 0) + fullClears;
            dungeon.playtimeSeconds = (dungeon.playtimeSeconds || 0) + playtimeSeconds;
            if (lastActivityDate && (!dungeon.lastActivityDate || lastActivityDate > dungeon.lastActivityDate)) {
              const prevLast = dungeon.lastActivityDate;
              dungeon.lastActivityDate = lastActivityDate;
              this.log('BATCH: updated lastActivityDate', { runKey, dungeonHash, prevLast, newLast: lastActivityDate });
            }

            await this.state.storage.put(runKey, meta);
            this.log('BATCH: persisted updated dungeon meta', { runKey, dungeonHash, received: dungeon.received, totalBatches: dungeon.totalBatches, seenIndicesCount: dungeon.seenIndices.length });

            // refresh alarm
            const alarmTime = Date.now() + this.alarmTTL;
            await this.state.storage.setAlarm(alarmTime);
            this.log('BATCH: alarm refreshed after batch', { alarmTimeISO: new Date(alarmTime).toISOString() });

            accepted = true;
          } catch (e) {
            this.error('BATCH: error inside blockConcurrencyWhile', e);
            throw e;
          }
        });

        if (!accepted) {
          this.warn('BATCH: rejected (unknown run or dungeon)', { runKey });
          return new Response(JSON.stringify({ error: 'Unknown run or dungeon' }), { status: 400, headers: { 'Content-Type': 'application/json' } });
        }

        this.log('BATCH: accepted', { runId, dungeonHash, batchIndex });
        return new Response(JSON.stringify({ ok: true }), { headers: { 'Content-Type': 'application/json' } });
      }

      // GET /result?runId=...&dungeonHash=...
      if (path === '/result' && request.method === 'GET') {
        const runId = String(url.searchParams.get('runId') ?? '');
        const dungeonHash = String(url.searchParams.get('dungeonHash') ?? '');
        if (!runId || !dungeonHash) {
          this.warn('RESULT: missing params', { runId, dungeonHash });
          return new Response(JSON.stringify({ error: 'runId and dungeonHash required' }), { status: 400, headers: { 'Content-Type': 'application/json' } });
        }
        const runKey = `${RUN_KEY_PREFIX}${runId}`;

        this.log('RESULT: request', { runId, dungeonHash, runKey });

        // Wait loop with periodic logging so callers can see progress in logs
        const maxWait = 30_000;
        const start = Date.now();
        let loopCount = 0;
        while (true) {
          loopCount++;
          try {
            const meta = (await this.state.storage.get(runKey)) as RunMeta | undefined;
            if (!meta) {
              this.warn('RESULT: run not found', { runKey, elapsedMs: Date.now() - start, loopCount });
              return new Response(JSON.stringify({ error: 'Run not found' }), { status: 404, headers: { 'Content-Type': 'application/json' } });
            }
            const dungeon = meta.dungeons[dungeonHash];
            if (!dungeon) {
              this.warn('RESULT: dungeon not found for run', { runKey, dungeonHash, elapsedMs: Date.now() - start, loopCount });
              return new Response(JSON.stringify({ error: 'Dungeon not found for run' }), { status: 404, headers: { 'Content-Type': 'application/json' } });
            }

            // Log progress periodically (every few loops)
            if (loopCount === 1 || loopCount % 10 === 0) {
              this.log('RESULT: polling progress', {
                runKey,
                dungeonHash,
                loopCount,
                elapsedMs: Date.now() - start,
                received: dungeon.received,
                totalBatches: dungeon.totalBatches,
                seenIndicesCount: dungeon.seenIndices.length,
                lastActivityDate: dungeon.lastActivityDate,
              });
            }

            if (dungeon.received >= dungeon.totalBatches) {
              const aggregated = {
                fullClears: dungeon.fullClears,
                playtimeSeconds: dungeon.playtimeSeconds,
                lastActivityDate: dungeon.lastActivityDate,
              };

              this.log('RESULT: all batches received, returning aggregated result', { runKey, dungeonHash, aggregated });

              // Remove dungeon from run meta; if run has no dungeons remove run and update runIndex
              await this.state.blockConcurrencyWhile?.(async () => {
                try {
                  const fresh = (await this.state.storage.get(runKey)) as RunMeta | undefined;
                  if (!fresh) {
                    this.warn('RESULT: fresh meta missing unexpectedly', { runKey });
                    return;
                  }
                  delete fresh.dungeons[dungeonHash];
                  if (Object.keys(fresh.dungeons).length === 0) {
                    // remove run key
                    await this.state.storage.delete(runKey);
                    this.log('RESULT: removed runKey as last dungeon cleared', { runKey });

                    // update runIndex
                    const runIndex = (await this.state.storage.get(RUN_INDEX_KEY)) as Array<{ runId: string; createdAt: number }> | undefined;
                    if (runIndex) {
                      const updated = runIndex.filter((r) => r.runId !== runId);
                      if (updated.length === 0) {
                        await this.state.storage.delete(RUN_INDEX_KEY);
                        this.log('RESULT: runIndex is now empty and deleted');
                      } else {
                        await this.state.storage.put(RUN_INDEX_KEY, updated);
                        this.log('RESULT: updated runIndex', { newCount: updated.length });
                      }
                    }
                  } else {
                    await this.state.storage.put(runKey, fresh);
                    this.log('RESULT: updated run meta after removing dungeon', { runKey, remainingDungeons: Object.keys(fresh.dungeons).length });
                  }
                } catch (e) {
                  this.error('RESULT: error in blockConcurrencyWhile cleanup', e);
                }
              });

              return new Response(JSON.stringify(aggregated), { headers: { 'Content-Type': 'application/json' } });
            }

            if (Date.now() - start > maxWait) {
              this.warn('RESULT: maxWait exceeded, returning timeout response', { runKey, dungeonHash, received: dungeon.received, total: dungeon.totalBatches, elapsedMs: Date.now() - start });
              return new Response(JSON.stringify({ error: 'Timeout waiting for batches', received: dungeon.received, total: dungeon.totalBatches }), { status: 408, headers: { 'Content-Type': 'application/json' } });
            }

            // Sleep briefly before next check (small to allow fast completion)
            await new Promise((r) => setTimeout(r, 100));
            continue;
          } catch (err) {
            this.error('RESULT: error while polling', err);
            // short backoff before retrying
            await new Promise((r) => setTimeout(r, 200));
          }
        }
      }

      this.log('FETCH: unknown path', { path });
      return new Response('Not Found', { status: 404 });
    } catch (err) {
      this.error('FETCH: top-level error', err);
      return new Response(JSON.stringify({ error: String(err) }), { status: 500, headers: { 'Content-Type': 'application/json' } });
    }
  }

  // Alarm: cleanup stale runs using the runIndex (no storage.list dependency)
  async alarm() {
    try {
      this.log('ALARM: triggered cleanup');
      const runIndex = (await this.state.storage.get(RUN_INDEX_KEY)) as Array<{ runId: string; createdAt: number }> | undefined;
      this.log('ALARM: runIndex read', { runIndexCount: runIndex ? runIndex.length : 0 });
      if (!runIndex || runIndex.length === 0) {
        this.log('ALARM: nothing to cleanup');
        return;
      }
      const now = Date.now();
      const TTL = this.alarmTTL;
      const remaining: Array<{ runId: string; createdAt: number }> = [];
      for (const entry of runIndex) {
        if (now - (entry.createdAt || 0) > TTL) {
          try {
            const key = `${RUN_KEY_PREFIX}${entry.runId}`;
            await this.state.storage.delete(key);
            this.log('ALARM: deleted stale run', { runId: entry.runId, key });
          } catch (e) {
            this.error('ALARM: error deleting stale run', { runId: entry.runId }, e);
          }
        } else {
          remaining.push(entry);
        }
      }
      if (remaining.length === 0) {
        await this.state.storage.delete(RUN_INDEX_KEY);
        this.log('ALARM: runIndex emptied and deleted');
      } else {
        await this.state.storage.put(RUN_INDEX_KEY, remaining);
        this.log('ALARM: runIndex updated with remaining entries', { remainingCount: remaining.length });
      }
    } catch (e) {
      this.error('ALARM: cleanup error', e);
    }
  }
}