/**
 * BatchCoordinator Durable Object (compatible with statsQueueProcessor)
 *
 * Purpose:
 * - Aggregates per-batch results coming from statsQueueProcessor.
 * - Exports the BatchResult type so processors can import it.
 * - Keeps internal aggregation small (totalActivities/totalFullClears/totalPlaytimeSeconds/latestActivityDate).
 *
 * Endpoints:
 * - POST /init  { membershipId, membershipType, dungeonHash, totalBatches }
 * - POST /batch { ...BatchResult }
 * - GET  /result
 */

import type { DurableObjectState } from '../types';

export interface BatchResult {
  batchIndex: number;
  activitiesCount: number;
  fullClearsFound: number;
  // Added: per-batch playtime and latestActivityDate so DO can aggregate properly
  playtimeSeconds?: number;
  latestActivityDate?: string | null;
}

export interface AggregatedResult {
  totalActivities: number;
  totalFullClears: number;
  totalPlaytimeSeconds?: number;
  latestActivityDate?: string | null;
}

type Meta = {
  membershipId: string;
  membershipType?: number;
  dungeonHash: string;
  totalBatches: number;
  createdAt: number;
};

type AggState = {
  receivedCount: number;
  totalActivities: number;
  totalFullClears: number;
  totalPlaytimeSeconds: number;
  latestActivityDate: string | null;
  seenIndices: number[];
};

const META_KEY = 'meta';
const AGG_KEY = 'agg';
const TTL_MS = 2 * 60 * 60 * 1000; // 2 hours

export class BatchCoordinator {
  private state: DurableObjectState;

  constructor(state: DurableObjectState, _env: any) {
    this.state = state;
    console.log('[BatchCoordinator] Instance created');
  }

  private now() {
    return Date.now();
  }

  async fetch(request: Request): Promise<Response> {
    try {
      const url = new URL(request.url);
      const path = url.pathname;

      console.log(`[BatchCoordinator] Request: ${request.method} ${path}`);

      if (path === '/init' && request.method === 'POST') {
        return await this.handleInit(request);
      } else if (path === '/batch' && request.method === 'POST') {
        return await this.handleBatch(request);
      } else if (path === '/result' && request.method === 'GET') {
        return await this.handleResult(request);
      } else {
        console.log(`[BatchCoordinator] ❌ 404 Not Found: ${path}`);
        return new Response('Not Found', { status: 404 });
      }
    } catch (err) {
      console.error('[BatchCoordinator] ❌ Top-level error:', err);
      return new Response(JSON.stringify({ error: String(err) }), {
        status: 500,
        headers: { 'Content-Type': 'application/json' },
      });
    }
  }

  private async handleInit(request: Request): Promise<Response> {
    console.log(`[BatchCoordinator:Init] Starting initialization`);
    
    const body = (await request.json().catch(() => ({} as any))) as any;

    const membershipId = String(body.membershipId ?? '');
    const membershipType = (typeof body.membershipType !== 'undefined') ? Number(body.membershipType) : undefined;
    const dungeonHash = String(body.dungeonHash ?? '');
    const totalBatches = Number(body.totalBatches) || 0;

    console.log(`[BatchCoordinator:Init] - Membership ID: ${membershipId}`);
    console.log(`[BatchCoordinator:Init] - Membership Type: ${membershipType}`);
    console.log(`[BatchCoordinator:Init] - Dungeon Hash: ${dungeonHash}`);
    console.log(`[BatchCoordinator:Init] - Total Batches: ${totalBatches}`);

    if (!membershipId || !dungeonHash || totalBatches <= 0) {
      console.log(`[BatchCoordinator:Init] ❌ Invalid parameters`);
      return new Response(JSON.stringify({
        error: 'Missing or invalid fields. Require membershipId, dungeonHash and totalBatches>0'
      }), { status: 400, headers: { 'Content-Type': 'application/json' } });
    }

    const meta: Meta = {
      membershipId,
      membershipType,
      dungeonHash,
      totalBatches,
      createdAt: this.now(),
    };

    const initialAgg: AggState = {
      receivedCount: 0,
      totalActivities: 0,
      totalFullClears: 0,
      totalPlaytimeSeconds: 0,
      latestActivityDate: null,
      seenIndices: [],
    };

    await this.state.storage.put(META_KEY, meta);
    await this.state.storage.put(AGG_KEY, initialAgg);
    
    const alarmTime = this.now() + TTL_MS;
    await this.state.storage.setAlarm(alarmTime);

    console.log(`[BatchCoordinator:Init] ✓ Metadata saved`);
    console.log(`[BatchCoordinator:Init] ✓ Aggregation state initialized`);
    console.log(`[BatchCoordinator:Init] ✓ Alarm set for ${new Date(alarmTime).toISOString()}`);
    console.log(`[BatchCoordinator:Init] ✅ Initialization complete`);

    return new Response(JSON.stringify({ ok: true }), { 
      status: 200, 
      headers: { 'Content-Type': 'application/json' } 
    });
  }

  private async handleBatch(request: Request): Promise<Response> {
    console.log(`[BatchCoordinator:Batch] Processing batch result`);
    
    const meta = (await this.state.storage.get(META_KEY)) as Meta | undefined;
    if (!meta) {
      console.log(`[BatchCoordinator:Batch] ❌ Job not initialized`);
      return new Response(JSON.stringify({ error: 'Job not initialized' }), { 
        status: 400, 
        headers: { 'Content-Type': 'application/json' } 
      });
    }

    console.log(`[BatchCoordinator:Batch] Job context:`);
    console.log(`[BatchCoordinator:Batch] - Membership ID: ${meta.membershipId}`);
    console.log(`[BatchCoordinator:Batch] - Dungeon Hash: ${meta.dungeonHash}`);
    console.log(`[BatchCoordinator:Batch] - Total Batches Expected: ${meta.totalBatches}`);

    const payload = (await request.json().catch(() => ({} as any))) as any;

    const batchIndex = Number.isFinite(Number(payload.batchIndex)) ? Number(payload.batchIndex) : null;
    const activitiesCount = Number.isFinite(Number(payload.activitiesCount)) ? Number(payload.activitiesCount) : 0;
    const fullClearsFound = Number.isFinite(Number(payload.fullClearsFound ?? payload.fullClears)) ? Number(payload.fullClearsFound ?? payload.fullClears) : 0;
    const playtimeSeconds = Number.isFinite(Number(payload.playtimeSeconds ?? payload.totalPlaytimeSeconds)) ? Number(payload.playtimeSeconds ?? payload.totalPlaytimeSeconds) : 0;
    const latestActivityDate = payload.latestActivityDate ?? payload.lastActivityDate ?? null;
    const dungeonHash = String(payload.dungeonHash ?? meta.dungeonHash);

    console.log(`[BatchCoordinator:Batch] Batch data:`);
    console.log(`[BatchCoordinator:Batch] - Batch Index: ${batchIndex}`);
    console.log(`[BatchCoordinator:Batch] - Activities Count: ${activitiesCount}`);
    console.log(`[BatchCoordinator:Batch] - Full Clears: ${fullClearsFound}`);
    console.log(`[BatchCoordinator:Batch] - Playtime: ${playtimeSeconds}s`);
    console.log(`[BatchCoordinator:Batch] - Latest Activity: ${latestActivityDate || 'N/A'}`);

    if (dungeonHash !== meta.dungeonHash) {
      console.log(`[BatchCoordinator:Batch] ❌ Dungeon hash mismatch: ${dungeonHash} != ${meta.dungeonHash}`);
      return new Response(JSON.stringify({ error: 'dungeonHash mismatch' }), { 
        status: 400, 
        headers: { 'Content-Type': 'application/json' } 
      });
    }

    if (batchIndex === null) {
      console.log(`[BatchCoordinator:Batch] ❌ Missing or invalid batchIndex`);
      return new Response(JSON.stringify({ error: 'Missing or invalid batchIndex' }), { 
        status: 400, 
        headers: { 'Content-Type': 'application/json' } 
      });
    }

    // Atomic update
    console.log(`[BatchCoordinator:Batch] Performing atomic aggregation update...`);
    
    await this.state.blockConcurrencyWhile?.(async () => {
      const stored = (await this.state.storage.get(AGG_KEY)) as AggState | undefined;
      const agg: AggState = stored ? {
        receivedCount: stored.receivedCount || 0,
        totalActivities: stored.totalActivities || 0,
        totalFullClears: stored.totalFullClears || 0,
        totalPlaytimeSeconds: stored.totalPlaytimeSeconds || 0,
        latestActivityDate: stored.latestActivityDate || null,
        seenIndices: Array.isArray(stored.seenIndices) ? stored.seenIndices.slice() : [],
      } : {
        receivedCount: 0,
        totalActivities: 0,
        totalFullClears: 0,
        totalPlaytimeSeconds: 0,
        latestActivityDate: null,
        seenIndices: [],
      };

      // Deduplicate by batchIndex
      if (agg.seenIndices.includes(batchIndex)) {
        console.log(`[BatchCoordinator:Batch] ⚠️  Duplicate batch index ${batchIndex} - ignoring`);
        await this.state.storage.setAlarm(this.now() + TTL_MS);
        return;
      }

      console.log(`[BatchCoordinator:Batch] Previous aggregation:`);
      console.log(`[BatchCoordinator:Batch] - Received: ${agg.receivedCount}`);
      console.log(`[BatchCoordinator:Batch] - Activities: ${agg.totalActivities}`);
      console.log(`[BatchCoordinator:Batch] - Full Clears: ${agg.totalFullClears}`);
      console.log(`[BatchCoordinator:Batch] - Playtime: ${agg.totalPlaytimeSeconds}s`);

      agg.seenIndices.push(batchIndex);
      agg.receivedCount = (agg.receivedCount || 0) + 1;
      agg.totalActivities = (agg.totalActivities || 0) + activitiesCount;
      agg.totalFullClears = (agg.totalFullClears || 0) + fullClearsFound;
      agg.totalPlaytimeSeconds = (agg.totalPlaytimeSeconds || 0) + playtimeSeconds;

      if (latestActivityDate) {
        if (!agg.latestActivityDate || latestActivityDate > agg.latestActivityDate) {
          console.log(`[BatchCoordinator:Batch] Updating latest activity date: ${latestActivityDate}`);
          agg.latestActivityDate = latestActivityDate;
        }
      }

      await this.state.storage.put(AGG_KEY, agg);
      await this.state.storage.setAlarm(this.now() + TTL_MS);

      console.log(`[BatchCoordinator:Batch] ✓ Aggregation updated`);
      console.log(`[BatchCoordinator:Batch] New aggregation:`);
      console.log(`[BatchCoordinator:Batch] - Received: ${agg.receivedCount}/${meta.totalBatches}`);
      console.log(`[BatchCoordinator:Batch] - Activities: ${agg.totalActivities}`);
      console.log(`[BatchCoordinator:Batch] - Full Clears: ${agg.totalFullClears}`);
      console.log(`[BatchCoordinator:Batch] - Playtime: ${agg.totalPlaytimeSeconds}s (${(agg.totalPlaytimeSeconds / 3600).toFixed(2)}h)`);
    });

    const updated = (await this.state.storage.get(AGG_KEY)) as AggState;
    const complete = updated.receivedCount >= meta.totalBatches;

    if (complete) {
      console.log(`[BatchCoordinator:Batch] 🎉 All batches received! Job complete`);
      
      const aggregated = {
        totalActivities: updated.totalActivities || 0,
        totalFullClears: updated.totalFullClears || 0,
        totalPlaytimeSeconds: updated.totalPlaytimeSeconds || 0,
        latestActivityDate: updated.latestActivityDate || null,
      };

      console.log(`[BatchCoordinator:Batch] Final aggregation:`);
      console.log(`[BatchCoordinator:Batch] - Total Activities: ${aggregated.totalActivities}`);
      console.log(`[BatchCoordinator:Batch] - Total Full Clears: ${aggregated.totalFullClears}`);
      console.log(`[BatchCoordinator:Batch] - Total Playtime: ${aggregated.totalPlaytimeSeconds}s (${(aggregated.totalPlaytimeSeconds / 3600).toFixed(2)}h)`);
      console.log(`[BatchCoordinator:Batch] - Latest Activity: ${aggregated.latestActivityDate || 'N/A'}`);

      console.log(`[BatchCoordinator:Batch] Cleaning up storage...`);
      await this.cleanup();
      console.log(`[BatchCoordinator:Batch] ✅ Storage cleaned`);

      return new Response(JSON.stringify({ complete: true, aggregated }), { 
        status: 200, 
        headers: { 'Content-Type': 'application/json' } 
      });
    }

    const progress = ((updated.receivedCount / meta.totalBatches) * 100).toFixed(1);
    console.log(`[BatchCoordinator:Batch] ⏳ Waiting for more batches (${progress}% complete)`);

    return new Response(JSON.stringify({
      complete: false,
      batchesReceived: updated.receivedCount || 0,
      totalBatches: meta.totalBatches,
    }), { status: 200, headers: { 'Content-Type': 'application/json' } });
  }

  private async handleResult(request: Request): Promise<Response> {
    console.log(`[BatchCoordinator:Result] Fetching result`);
    
    const meta = (await this.state.storage.get(META_KEY)) as Meta | undefined;
    const agg = (await this.state.storage.get(AGG_KEY)) as AggState | undefined;

    if (!meta || !agg) {
      console.log(`[BatchCoordinator:Result] ❌ Job not initialized`);
      return new Response(JSON.stringify({ error: 'Job not initialized' }), { 
        status: 404, 
        headers: { 'Content-Type': 'application/json' } 
      });
    }

    const complete = agg.receivedCount >= meta.totalBatches;

    console.log(`[BatchCoordinator:Result] Job status:`);
    console.log(`[BatchCoordinator:Result] - Complete: ${complete}`);
    console.log(`[BatchCoordinator:Result] - Progress: ${agg.receivedCount}/${meta.totalBatches}`);

    if (!complete) {
      console.log(`[BatchCoordinator:Result] ⏳ Job incomplete - timeout`);
      return new Response(JSON.stringify({
        error: 'Timeout waiting for batches',
        received: agg.receivedCount || 0,
        total: meta.totalBatches,
      }), { status: 408, headers: { 'Content-Type': 'application/json' } });
    }

    const aggregated = {
      totalActivities: agg.totalActivities || 0,
      totalFullClears: agg.totalFullClears || 0,
      totalPlaytimeSeconds: agg.totalPlaytimeSeconds || 0,
      latestActivityDate: agg.latestActivityDate || null,
    };

    console.log(`[BatchCoordinator:Result] ✓ Returning final aggregation`);
    console.log(`[BatchCoordinator:Result] - Activities: ${aggregated.totalActivities}`);
    console.log(`[BatchCoordinator:Result] - Full Clears: ${aggregated.totalFullClears}`);
    console.log(`[BatchCoordinator:Result] - Playtime: ${aggregated.totalPlaytimeSeconds}s`);

    await this.cleanup();
    console.log(`[BatchCoordinator:Result] ✓ Storage cleaned`);

    return new Response(JSON.stringify(aggregated), { 
      status: 200, 
      headers: { 'Content-Type': 'application/json' } 
    });
  }

  private async cleanup(): Promise<void> {
    try {
      console.log(`[BatchCoordinator:Cleanup] Starting cleanup`);
      await this.state.storage.delete(META_KEY);
      await this.state.storage.delete(AGG_KEY);
      console.log(`[BatchCoordinator:Cleanup] ✓ Deleted meta and aggregation keys`);
    } catch (err) {
      console.warn(`[BatchCoordinator:Cleanup] ⚠️  Cleanup error:`, err);
    }
  }

  async alarm(): Promise<void> {
    console.log('[BatchCoordinator:Alarm] Alarm triggered - cleaning up stale job');
    await this.cleanup();
    console.log('[BatchCoordinator:Alarm] ✓ Cleanup complete');
  }
}