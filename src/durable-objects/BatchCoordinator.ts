/**
 * BatchCoordinator Durable Object - reduced logging to essentials
 */

import type { DurableObjectState } from '../types';

export interface BatchResult {
  batchIndex: number;
  activitiesCount: number;
  fullClearsFound: number;
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
  }

  private now() {
    return Date.now();
  }

  async fetch(request: Request): Promise<Response> {
    try {
      const url = new URL(request.url);
      const path = url.pathname;

      if (path === '/init' && request.method === 'POST') {
        return await this.handleInit(request);
      } else if (path === '/batch' && request.method === 'POST') {
        return await this.handleBatch(request);
      } else if (path === '/result' && request.method === 'GET') {
        return await this.handleResult(request);
      } else {
        return new Response('Not Found', { status: 404 });
      }
    } catch (err) {
      console.error('[BatchCoordinator] Top-level error:', err);
      return new Response(JSON.stringify({ error: String(err) }), {
        status: 500,
        headers: { 'Content-Type': 'application/json' },
      });
    }
  }

  private async handleInit(request: Request): Promise<Response> {
    const body = (await request.json().catch(() => ({} as any))) as any;

    const membershipId = String(body.membershipId ?? '');
    const membershipType = (typeof body.membershipType !== 'undefined') ? Number(body.membershipType) : undefined;
    const dungeonHash = String(body.dungeonHash ?? '');
    const totalBatches = Number(body.totalBatches) || 0;

    if (!membershipId || !dungeonHash || totalBatches <= 0) {
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

    return new Response(JSON.stringify({ ok: true }), {
      status: 200,
      headers: { 'Content-Type': 'application/json' }
    });
  }

  private async handleBatch(request: Request): Promise<Response> {
    const meta = (await this.state.storage.get(META_KEY)) as Meta | undefined;
    if (!meta) {
      return new Response(JSON.stringify({ error: 'Job not initialized' }), {
        status: 400,
        headers: { 'Content-Type': 'application/json' }
      });
    }

    const payload = (await request.json().catch(() => ({} as any))) as any;

    const batchIndex = Number.isFinite(Number(payload.batchIndex)) ? Number(payload.batchIndex) : null;
    const activitiesCount = Number.isFinite(Number(payload.activitiesCount)) ? Number(payload.activitiesCount) : 0;
    const fullClearsFound = Number.isFinite(Number(payload.fullClearsFound ?? payload.fullClears)) ? Number(payload.fullClearsFound ?? payload.fullClears) : 0;
    const playtimeSeconds = Number.isFinite(Number(payload.playtimeSeconds ?? payload.totalPlaytimeSeconds)) ? Number(payload.playtimeSeconds ?? payload.totalPlaytimeSeconds) : 0;
    const latestActivityDate = payload.latestActivityDate ?? payload.lastActivityDate ?? null;
    const dungeonHash = String(payload.dungeonHash ?? meta.dungeonHash);

    if (dungeonHash !== meta.dungeonHash) {
      return new Response(JSON.stringify({ error: 'dungeonHash mismatch' }), {
        status: 400,
        headers: { 'Content-Type': 'application/json' }
      });
    }

    if (batchIndex === null) {
      return new Response(JSON.stringify({ error: 'Missing or invalid batchIndex' }), {
        status: 400,
        headers: { 'Content-Type': 'application/json' }
      });
    }

    // Atomic update
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
        await this.state.storage.setAlarm(this.now() + TTL_MS);
        return;
      }

      agg.seenIndices.push(batchIndex);
      agg.receivedCount = (agg.receivedCount || 0) + 1;
      agg.totalActivities = (agg.totalActivities || 0) + activitiesCount;
      agg.totalFullClears = (agg.totalFullClears || 0) + fullClearsFound;
      agg.totalPlaytimeSeconds = (agg.totalPlaytimeSeconds || 0) + playtimeSeconds;

      if (latestActivityDate) {
        if (!agg.latestActivityDate || latestActivityDate > agg.latestActivityDate) {
          agg.latestActivityDate = latestActivityDate;
        }
      }

      await this.state.storage.put(AGG_KEY, agg);
      await this.state.storage.setAlarm(this.now() + TTL_MS);
    });

    const updated = (await this.state.storage.get(AGG_KEY)) as AggState;
    const complete = updated.receivedCount >= meta.totalBatches;

    if (complete) {
      const aggregated = {
        totalActivities: updated.totalActivities || 0,
        totalFullClears: updated.totalFullClears || 0,
        totalPlaytimeSeconds: updated.totalPlaytimeSeconds || 0,
        latestActivityDate: updated.latestActivityDate || null,
      };
      await this.cleanup();
      return new Response(JSON.stringify({ complete: true, aggregated }), {
        status: 200,
        headers: { 'Content-Type': 'application/json' }
      });
    }

    return new Response(JSON.stringify({
      complete: false,
      batchesReceived: updated.receivedCount || 0,
      totalBatches: meta.totalBatches,
    }), { status: 200, headers: { 'Content-Type': 'application/json' } });
  }

  private async handleResult(request: Request): Promise<Response> {
    const meta = (await this.state.storage.get(META_KEY)) as Meta | undefined;
    const agg = (await this.state.storage.get(AGG_KEY)) as AggState | undefined;

    if (!meta || !agg) {
      return new Response(JSON.stringify({ error: 'Job not initialized' }), {
        status: 404,
        headers: { 'Content-Type': 'application/json' }
      });
    }

    const complete = agg.receivedCount >= meta.totalBatches;
    if (!complete) {
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

    await this.cleanup();

    return new Response(JSON.stringify(aggregated), {
      status: 200,
      headers: { 'Content-Type': 'application/json' }
    });
  }

  private async cleanup(): Promise<void> {
    try {
      await this.state.storage.delete(META_KEY);
      await this.state.storage.delete(AGG_KEY);
    } catch (err) {
      console.warn('[BatchCoordinator] Cleanup error:', err);
    }
  }

  async alarm(): Promise<void> {
    await this.cleanup();
  }
}