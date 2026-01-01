// ============================================================================
// FILE: src/durable-objects/MemberCoordinator.ts
// Coordinates all batches for a single member across all dungeons
// One DO per member, tracks progress and aggregates results
// ============================================================================

export interface MemberBatchMetadata {
  membershipId: string;
  membershipType: number;
  clanId: string;
  totalBatches: number; // Total batches across ALL dungeons
  dungeonBatches: Record<string, number>; // dungeonHash -> batch count
}

export interface BatchResult {
  batchId: string; // Format: "${membershipId}-${dungeonHash}-${batchIndex}"
  dungeonHash: string;
  batchIndex: number;
  clearsDelta: number;
  fullClearsDelta: number;
  playtimeDelta: number;
  lastProcessedDate: string | null;
}

export interface AggregatedResult {
  membershipId: string;
  membershipType: number;
  clanId: string;
  byDungeon: Record<string, {
    totalClears: number;
    totalFullClears: number;
    totalPlaytime: number;
    lastProcessedDate: string | null;
    batchesComplete: number;
    batchesExpected: number;
  }>;
  totalBatchesComplete: number;
  totalBatchesExpected: number;
  allComplete: boolean;
}

export class MemberCoordinator implements DurableObject {
  private state: DurableObjectState;
  private metadata: MemberBatchMetadata | null;
  private batches: Map<string, BatchResult>; // batchId -> result
  private dungeonBatchesReceived: Record<string, number>; // dungeonHash -> count
  private createdAt: number;

  constructor(state: DurableObjectState) {
    this.state = state;
    this.metadata = null;
    this.batches = new Map();
    this.dungeonBatchesReceived = {};
    this.createdAt = Date.now();
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const path = url.pathname;

    try {
      if (path === '/init' && request.method === 'POST') {
        return await this.handleInit(request);
      } else if (path === '/batch' && request.method === 'POST') {
        return await this.handleBatch(request);
      } else if (path === '/status' && request.method === 'GET') {
        return await this.handleStatus();
      } else {
        return new Response('Not found', { status: 404 });
      }
    } catch (error) {
      console.error('[MemberCoordinator] Error:', error);
      return new Response(
        JSON.stringify({ error: String(error) }),
        { status: 500, headers: { 'Content-Type': 'application/json' } }
      );
    }
  }

  private async handleInit(request: Request): Promise<Response> {
    const body = await request.json() as MemberBatchMetadata;

    if (!body.membershipId || !body.membershipType || !body.clanId || !body.totalBatches) {
      return new Response(
        JSON.stringify({ error: 'Missing required fields' }),
        { status: 400, headers: { 'Content-Type': 'application/json' } }
      );
    }

    this.metadata = body;
    this.batches.clear();
    this.dungeonBatchesReceived = {};
    this.createdAt = Date.now();

    await this.state.storage.put('metadata', this.metadata);
    await this.state.storage.put('createdAt', this.createdAt);

    // Auto-cleanup after 4 hours
    const fourHours = 4 * 60 * 60 * 1000;
    await this.state.storage.setAlarm(Date.now() + fourHours);

    return new Response(
      JSON.stringify({ success: true, message: 'Coordinator initialized' }),
      { status: 200, headers: { 'Content-Type': 'application/json' } }
    );
  }

  private async handleBatch(request: Request): Promise<Response> {
    if (!this.metadata) {
      this.metadata = await this.state.storage.get('metadata') as MemberBatchMetadata | null;
      if (!this.metadata) {
        return new Response(
          JSON.stringify({ error: 'Coordinator not initialized' }),
          { status: 400, headers: { 'Content-Type': 'application/json' } }
        );
      }
    }

    const batchResult = await request.json() as BatchResult;

    if (!batchResult.batchId || !batchResult.dungeonHash) {
      return new Response(
        JSON.stringify({ error: 'Missing batchId or dungeonHash' }),
        { status: 400, headers: { 'Content-Type': 'application/json' } }
      );
    }

    // Store batch result
    this.batches.set(batchResult.batchId, batchResult);

    // Track dungeon progress
    const dungeonHash = batchResult.dungeonHash;
    this.dungeonBatchesReceived[dungeonHash] = (this.dungeonBatchesReceived[dungeonHash] || 0) + 1;

    const totalReceived = this.batches.size;
    const allComplete = totalReceived === this.metadata.totalBatches;

    if (allComplete) {
      const aggregated = this.aggregateBatches();
      await this.cleanup();

      console.log(
        `[MemberCoordinator] All batches complete for ${this.metadata.membershipId} ` +
        `(${Object.keys(aggregated.byDungeon).length} dungeons)`
      );

      return new Response(
        JSON.stringify({
          complete: true,
          aggregated
        }),
        { status: 200, headers: { 'Content-Type': 'application/json' } }
      );
    }

    return new Response(
      JSON.stringify({
        complete: false,
        batchesReceived: totalReceived,
        totalBatches: this.metadata.totalBatches,
        dungeonProgress: this.dungeonBatchesReceived
      }),
      { status: 200, headers: { 'Content-Type': 'application/json' } }
    );
  }

  private async handleStatus(): Promise<Response> {
    if (!this.metadata) {
      this.metadata = await this.state.storage.get('metadata') as MemberBatchMetadata | null;
    }

    if (!this.metadata) {
      return new Response(
        JSON.stringify({ error: 'Coordinator not initialized' }),
        { status: 404, headers: { 'Content-Type': 'application/json' } }
      );
    }

    return new Response(
      JSON.stringify({
        metadata: this.metadata,
        batchesReceived: this.batches.size,
        totalBatches: this.metadata.totalBatches,
        dungeonProgress: this.dungeonBatchesReceived,
        complete: this.batches.size === this.metadata.totalBatches,
        createdAt: this.createdAt
      }),
      { status: 200, headers: { 'Content-Type': 'application/json' } }
    );
  }

  private aggregateBatches(): AggregatedResult {
    const byDungeon: Record<string, {
      totalClears: number;
      totalFullClears: number;
      totalPlaytime: number;
      lastProcessedDate: string | null;
      batchesComplete: number;
      batchesExpected: number;
    }> = {};

    // Initialize all dungeons
    for (const [dungeonHash, expectedBatches] of Object.entries(this.metadata!.dungeonBatches)) {
      byDungeon[dungeonHash] = {
        totalClears: 0,
        totalFullClears: 0,
        totalPlaytime: 0,
        lastProcessedDate: null,
        batchesComplete: 0,
        batchesExpected: expectedBatches
      };
    }

    // Aggregate batch results
    for (const batch of this.batches.values()) {
      const dungeon = byDungeon[batch.dungeonHash];
      if (!dungeon) continue;

      dungeon.totalClears += batch.clearsDelta;
      dungeon.totalFullClears += batch.fullClearsDelta;
      dungeon.totalPlaytime += batch.playtimeDelta;
      dungeon.batchesComplete += 1;

      if (batch.lastProcessedDate) {
        if (!dungeon.lastProcessedDate || batch.lastProcessedDate > dungeon.lastProcessedDate) {
          dungeon.lastProcessedDate = batch.lastProcessedDate;
        }
      }
    }

    return {
      membershipId: this.metadata!.membershipId,
      membershipType: this.metadata!.membershipType,
      clanId: this.metadata!.clanId,
      byDungeon,
      totalBatchesComplete: this.batches.size,
      totalBatchesExpected: this.metadata!.totalBatches,
      allComplete: true
    };
  }

  private async cleanup(): Promise<void> {
    await this.state.storage.deleteAll();
    this.batches.clear();
    this.metadata = null;
    this.dungeonBatchesReceived = {};
  }

  async alarm(): Promise<void> {
    await this.cleanup();
  }
}