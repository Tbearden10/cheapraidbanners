// ============================================================================
// FILE: src/processors/memberJobProcessor.ts
// Clean member job processor with MemberCoordinator for batch aggregation
// ============================================================================

import type { Env, MemberJob } from '../types';
import { ACTIVITY_REFERENCE_MAP } from '../constants/activityReferenceMap';
import {
  fetchCharactersForMember,
  fetchActivitiesForCharacter,
  withRateLimit,
} from '../api/bungieApi';

const sleep = (ms: number) => new Promise(r => setTimeout(r, ms));

// Smaller batches = faster processing, better throughput
const MAX_BATCH_SIZE = 30;

export async function processMemberJob(env: Env, job: MemberJob): Promise<void> {
  const startTime = Date.now();
  console.log(`[MemberJob] START: ${job.displayName}`);

  // Fetch characters
  const characters = await fetchCharactersForMember(
    job.membershipId,
    job.membershipType,
    env.BUNGIE_API_KEY
  );

  if (!characters || characters.length === 0) {
    console.log(`[MemberJob] No characters found for ${job.displayName}`);
    await notifyRunComplete(env, job);
    return;
  }

  // Fetch all activities
  const activitiesByChar = await fetchAllActivities(
    job.membershipType,
    job.membershipId,
    characters.map((c: any) => c.characterId),
    env.BUNGIE_API_KEY
  );

  const totalActivitiesFetched = Object.values(activitiesByChar).reduce(
    (sum, acts) => sum + acts.length, 0
  );
  
  console.log(`[MemberJob] Fetched ${totalActivitiesFetched} activities for ${job.displayName}`);

  // Group by dungeon hash with inline deduplication
  // Use Maps to dedupe as we go - handles character switching during runs
  // When a user switches characters mid-dungeon:
  //   - Character 1 may show run as "non-completed" (left early)
  //   - Character 2 shows same run as "completed" (finished it)
  // We track by instanceId and always prefer the completed version
  const activitiesByDungeon: Record<string, Map<string, any>> = {};
  for (const dungeon of ACTIVITY_REFERENCE_MAP) {
    activitiesByDungeon[dungeon.hash] = new Map<string, any>();
  }

  for (const charId of Object.keys(activitiesByChar)) {
    for (const activity of activitiesByChar[charId]) {
      const refId = String(activity?.activityDetails?.referenceId || '');
      
      for (const dungeon of ACTIVITY_REFERENCE_MAP) {
        if (dungeon.referenceIds.includes(refId)) {
          const instanceId = activity.activityDetails?.instanceId || activity.instanceId;
          if (!instanceId) {
            // Skip activities without instanceId - can't deduplicate or fetch PGCR later
            // Note: break (not continue) to match original behavior - stops checking remaining dungeons
            break;
          }
          
          const dungeonMap = activitiesByDungeon[dungeon.hash];
          const actWithChar = { ...activity, characterId: charId };
          const existing = dungeonMap.get(instanceId);
          
          if (!existing) {
            // First time seeing this instance
            dungeonMap.set(instanceId, actWithChar);
          } else {
            // Duplicate instance - prefer completed over non-completed
            const existingCompleted = !!(existing?.values?.completed?.basic?.value === 1);
            const newCompleted = !!(actWithChar?.values?.completed?.basic?.value === 1);
            
            if (!existingCompleted && newCompleted) {
              // Replace non-completed with completed version
              dungeonMap.set(instanceId, actWithChar);
            }
            // If both completed or both not completed, keep existing (first seen)
          }
          break;
        }
      }
    }
  }

  // Calculate total batches across all dungeons
  let totalBatches = 0;
  const dungeonBatchCounts: Record<string, number> = {};
  
  for (const dungeon of ACTIVITY_REFERENCE_MAP) {
    const dungeonHash = dungeon.hash;
    const activitiesMap = activitiesByDungeon[dungeonHash];
    const activities = activitiesMap ? Array.from(activitiesMap.values()) : [];
    if (activities.length === 0) continue;

    const completed = activities.filter(a => a?.values?.completed?.basic?.value === 1);
    if (completed.length === 0) continue;

    // Check DB for previous stats
    const prevRow = await env.DB.prepare(`
      SELECT last_processed_date, total_clears
      FROM member_dungeon_stats
      WHERE clan_id = ? AND membership_id = ? AND dungeon_hash = ?
    `).bind(job.clanId, job.membershipId, dungeonHash).first();

    let cutoffDate: Date | null = null;
    if (prevRow && (prevRow as any).last_processed_date) {
      cutoffDate = new Date((prevRow as any).last_processed_date);
    } else if (job.lastProcessedDate) {
      cutoffDate = new Date(job.lastProcessedDate);
    }

    const dbTotalClears = prevRow ? Number((prevRow as any).total_clears ?? 0) : 0;

    // Log comparison per dungeon
    console.log(`[MemberJob:${dungeon.displayName}] DB clears: ${dbTotalClears}, Fetched completed: ${completed.length}`);

    // Do count check first per dungeon
    if (completed.length <= dbTotalClears) {
      console.log(`[MemberJob:${dungeon.displayName}] Skipping - no new activities (fetched <= DB)`);
      continue;
    }

    completed.sort((a, b) => {
      const ta = a.period ? new Date(a.period).getTime() : 0;
      const tb = b.period ? new Date(b.period).getTime() : 0;
      return ta - tb;
    });

    let newActivities = completed;
    if (cutoffDate) {
      newActivities = completed.filter(a => {
        try {
          const actDate = new Date(a.period);
          return actDate.getTime() > cutoffDate!.getTime();
        } catch {
          return true;
        }
      });
      console.log(`[MemberJob:${dungeon.displayName}] After date filter: ${newActivities.length} new activities (cutoff: ${cutoffDate.toISOString()})`);
    }

    if (newActivities.length === 0) {
      console.log(`[MemberJob:${dungeon.displayName}] Skipping - no activities after date filter`);
      continue;
    }

    const batchCount = Math.ceil(newActivities.length / MAX_BATCH_SIZE);
    totalBatches += batchCount;
    dungeonBatchCounts[dungeonHash] = batchCount;
  }

  // Initialize MemberCoordinator if we have batches to process
  if (totalBatches > 0) {
    const coordinatorId = env.MEMBER_COORDINATOR.idFromName(job.membershipId);
    const coordinator = env.MEMBER_COORDINATOR.get(coordinatorId);
    
    await coordinator.fetch('https://internal/init', {
      method: 'POST',
      body: JSON.stringify({
        membershipId: job.membershipId,
        membershipType: job.membershipType,
        clanId: job.clanId,
        totalBatches,
        dungeonBatches: dungeonBatchCounts
      }),
      headers: { 'Content-Type': 'application/json' },
    });
  }

  // Queue per-dungeon jobs
  let totalQueued = 0;
  let batchesQueued = 0;

  for (const dungeon of ACTIVITY_REFERENCE_MAP) {
    const dungeonHash = dungeon.hash;
    const activitiesMap = activitiesByDungeon[dungeonHash];
    const activities = activitiesMap ? Array.from(activitiesMap.values()) : [];

    if (activities.length === 0) continue;

    // Filter to completed only
    const completed = activities.filter(a => a?.values?.completed?.basic?.value === 1);
    if (completed.length === 0) continue;

    // Check DB for previous stats
    const prevRow = await env.DB.prepare(`
      SELECT last_processed_date, total_clears
      FROM member_dungeon_stats
      WHERE clan_id = ? AND membership_id = ? AND dungeon_hash = ?
    `).bind(job.clanId, job.membershipId, dungeonHash).first();

    let cutoffDate: Date | null = null;
    if (prevRow && (prevRow as any).last_processed_date) {
      cutoffDate = new Date((prevRow as any).last_processed_date);
    } else if (job.lastProcessedDate) {
      cutoffDate = new Date(job.lastProcessedDate);
    }

    const dbTotalClears = prevRow ? Number((prevRow as any).total_clears ?? 0) : 0;

    // Log comparison per dungeon
    console.log(`[MemberJob:Queue:${dungeon.displayName}] DB clears: ${dbTotalClears}, Fetched completed: ${completed.length}`);

    // Do count check first per dungeon
    if (completed.length <= dbTotalClears) {
      console.log(`[MemberJob:Queue:${dungeon.displayName}] Skipping - no new activities (fetched <= DB)`);
      continue;
    }

    // Sort by period ascending (oldest first)
    completed.sort((a, b) => {
      const ta = a.period ? new Date(a.period).getTime() : 0;
      const tb = b.period ? new Date(b.period).getTime() : 0;
      return ta - tb;
    });

    // Filter to new activities after cutoff
    let newActivities = completed;
    if (cutoffDate) {
      newActivities = completed.filter(a => {
        try {
          const actDate = new Date(a.period);
          return actDate.getTime() > cutoffDate!.getTime();
        } catch {
          return true;
        }
      });
      console.log(`[MemberJob:Queue:${dungeon.displayName}] After date filter: ${newActivities.length} new activities (cutoff: ${cutoffDate.toISOString()})`);
    }

    if (newActivities.length === 0) {
      console.log(`[MemberJob:Queue:${dungeon.displayName}] Skipping - no activities after date filter`);
      continue;
    }

    // Prepare activities payload
    const activitiesPayload = newActivities.map(a => ({
      instanceId: a.activityDetails?.instanceId || a.instanceId,
      date: a.period,
      characterId: (a as any).characterId,
    }));

    // Split into batches
    const batches: any[][] = [];
    for (let i = 0; i < activitiesPayload.length; i += MAX_BATCH_SIZE) {
      batches.push(activitiesPayload.slice(i, i + MAX_BATCH_SIZE));
    }

    // Queue each batch
    for (let batchIndex = 0; batchIndex < batches.length; batchIndex++) {
      await env.STATS_QUEUE.send({
        clanId: job.clanId,
        membershipId: job.membershipId,
        membershipType: job.membershipType,
        dungeonHash,
        activities: batches[batchIndex],
        jobId: `${job.membershipId}-${dungeonHash}-${batchIndex}`,
      });
      
      totalQueued += batches[batchIndex].length;
      batchesQueued++;
    }

    console.log(`[MemberJob] Queued ${batches.length} batch(es) for ${dungeon.displayName} (${activitiesPayload.length} activities)`);
  }

  await notifyRunComplete(env, job);

  const duration = Date.now() - startTime;
  console.log(`[MemberJob] COMPLETE: ${job.displayName} | Queued: ${totalQueued} | Batches: ${batchesQueued} | ${(duration/1000).toFixed(1)}s`);
}

async function notifyRunComplete(env: Env, job: MemberJob): Promise<void> {
  if (!job.runId) return;
  
  try {
    const trackerId = env.RUN_TRACKER.idFromName(`run-tracker-${job.clanId}`);
    const tracker = env.RUN_TRACKER.get(trackerId);
    
    const res = await tracker.fetch('https://internal/complete', {
      method: 'POST',
      body: JSON.stringify({
        runId: job.runId,
        membershipId: job.membershipId,
      }),
      headers: { 'Content-Type': 'application/json' },
    });

    // Consume body
    try {
      await res.text();
    } catch (e) {
      try { res.body?.cancel(); } catch {}
    }
  } catch (err) {
    console.warn('[MemberJob] Failed to notify RunTracker:', err);
  }
}

async function fetchAllActivities(
  membershipType: number,
  membershipId: string,
  characterIds: string[],
  apiKey: string
): Promise<Record<string, any[]>> {
  const out: Record<string, any[]> = {};
  for (const id of characterIds) out[id] = [];

  const modes = [82, 2]; // Dungeon, Story

  async function fetchAllPagesForCharacter(charId: string, mode: number) {
    const pageSize = 250;
    let page = 0;

    while (true) {
      const activities: any[] = await withRateLimit(
        () =>
          fetchActivitiesForCharacter(
            membershipType,
            membershipId,
            charId,
            page,
            mode,
            pageSize,
            apiKey
          ),
        3
      ).catch(() => []);

      if (activities && activities.length > 0) {
        out[charId].push(...activities);
      }

      if (!activities || activities.length < pageSize) break;

      page++;
      await sleep(200);
    }
  }

  for (const mode of modes) {
    await Promise.all(
      characterIds.map((charId) => fetchAllPagesForCharacter(charId, mode))
    );
    await sleep(250);
  }

  return out;
}