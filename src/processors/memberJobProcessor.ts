
// ============================================================================
// FILE: src/processors/memberJobProcessor.ts
// REFACTORED: Batches of 50 instances (not 500), no DO coordination
// Logging reduced to only high-level steps and summaries
// ============================================================================

import type { Env, MemberJob } from '../types';
import { ACTIVITY_REFERENCE_MAP } from '../constants/activityReferenceMap';
import {
  fetchCharactersForMember,
  fetchActivitiesForCharacter,
  withRateLimit,
} from '../api/bungieApi';

const sleep = (ms: number) => new Promise(r => setTimeout(r, ms));

// REDUCED from 500 to 50 to stay under subrequest & CPU limits
const MAX_BATCH_SIZE = 50;

export async function processMemberJob(env: Env, job: MemberJob): Promise<void> {
  const startTime = Date.now();
  console.log(`\n${'='.repeat(80)}`);
  console.log(`[MemberJob] START: ${job.displayName} | Membership: ${job.membershipId} | LastProcessed: ${job.lastProcessedDate || 'Never'}`);
  console.log(`${'='.repeat(80)}`);

  // 1. Fetch characters
  const characters = await fetchCharactersForMember(
    job.membershipId,
    job.membershipType,
    env.BUNGIE_API_KEY
  );

  if (!characters || characters.length === 0) {
    console.log(`[MemberJob] ⚠️ No characters found for ${job.displayName}`);
    await notifyRunComplete(env, job);
    return;
  }

  // 2. Fetch ALL activities
  const activitiesByChar = await fetchAllActivities(
    job.membershipType,
    job.membershipId,
    characters.map((c: any) => c.characterId),
    env.BUNGIE_API_KEY,
    job.displayName
  );

  const totalActivitiesFetched = Object.values(activitiesByChar).reduce(
    (sum, acts) => sum + acts.length, 0
  );
  console.log(`[MemberJob] ✓ Fetched ${totalActivitiesFetched} activities for ${job.displayName}`);

  // 3. Group by dungeon hash
  const activitiesByDungeon: Record<string, any[]> = {};
  for (const dungeon of ACTIVITY_REFERENCE_MAP) {
    activitiesByDungeon[dungeon.hash] = [];
  }

  let totalMatched = 0;
  for (const charId of Object.keys(activitiesByChar)) {
    for (const activity of activitiesByChar[charId]) {
      const refId = String(activity?.activityDetails?.referenceId || '');
      
      for (const dungeon of ACTIVITY_REFERENCE_MAP) {
        if (dungeon.referenceIds.includes(refId)) {
          activitiesByDungeon[dungeon.hash].push({
            ...activity,
            characterId: charId,
          });
          totalMatched++;
          break;
        }
      }
    }
  }

  console.log(`[MemberJob] ✓ Matched ${totalMatched} activities to known dungeons`);

  // 4. Dedupe per dungeon
  for (const hash of Object.keys(activitiesByDungeon)) {
    const map = new Map<string, any>();
    
    for (const act of activitiesByDungeon[hash]) {
      const id = act.activityDetails?.instanceId || act.instanceId;
      if (!id) continue;
      
      const existing = map.get(id);
      if (!existing) {
        map.set(id, act);
      } else {
        const existingCompleted = !!(existing?.values?.completed?.basic?.value === 1);
        const newCompleted = !!(act?.values?.completed?.basic?.value === 1);
        if (!existingCompleted && newCompleted) {
          map.set(id, act);
        }
      }
    }
    activitiesByDungeon[hash] = Array.from(map.values());
  }

  // 5. Queue per-dungeon jobs
  console.log(`[MemberJob] Queueing per-dungeon batches (max batch size ${MAX_BATCH_SIZE})...`);
  let totalQueued = 0;
  let totalBatches = 0;

  for (const dungeon of ACTIVITY_REFERENCE_MAP) {
    const dungeonHash = dungeon.hash;
    const activities = activitiesByDungeon[dungeonHash] || [];

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

    // Quick skip if Bungie shows same or fewer completions
    if (completed.length <= dbTotalClears) {
      // skip quietly
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
    }

    if (newActivities.length === 0) {
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
      const batchSize = batches[batchIndex].length;
      
      await env.STATS_QUEUE.send({
        clanId: job.clanId,
        membershipId: job.membershipId,
        membershipType: job.membershipType,
        dungeonHash,
        activities: batches[batchIndex],
        jobId: `${job.membershipId}-${dungeonHash}-${batchIndex}`,
      });
      
      totalQueued += batchSize;
      totalBatches++;
    }

    // Log per-dungeon summary
    console.log(`[MemberJob]   Queued ${batches.length} batch(es) for ${dungeon.displayName} (${activitiesPayload.length} activities)`);
  }

  // Notify RunTracker that this member is complete
  await notifyRunComplete(env, job);

  const duration = Date.now() - startTime;
  console.log(`\n${'='.repeat(80)}`);
  console.log(`[MemberJob] COMPLETE: ${job.displayName} | Activities queued: ${totalQueued} | Batches: ${totalBatches} | Duration: ${duration}ms`);
  console.log(`${'='.repeat(80)}\n`);
}

async function notifyRunComplete(env: Env, job: MemberJob): Promise<void> {
  if (!job.runId) return;
  
  try {
    const trackerId = env.RUN_TRACKER.idFromName(`run-tracker-${job.clanId}`);
    const tracker = env.RUN_TRACKER.get(trackerId);
    
    await tracker.fetch('https://internal/complete', {
      method: 'POST',
      body: JSON.stringify({
        runId: job.runId,
        membershipId: job.membershipId,
      }),
      headers: { 'Content-Type': 'application/json' },
    });
  } catch (err) {
    console.warn('[MemberJob] Failed to notify RunTracker:', err);
  }
}

async function fetchAllActivities(
  membershipType: number,
  membershipId: string,
  characterIds: string[],
  apiKey: string,
  displayName: string
): Promise<Record<string, any[]>> {
  const out: Record<string, any[]> = {};
  for (const id of characterIds) out[id] = [];

  const modes = [82, 2]; // Dungeon, Story
  let totalFetched = 0;

  async function fetchAllPagesForCharacter(charId: string, mode: number) {
    const pageSize = 250;
    let page = 0;
    let charTotal = 0;

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
        charTotal += activities.length;
      }

      if (!activities || activities.length < pageSize) break;

      page++;
      await sleep(200);
    }
    
    totalFetched += charTotal;
  }

  for (const mode of modes) {
    const workers = characterIds.map((charId) => fetchAllPagesForCharacter(charId, mode));
    await Promise.all(workers);
    await sleep(250);
  }

  return out;
}