// ============================================================================
// FILE: src/processors/memberJobProcessor.ts
// Processes a single member - fetches activities and queues per-dungeon jobs
// ============================================================================

import type { Env, MemberJob } from '../types';
import { ACTIVITY_REFERENCE_MAP } from '../constants/activityReferenceMap';
import {
  fetchCharactersForMember,
  fetchActivitiesForCharacter,
  withRateLimit,
} from '../api/bungieApi';

const sleep = (ms: number) => new Promise(r => setTimeout(r, ms));

export async function processMemberJob(env: Env, job: MemberJob): Promise<void> {
  const startTime = Date.now();
  console.log(`\n${'='.repeat(80)}`);
  console.log(`[MemberJob] START Processing member: ${job.displayName}`);
  console.log(`[MemberJob] - Membership ID: ${job.membershipId}`);
  console.log(`[MemberJob] - Membership Type: ${job.membershipType}`);
  console.log(`[MemberJob] - Clan ID: ${job.clanId}`);
  console.log(`[MemberJob] - Last Processed: ${job.lastProcessedDate || 'Never'}`);
  console.log(`[MemberJob] - Run ID: ${job.runId || 'N/A'}`);
  console.log(`${'='.repeat(80)}`);

  // 1. Fetch characters
  console.log(`[MemberJob] Step 1: Fetching characters for ${job.displayName}...`);
  const characters = await fetchCharactersForMember(
    job.membershipId,
    job.membershipType,
    env.BUNGIE_API_KEY
  );

  if (!characters || characters.length === 0) {
    console.log(`[MemberJob] ⚠️  No characters found for ${job.displayName}`);
    console.log(`[MemberJob] COMPLETE (no characters) - Duration: ${Date.now() - startTime}ms\n`);
    return;
  }

  const characterIds = characters.map((c: any) => c.characterId);
  console.log(`[MemberJob] ✓ Found ${characters.length} character(s): [${characterIds.join(', ')}]`);

  // 2. Fetch ALL activities (dungeon + story modes)
  console.log(`[MemberJob] Step 2: Fetching all activities (modes 82 + 2)...`);
  const activitiesByChar = await fetchAllActivities(
    job.membershipType,
    job.membershipId,
    characterIds,
    env.BUNGIE_API_KEY,
    job.displayName
  );

  const totalActivitiesFetched = Object.values(activitiesByChar).reduce((sum, acts) => sum + acts.length, 0);
  console.log(`[MemberJob] ✓ Fetched ${totalActivitiesFetched} total activities across all characters`);
  
  for (const [charId, acts] of Object.entries(activitiesByChar)) {
    console.log(`[MemberJob]   - Character ${charId}: ${acts.length} activities`);
  }

  // 3. Group by dungeon hash
  console.log(`[MemberJob] Step 3: Grouping activities by dungeon...`);
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

  console.log(`[MemberJob] ✓ Matched ${totalMatched} activities to dungeons`);
  for (const dungeon of ACTIVITY_REFERENCE_MAP) {
    const count = activitiesByDungeon[dungeon.hash].length;
    if (count > 0) {
      console.log(`[MemberJob]   - ${dungeon.displayName}: ${count} activities (before dedup)`);
    }
  }

  // Dedupe per dungeon (instanceId-based)
  console.log(`[MemberJob] Step 4: Deduplicating activities per dungeon...`);
  for (const hash of Object.keys(activitiesByDungeon)) {
    const beforeDedup = activitiesByDungeon[hash].length;
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
    const afterDedup = activitiesByDungeon[hash].length;
    
    if (beforeDedup > 0) {
      const dungeonName = ACTIVITY_REFERENCE_MAP.find(d => d.hash === hash)?.displayName || hash;
      console.log(`[MemberJob]   - ${dungeonName}: ${beforeDedup} → ${afterDedup} (removed ${beforeDedup - afterDedup} duplicates)`);
    }
  }

  // 4. Queue per-dungeon jobs
  console.log(`[MemberJob] Step 5: Processing and queueing per-dungeon jobs...`);
  let totalQueued = 0;
  let totalBatches = 0;

  for (const dungeon of ACTIVITY_REFERENCE_MAP) {
    const dungeonHash = dungeon.hash;
    const activities = activitiesByDungeon[dungeonHash] || [];

    if (activities.length === 0) {
      console.log(`[MemberJob]   - ${dungeon.displayName}: No activities, skipping`);
      continue;
    }

    // Filter to completed only
    const completed = activities.filter(a => a?.values?.completed?.basic?.value === 1);
    console.log(`[MemberJob]   - ${dungeon.displayName}: ${completed.length}/${activities.length} completed`);
    
    if (completed.length === 0) {
      console.log(`[MemberJob]   - ${dungeon.displayName}: No completed activities, skipping`);
      continue;
    }

    // Check DB for previous stats and last_processed_date (resume logic)
    console.log(`[MemberJob]   - ${dungeon.displayName}: Checking for previous processing and DB totals...`);
    const prevRow = await env.DB.prepare(`
      SELECT last_processed_date, total_clears, total_full_clears
      FROM member_dungeon_stats
      WHERE clan_id = ? AND membership_id = ? AND dungeon_hash = ?
    `).bind(job.clanId, job.membershipId, dungeonHash).first();

    let cutoffDate: Date | null = null;
    if (prevRow && (prevRow as any).last_processed_date) {
      cutoffDate = new Date((prevRow as any).last_processed_date);
      console.log(`[MemberJob]   - ${dungeon.displayName}: Found previous cutoff: ${cutoffDate.toISOString()}`);
    } else if (job.lastProcessedDate) {
      cutoffDate = new Date(job.lastProcessedDate);
      console.log(`[MemberJob]   - ${dungeon.displayName}: Using job cutoff: ${cutoffDate.toISOString()}`);
    } else {
      console.log(`[MemberJob]   - ${dungeon.displayName}: No previous processing, will process all`);
    }

    // Obtain DB totals (prefer total_clears then total_full_clears)
    const dbTotalClears = prevRow ? Number((prevRow as any).total_clears ?? (prevRow as any).total_full_clears ?? 0) : 0;
    const totalCompletionsFromBungie = completed.length;

    console.log(`[MemberJob]   - ${dungeon.displayName}: DB totals=${dbTotalClears}, Bungie completions=${totalCompletionsFromBungie}`);

    // Quick skip based on counts: if Bungie shows no more completions than DB, skip this dungeon entirely.
    if (totalCompletionsFromBungie <= dbTotalClears) {
      console.log(`[MemberJob]   - ${dungeon.displayName}: Skipping - Bungie completions (${totalCompletionsFromBungie}) <= DB totals (${dbTotalClears})`);
      continue;
    }

    // Sort completed activities by period ascending (oldest first). This ensures we only process new instances
    // and that batching proceeds from oldest→newest (helps consistent last_processed_date updates).
    completed.sort((a, b) => {
      const ta = a.period ? new Date(a.period).getTime() : 0;
      const tb = b.period ? new Date(b.period).getTime() : 0;
      return ta - tb;
    });

    // Filter to new activities using cutoffDate (strict >). This avoids double-processing.
    let newActivities = completed;
    if (cutoffDate) {
      newActivities = completed.filter(a => {
        try {
          const actDate = new Date(a.period);
          return actDate.getTime() > cutoffDate!.getTime();
        } catch {
          // If parsing fails, err on the side of processing (to be safe)
          return true;
        }
      });
      console.log(`[MemberJob]   - ${dungeon.displayName}: Filtered to ${newActivities.length} new activities after ${cutoffDate.toISOString()}`);
    }

    // If nothing remains after date filtering, skip.
    if (newActivities.length === 0) {
      console.log(`[MemberJob]   - ${dungeon.displayName}: ✓ No new activities to process after date filtering`);
      continue;
    }

    console.log(`[MemberJob]   - ${dungeon.displayName}: 📊 Will process ${newActivities.length} new activities (of ${totalCompletionsFromBungie} total from Bungie)`);

    // Prepare activities for queue (ensure period is included)
    const activitiesPayload = newActivities.map(a => ({
      instanceId: a.activityDetails?.instanceId || a.instanceId,
      // seconds here is just informational; authoritative player playtime is extracted from PGCR in worker
      seconds: a.values?.activityDurationSeconds?.basic?.value,
      date: a.period,
      characterId: (a as any).characterId,
    }));

    // Send to STATS_QUEUE
    const jobId = `member-${job.membershipId}-${dungeonHash}`;
    
    // Split into batches of MAX_BATCH_SIZE (process oldest first)
    const MAX_BATCH_SIZE = 25;
    const batches: any[][] = [];
    for (let i = 0; i < activitiesPayload.length; i += MAX_BATCH_SIZE) {
      batches.push(activitiesPayload.slice(i, i + MAX_BATCH_SIZE));
    }

    console.log(`[MemberJob]   - ${dungeon.displayName}: Split into ${batches.length} batch(es) of max ${MAX_BATCH_SIZE}`);

    // Send each batch to queue
    for (let batchIndex = 0; batchIndex < batches.length; batchIndex++) {
      const batchSize = batches[batchIndex].length;
      console.log(`[MemberJob]   - ${dungeon.displayName}: Queueing batch ${batchIndex + 1}/${batches.length} (${batchSize} activities)...`);
      
      await env.STATS_QUEUE.send({
        clanId: job.clanId,
        membershipId: job.membershipId,
        membershipType: job.membershipType,
        dungeonHash,
        activities: batches[batchIndex],
        jobId,
        batchIndex,
        totalBatches: batches.length,
      });
      
      totalQueued += batchSize;
      totalBatches++;
    }

    console.log(`[MemberJob]   - ${dungeon.displayName}: ✓ Queued ${batches.length} batch(es) with ${newActivities.length} activities`);
  }

  const duration = Date.now() - startTime;
  console.log(`\n${'='.repeat(80)}`);
  console.log(`[MemberJob] COMPLETE: ${job.displayName}`);
  console.log(`[MemberJob] - Total activities queued: ${totalQueued}`);
  console.log(`[MemberJob] - Total batches created: ${totalBatches}`);
  console.log(`[MemberJob] - Duration: ${duration}ms (${(duration / 1000).toFixed(2)}s)`);
  console.log(`${'='.repeat(80)}\n`);
}

async function fetchAllActivities(
  membershipType: number,
  membershipId: string,
  characterIds: string[],
  apiKey: string,
  displayName: string
): Promise<Record<string, any[]>> {
  console.log(`[FetchActivities] Starting for ${displayName} (${characterIds.length} characters)`);
  
  const out: Record<string, any[]> = {};
  for (const id of characterIds) out[id] = [];

  const modes = [82, 2]; // Dungeon, Story
  let totalFetched = 0;

  // Helper: fetch all pages for a single character & mode
  async function fetchAllPagesForCharacter(charId: string, mode: number) {
    const modeName = mode === 82 ? 'Dungeon' : mode === 2 ? 'Story' : `Mode ${mode}`;
    console.log(`[FetchActivities]   - Character ${charId} / ${modeName}: Starting pagination...`);
    
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
      ).catch((err) => {
        console.error(`[FetchActivities]   - Character ${charId} / ${modeName} / Page ${page}: ❌ Error:`, err);
        return [];
      });

      if (activities && activities.length > 0) {
        out[charId].push(...activities);
        charTotal += activities.length;
        console.log(`[FetchActivities]   - Character ${charId} / ${modeName} / Page ${page}: ✓ Fetched ${activities.length} activities`);
      } else {
        console.log(`[FetchActivities]   - Character ${charId} / ${modeName} / Page ${page}: No activities returned`);
      }

      // If less than a full page, we're done for this character/mode
      if (!activities || activities.length < pageSize) {
        console.log(`[FetchActivities]   - Character ${charId} / ${modeName}: Complete (${charTotal} total activities)`);
        break;
      }

      page++;
      await sleep(200);
    }
    
    totalFetched += charTotal;
  }

  // Fetch modes sequentially with parallel per-character workers
  for (const mode of modes) {
    const modeName = mode === 82 ? 'Dungeon' : mode === 2 ? 'Story' : `Mode ${mode}`;
    console.log(`[FetchActivities] Starting ${modeName} mode for all ${characterIds.length} character(s)...`);
    
    const workers = characterIds.map((charId) => fetchAllPagesForCharacter(charId, mode));
    await Promise.all(workers);
    
    console.log(`[FetchActivities] ✓ Completed ${modeName} mode for all characters`);
    await sleep(250);
  }

  console.log(`[FetchActivities] Complete: Fetched ${totalFetched} total activities for ${displayName}`);
  return out;
}