// ============================================================================
// FILE: src/processors/memberJobProcessor.ts
// Clean member job processor with MemberCoordinator for batch aggregation
// UPDATED: Added aggregate comparison, parallel fetching, pagination fix
// ============================================================================

import type { Env, MemberJob } from '../types';
import { ACTIVITY_REFERENCE_MAP } from '../constants/activityReferenceMap';
import {
  fetchCharactersForMember,
  fetchActivitiesForCharacter,
  fetchAggregateStatsForCharacter,
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

  console.log(`[MemberJob] Found ${characters.length} character(s) for ${job.displayName}`);
  characters.forEach((char: any, idx: number) => {
    console.log(`  Character ${idx + 1}: ${char.characterId}${char.deleted ? ' (deleted)' : ''}`);
  });

  // NEW: Fetch aggregate targets for comparison
  console.log(`[MemberJob] Fetching aggregate targets for ${job.displayName}...`);

  const aggregateTargets: Record<string, number> = {};

  // Build reference map for looking up dungeon hashes from activity hashes
  const referenceMap: Record<string, { hash: string; displayName: string }> = {};
  for (const dungeon of ACTIVITY_REFERENCE_MAP) {
    for (const refId of dungeon.referenceIds) {
      referenceMap[refId] = {
        hash: dungeon.hash,
        displayName: dungeon.displayName
      };
    }
  }

  // Fetch aggregates for each character
  for (const character of characters) {
    const aggregates = await fetchAggregateStatsForCharacter(
      job.membershipType,
      job.membershipId,
      character.characterId,
      env.BUNGIE_API_KEY
    );
    
    for (const activity of aggregates) {
      const activityHash = String(activity.activityHash || '');
      const completions = Number(activity.values?.activityCompletions?.basic?.value || 0);
      
      // Map activity hash to dungeon hash
      if (activityHash in referenceMap) {
        const dungeonHash = referenceMap[activityHash].hash;
        aggregateTargets[dungeonHash] = (aggregateTargets[dungeonHash] || 0) + completions;
      }
    }
  }

  const totalAggregateTarget = Object.values(aggregateTargets).reduce((sum, v) => sum + v, 0);
  console.log(`[MemberJob] Aggregate targets: ${totalAggregateTarget} total completions`);
  for (const dungeon of ACTIVITY_REFERENCE_MAP) {
    const target = aggregateTargets[dungeon.hash] || 0;
    if (target > 0) {
      console.log(`  ${dungeon.displayName}: ${target} completions`);
    }
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

  // Group by dungeon hash and track ungrouped + missing refIds
  const activitiesByDungeon: Record<string, any[]> = {};
  const ungroupedActivities: any[] = [];
  const missingRefIdActivities: any[] = [];
  let totalProcessed = 0;
  
  for (const dungeon of ACTIVITY_REFERENCE_MAP) {
    activitiesByDungeon[dungeon.hash] = [];
  }

  for (const charId of Object.keys(activitiesByChar)) {
    for (const activity of activitiesByChar[charId]) {
      totalProcessed++;
      const refId = String(activity?.activityDetails?.referenceId || '');
      
      // Track activities without reference IDs
      if (!refId) {
        missingRefIdActivities.push({
          characterId: charId,
          period: activity.period,
          instanceId: activity?.activityDetails?.instanceId || activity?.instanceId,
        });
        continue;
      }
      
      let grouped = false;
      for (const dungeon of ACTIVITY_REFERENCE_MAP) {
        if (dungeon.referenceIds.includes(refId)) {
          activitiesByDungeon[dungeon.hash].push({
            ...activity,
            characterId: charId,
          });
          grouped = true;
          break;
        }
      }
      
      // Track ungrouped activities that have a refId but don't match any dungeon
      if (!grouped) {
        ungroupedActivities.push({
          referenceId: refId,
          characterId: charId,
          period: activity.period,
        });
      }
    }
  }

  console.log(`[MemberJob] Processed ${totalProcessed} activities (should match ${totalActivitiesFetched})`);
  if (totalProcessed !== totalActivitiesFetched) {
    console.warn(`[MemberJob] ⚠️  MISMATCH: Processed ${totalProcessed} but fetched ${totalActivitiesFetched} (diff: ${totalActivitiesFetched - totalProcessed})`);
  }

  // Log missing refId activities
  if (missingRefIdActivities.length > 0) {
    console.log(`[MemberJob] ⚠️  ${missingRefIdActivities.length} activities missing referenceId (skipped)`);
  }

  // Log ungrouped activities summary if any exist
  if (ungroupedActivities.length > 0) {
    console.log(`[MemberJob] ⚠️  ${ungroupedActivities.length} ungrouped activities (not matching any dungeon)`);
    const ungroupedByRefId: Record<string, number> = {};
    for (const act of ungroupedActivities) {
      ungroupedByRefId[act.referenceId] = (ungroupedByRefId[act.referenceId] || 0) + 1;
    }
    console.log(`[MemberJob] Ungrouped by RefID:`, ungroupedByRefId);
  }

  // Dedupe per dungeon
  console.log(`\n[MemberJob] Starting deduplication...`);
  let totalBeforeDedup = 0;
  let totalAfterDedup = 0;
  let totalDuplicatesRemoved = 0;
  
  for (const hash of Object.keys(activitiesByDungeon)) {
    const beforeCount = activitiesByDungeon[hash].length;
    totalBeforeDedup += beforeCount;
    
    const map = new Map<string, any>();
    const instanceIdsWithoutId: any[] = [];
    
    for (const act of activitiesByDungeon[hash]) {
      const id = act.activityDetails?.instanceId || act.instanceId;
      
      if (!id) {
        instanceIdsWithoutId.push({
          characterId: act.characterId,
          period: act.period,
        });
        continue;
      }
      
      const existing = map.get(id);
      if (!existing) {
        map.set(id, act);
      } else {
        // Prefer completed activities
        const existingCompleted = !!(existing?.values?.completed?.basic?.value === 1);
        const newCompleted = !!(act?.values?.completed?.basic?.value === 1);
        if (!existingCompleted && newCompleted) {
          map.set(id, act);
        }
      }
    }
    
    activitiesByDungeon[hash] = Array.from(map.values());
    
    const afterCount = activitiesByDungeon[hash].length;
    totalAfterDedup += afterCount;
    const removed = beforeCount - afterCount;
    totalDuplicatesRemoved += removed;
    
    if (beforeCount > 0) {
      const dungeon = ACTIVITY_REFERENCE_MAP.find(d => d.hash === hash);
      const dungeonName = dungeon?.displayName || hash;
      console.log(`[MemberJob] ${dungeonName}: ${afterCount} unique activities (${removed} duplicates removed${instanceIdsWithoutId.length > 0 ? `, ${instanceIdsWithoutId.length} missing instanceId` : ''})`);
      
      if (instanceIdsWithoutId.length > 0) {
        console.warn(`[MemberJob] ⚠️  ${dungeonName} has ${instanceIdsWithoutId.length} activities without instanceId - these were EXCLUDED from deduplication`);
      }
    }
  }
  
  console.log(`[MemberJob] Deduplication summary: ${totalBeforeDedup} → ${totalAfterDedup} (removed ${totalDuplicatesRemoved} duplicates)`);
  
  // Sanity check: compare to fetched total
  const totalGrouped = totalAfterDedup + ungroupedActivities.length + missingRefIdActivities.length;
  if (totalGrouped !== totalActivitiesFetched) {
    console.warn(`[MemberJob] ⚠️  Activity accounting mismatch!`);
    console.warn(`[MemberJob]   Fetched: ${totalActivitiesFetched}`);
    console.warn(`[MemberJob]   After dedup: ${totalAfterDedup}`);
    console.warn(`[MemberJob]   Ungrouped: ${ungroupedActivities.length}`);
    console.warn(`[MemberJob]   Missing refId: ${missingRefIdActivities.length}`);
    console.warn(`[MemberJob]   Total accounted: ${totalGrouped}`);
    console.warn(`[MemberJob]   Difference: ${totalActivitiesFetched - totalGrouped}`);
  }

  // NEW: Compare against aggregate targets
  console.log(`\n[MemberJob] Comparing against aggregate targets...`);

  const comparisonResults: Array<{
    dungeonHash: string;
    dungeonName: string;
    target: number;
    found: number;
    difference: number;
  }> = [];

  let perfectMatches = 0;
  let totalTarget = 0;
  let totalFound = 0;

  for (const dungeon of ACTIVITY_REFERENCE_MAP) {
    const dungeonHash = dungeon.hash;
    const target = aggregateTargets[dungeonHash] || 0;
    const activities = activitiesByDungeon[dungeonHash] || [];
    const completed = activities.filter(a => a?.values?.completed?.basic?.value === 1);
    const found = completed.length;
    
    if (target > 0 || found > 0) {
      const difference = target - found;
      
      totalTarget += target;
      totalFound += found;
      
      comparisonResults.push({
        dungeonHash,
        dungeonName: dungeon.displayName,
        target,
        found,
        difference
      });
      
      if (difference === 0) {
        perfectMatches++;
      } else {
        console.warn(
          `[MemberJob] ⚠️  ${dungeon.displayName}: ` +
          `Target=${target}, Found=${found}, Diff=${difference > 0 ? '+' : ''}${difference}`
        );
      }
    }
  }

  const totalMissing = totalTarget - totalFound;

  console.log(
    `[MemberJob] Aggregate comparison summary: ` +
    `Target=${totalTarget}, Found=${totalFound}, Missing=${totalMissing}, ` +
    `Matches=${perfectMatches}/${comparisonResults.length}`
  );

  if (totalMissing > 0) {
    console.warn(
      `[MemberJob] ❌ Missing ${totalMissing} completions in history ` +
      `(${((totalMissing/totalTarget)*100).toFixed(1)}% of aggregate)`
    );
    console.warn(`[MemberJob] These completions exist in aggregate but NOT in activity history`);
    console.warn(`[MemberJob] Possible causes:`);
    console.warn(`[MemberJob]   • Activities without instance IDs (not in history)`);
    console.warn(`[MemberJob]   • Private/hidden activities`);
    console.warn(`[MemberJob]   • Activities from before history tracking`);
    console.warn(`[MemberJob]   • API data inconsistency`);
  } else if (totalMissing < 0) {
    console.warn(
      `[MemberJob] ⚠️  Found ${Math.abs(totalMissing)} MORE completions than aggregate ` +
      `(unexpected - investigate)`
    );
  }

  // Calculate total batches across all dungeons
  let totalBatches = 0;
  const dungeonBatchCounts: Record<string, number> = {};
  
  for (const dungeon of ACTIVITY_REFERENCE_MAP) {
    const dungeonHash = dungeon.hash;
    const activities = activitiesByDungeon[dungeonHash] || [];
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

    // Skip if fewer completions than DB (should not happen under normal circumstances)
    // Equal counts are handled via date-based filtering below
    if (completed.length < dbTotalClears) {
      continue;
    }

    completed.sort((a, b) => {
      const ta = a.period ? new Date(a.period).getTime() : 0;
      const tb = b.period ? new Date(b.period).getTime() : 0;
      return ta - tb;
    });

    // Determine which activities are new
    let newActivities = completed;
    
    if (completed.length > dbTotalClears) {
      // We have more completions than before
      // Process only the NEW completions (the last N in chronological order by instance start time)
      // Note: This is a heuristic since 'period' reflects instance start time, not completion time.
      // For the character-switching case, this ensures we process recent instances even if they
      // were started before the cutoff date but completed after.
      // Assumption: Newer instances (by start time) are more likely to be new completions.
      // Edge case: If old instances are completed out of order, they might be missed in this pass
      // but will be caught in the next sync when the count increases further.
      const newCount = completed.length - dbTotalClears;
      newActivities = completed.slice(-newCount);
    } else if (cutoffDate) {
      // Same number of completions, filter by date for incremental updates
      newActivities = completed.filter(a => {
        try {
          const actDate = new Date(a.period);
          return actDate.getTime() > cutoffDate!.getTime();
        } catch {
          return true;
        }
      });
    }

    if (newActivities.length === 0) continue;

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

    // Skip if fewer completions than DB (should not happen under normal circumstances)
    // Equal counts are handled via date-based filtering below
    if (completed.length < dbTotalClears) {
      continue;
    }

    // Sort by period ascending (oldest first)
    completed.sort((a, b) => {
      const ta = a.period ? new Date(a.period).getTime() : 0;
      const tb = b.period ? new Date(b.period).getTime() : 0;
      return ta - tb;
    });

    // Determine which activities are new
    let newActivities = completed;
    
    if (completed.length > dbTotalClears) {
      // We have more completions than before
      // Process only the NEW completions (the last N in chronological order by instance start time)
      // Note: This is a heuristic since 'period' reflects instance start time, not completion time.
      // For the character-switching case, this ensures we process recent instances even if they
      // were started before the cutoff date but completed after.
      // Assumption: Newer instances (by start time) are more likely to be new completions.
      // Edge case: If old instances are completed out of order, they might be missed in this pass
      // but will be caught in the next sync when the count increases further.
      const newCount = completed.length - dbTotalClears;
      newActivities = completed.slice(-newCount);
    } else if (cutoffDate) {
      // Same number of completions, filter by date for incremental updates
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
      await env.STATS_QUEUE.send({
        clanId: job.clanId,
        membershipId: job.membershipId,
        membershipType: job.membershipType,
        dungeonHash,
        activities: batches[batchIndex],
        jobId: `${job.membershipId}-${dungeonHash}-${batchIndex}`,
      });
      
      totalQueued += batches[batchIndex].length;
      totalBatches++;
    }

    console.log(`[MemberJob] Queued ${batches.length} batch(es) for ${dungeon.displayName} (${activitiesPayload.length} activities)`);
  }

  await notifyRunComplete(env, job);

  const duration = Date.now() - startTime;
  console.log(`[MemberJob] COMPLETE: ${job.displayName} | Queued: ${totalQueued} | Batches: ${totalBatches} | ${(duration/1000).toFixed(1)}s`);
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
  let totalRateLimitRetries = 0;

  console.log(`[MemberJob:Fetch] Starting activity fetch for ${characterIds.length} character(s)`);
  characterIds.forEach((charId, idx) => {
    console.log(`  Character ${idx + 1}: ${charId}`);
  });

  // Helper to fetch a single page for a character
  async function fetchPage(charId: string, mode: number, page: number): Promise<any[]> {
    return await withRateLimit(
      async () => {
        try {
          return await fetchActivitiesForCharacter(
            membershipType,
            membershipId,
            charId,
            page,
            mode,
            250,
            apiKey
          );
        } catch (err: any) {
          if (err.message && (err.message.includes('429') || err.message.includes('rate limit'))) {
            totalRateLimitRetries++;
            console.warn(`[MemberJob:Fetch] RATE LIMITED - Retry #${totalRateLimitRetries}`);
          }
          throw err;
        }
      },
      3
    ).catch((err) => {
      console.warn(`[MemberJob:Fetch] FAILED - CharID: ${charId}, Mode: ${mode}, Page: ${page}: ${err}`);
      return [];
    });
  }

  for (const mode of modes) {
    const modeName = mode === 82 ? 'Dungeon' : mode === 2 ? 'Story' : `Mode-${mode}`;
    console.log(`[MemberJob:Fetch] Fetching mode ${mode} (${modeName})...`);
    
    // Track which characters still need more pages
    const activeCharacters = new Set(characterIds);
    let currentPage = 0;
    
    while (activeCharacters.size > 0 && currentPage <= 100) {
      // Fetch current page for ALL active characters in parallel
      const pagePromises = Array.from(activeCharacters).map(charId => 
        fetchPage(charId, mode, currentPage).then(activities => ({ charId, activities }))
      );
      
      const results = await Promise.allSettled(pagePromises);
      
      // Process results
      for (const result of results) {
        if (result.status === 'fulfilled') {
          const { charId, activities } = result.value;
          
          if (activities && activities.length > 0) {
            out[charId].push(...activities);
            
            // CRITICAL FIX: Stop only on empty response, NOT when length < 250
            if (activities.length === 0) {
              activeCharacters.delete(charId);
            }
          } else {
            // Empty response - character is done
            activeCharacters.delete(charId);
          }
        } else {
          // On error, we could track and retry, but for now mark as done to avoid infinite loops
          console.error(`[MemberJob:Fetch] Error in parallel fetch:`, result.reason);
        }
      }
      
      console.log(
        `[MemberJob:Fetch] Mode ${mode} Page ${currentPage}: ` +
        `${activeCharacters.size}/${characterIds.length} characters still active`
      );
      
      currentPage++;
      
      if (activeCharacters.size > 0) {
        await sleep(200); // Rate limit delay between page waves
      }
    }
  }

  const totalActivities = Object.values(out).reduce((sum, acts) => sum + acts.length, 0);
  console.log(`[MemberJob:Fetch] Complete: ${totalActivities} total activities fetched`);
  if (totalRateLimitRetries > 0) {
    console.warn(`[MemberJob:Fetch] ⚠️  Total rate limit retries: ${totalRateLimitRetries}`);
  }

  return out;
}