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
  console.log(`[MemberJob] Processing ${job.displayName}`);

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

  // Fetch aggregate targets for comparison
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

  // Log only warnings for mismatches
  if (totalProcessed !== totalActivitiesFetched) {
    console.warn(`[MemberJob] Activity count mismatch: processed ${totalProcessed}, fetched ${totalActivitiesFetched}`);
  }

  if (missingRefIdActivities.length > 0) {
    console.warn(`[MemberJob] ${missingRefIdActivities.length} activities missing referenceId (skipped)`);
  }

  if (ungroupedActivities.length > 0) {
    const ungroupedByRefId: Record<string, number> = {};
    for (const act of ungroupedActivities) {
      ungroupedByRefId[act.referenceId] = (ungroupedByRefId[act.referenceId] || 0) + 1;
    }
    console.warn(`[MemberJob] ${ungroupedActivities.length} ungrouped activities:`, ungroupedByRefId);
  }

  // Dedupe per dungeon
  let totalBeforeDedup = 0;
  let totalAfterDedup = 0;
  
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
    totalAfterDedup += activitiesByDungeon[hash].length;
    
    if (instanceIdsWithoutId.length > 0) {
      const dungeon = ACTIVITY_REFERENCE_MAP.find(d => d.hash === hash);
      console.warn(`[MemberJob] ${dungeon?.displayName || hash}: ${instanceIdsWithoutId.length} activities without instanceId (excluded)`);
    }
  }

  // Compare against aggregate targets
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
      
      if (difference === 0) {
        perfectMatches++;
      } else if (difference !== 0) {
        console.warn(
          `[MemberJob] ${dungeon.displayName}: Target=${target}, Found=${found}, Diff=${difference > 0 ? '+' : ''}${difference}`
        );
      }
    }
  }

  const totalMissing = totalTarget - totalFound;
  if (totalMissing > 0) {
    console.warn(`[MemberJob] Missing ${totalMissing} completions (${((totalMissing/totalTarget)*100).toFixed(1)}% of aggregate)`);
  }

  // Queue per-dungeon jobs
  let totalQueued = 0;
  const dungeonBatchCounts: Record<string, number> = {};

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

    // Skip if fewer completions than DB (data inconsistency)
    if (completed.length < dbTotalClears) {
      console.warn(`[MemberJob] ${dungeon.displayName}: Fewer completions than DB (${completed.length} < ${dbTotalClears})`);
      continue;
    }

    // Sort by period ascending (oldest first) BEFORE filtering
    completed.sort((a, b) => {
      const ta = a.period ? new Date(a.period).getTime() : 0;
      const tb = b.period ? new Date(b.period).getTime() : 0;
      return ta - tb;
    });

    // Determine which activities are new using date-based filtering
    // This ensures we only process activities that haven't been seen before
    let newActivities = completed;
    
    if (cutoffDate) {
      // We have a cutoff date - filter to activities AFTER that date
      // This is the correct approach: after deduplication, filter by date
      newActivities = completed.filter(a => {
        try {
          const actDate = new Date(a.period);
          return actDate.getTime() > cutoffDate!.getTime();
        } catch {
          return true;
        }
      });
    } else if (dbTotalClears > 0) {
      // No cutoff date but we have a DB count - this shouldn't happen normally
      // Use count-based as fallback (process activities beyond what's in DB)
      if (completed.length > dbTotalClears) {
        const newCount = completed.length - dbTotalClears;
        newActivities = completed.slice(-newCount);
      } else {
        newActivities = [];
      }
    }
    // If no cutoff date and no DB count, this is an initial sync - process all

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

    dungeonBatchCounts[dungeonHash] = batches.length;

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
    }

    console.log(`[MemberJob] Queued ${batches.length} batch(es) for ${dungeon.displayName} (${activitiesPayload.length} new activities)`);
  }

  // Initialize MemberCoordinator if we have batches to process
  const totalBatches = Object.values(dungeonBatchCounts).reduce((sum, count) => sum + count, 0);
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

  await notifyRunComplete(env, job);

  const duration = Date.now() - startTime;
  console.log(`[MemberJob] Complete: ${job.displayName} | ${totalQueued} activities queued | ${(duration/1000).toFixed(1)}s`);
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
          }
          throw err;
        }
      },
      3
    ).catch((err) => {
      return [];
    });
  }

  for (const mode of modes) {
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
          // On error, mark as done to avoid infinite loops
        }
      }
      
      currentPage++;
      
      if (activeCharacters.size > 0) {
        await sleep(200); // Rate limit delay between page waves
      }
    }
  }

  if (totalRateLimitRetries > 0) {
    console.warn(`[MemberJob:Fetch] Total rate limit retries: ${totalRateLimitRetries}`);
  }

  return out;
}