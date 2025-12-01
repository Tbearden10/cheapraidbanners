// ============================================================================
// FILE: src/processors/memberJobProcessor.ts
// Processes a single member - fetches activities and queues per-dungeon jobs
// NOW WITH: Activity splitting to avoid 15-minute timeout
// Minimal change: keep original flow but use stronger filtering and smaller
// in-memory objects when fetching pages to avoid OOM. No page caps.
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
  console.log(`[MemberJob] START Processing member: ${job.displayName} (${job.membershipId})`);

  // 1. Fetch characters
  const characters = await fetchCharactersForMember(
    job.membershipId,
    job.membershipType,
    env.BUNGIE_API_KEY
  ).catch((err) => {
    console.error(`[MemberJob] ❌ Failed to fetch characters for ${job.displayName}:`, err);
    return [];
  });

  if (!characters || characters.length === 0) {
    console.log(`[MemberJob] No characters for ${job.displayName} - skipping`);
    return;
  }

  const characterIds = characters.map((c: any) => c.characterId);

  // 2. Fetch ALL activities (dungeon + story modes)
  const activitiesByChar = await fetchAllActivities(
    job.membershipType,
    job.membershipId,
    characterIds,
    env.BUNGIE_API_KEY,
    job.displayName
  ).catch((err) => {
    console.error(`[MemberJob] ❌ Failed to fetch activities for ${job.displayName}:`, err);
    return characterIds.reduce((acc: Record<string, any[]>, id) => { acc[id] = []; return acc; }, {});
  });

  const totalActivitiesFetched = Object.values(activitiesByChar).reduce((sum, acts) => sum + acts.length, 0);

  // 3. Group by dungeon
  const activitiesByDungeon: Record<string, any[]> = {};
  for (const dungeon of ACTIVITY_REFERENCE_MAP) {
    activitiesByDungeon[dungeon.hash] = [];
  }

  for (const charId of Object.keys(activitiesByChar)) {
    for (const activity of activitiesByChar[charId]) {
      const refId = String(activity?.activityDetails?.referenceId || '');
      for (const dungeon of ACTIVITY_REFERENCE_MAP) {
        if (dungeon.referenceIds.includes(refId)) {
          activitiesByDungeon[dungeon.hash].push({
            ...activity,
            characterId: charId,
          });
          break;
        }
      }
    }
  }

  // Dedupe per dungeon (instanceId-based)
  for (const hash of Object.keys(activitiesByDungeon)) {
    const map = new Map<string, any>();
    for (const act of activitiesByDungeon[hash]) {
      const id = act.activityDetails?.instanceId || act.instanceId;
      if (!id) continue;
      const existing = map.get(id);
      if (!existing) {
        map.set(id, act);
      } else {
        const existingCompleted = !!(existing?.values?.completed === true);
        const newCompleted = !!(act?.values?.completed === true);
        if (!existingCompleted && newCompleted) {
          map.set(id, act);
        }
      }
    }
    activitiesByDungeon[hash] = Array.from(map.values());
  }

  // 4. Queue per-dungeon jobs WITH SPLITTING LOGIC
  let totalQueued = 0;
  let totalBatches = 0;

  // Configuration for splitting large jobs
  const PGCR_BATCH_SIZE = 25;              // Activities per PGCR fetch batch
  const BATCH_PROCESSING_TIME = 10;        // Seconds per batch (measured)
  const MAX_QUEUE_JOB_DURATION = 8 * 60;   // 8 minutes (safe margin under 15 min)
  
  // Calculate max activities per STATS_QUEUE job
  const MAX_ACTIVITIES_PER_JOB = Math.floor(
    (MAX_QUEUE_JOB_DURATION / BATCH_PROCESSING_TIME) * PGCR_BATCH_SIZE
  );
  // Result: (8*60 / 10) * 25 = 48 * 25 = 1,200 activities per job

  for (const dungeon of ACTIVITY_REFERENCE_MAP) {
    const dungeonHash = dungeon.hash;
    const dungeonName = dungeon.displayName;
    const activities = activitiesByDungeon[dungeonHash] || [];

    if (activities.length === 0) {
      continue;
    }

    // Filter to completed only
    const completed = activities.filter(a => a?.values?.completed === true);
    if (completed.length === 0) continue;

    // Check DB for previous stats and last_processed_date (resume logic)
    const prevRow = await env.DB.prepare(`
      SELECT last_processed_date, total_clears, total_full_clears
      FROM member_dungeon_stats
      WHERE clan_id = ? AND membership_id = ? AND dungeon_hash = ?
    `).bind(job.clanId, job.membershipId, dungeonHash).first();

    let cutoffDate: Date | null = null;
    if (prevRow && (prevRow as any).last_processed_date) {
      cutoffDate = new Date((prevRow as any).last_processed_date);
    } else if (job.lastProcessedDate) {
      cutoffDate = new Date(job.lastProcessedDate);
    }

    const dbTotalClears = prevRow ? Number((prevRow as any).total_clears ?? (prevRow as any).total_full_clears ?? 0) : 0;
    const totalCompletionsFromBungie = completed.length;

    // Quick skip based on counts
    if (totalCompletionsFromBungie <= dbTotalClears) {
      continue;
    }

    // Sort completed activities by period ascending (oldest first)
    completed.sort((a, b) => {
      const ta = a.period ? new Date(a.period).getTime() : 0;
      const tb = b.period ? new Date(b.period).getTime() : 0;
      return ta - tb;
    });

    // Filter to new activities using cutoffDate (strict >)
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

    console.log(`[MemberJob] ${dungeonName}: ${newActivities.length} new activities to process`);

    // Prepare activities for queue
    const activitiesPayload = newActivities.map(a => ({
      instanceId: a.activityDetails?.instanceId || a.instanceId,
      seconds: a.values?.activityDurationSeconds || 0,
      date: a.period,
      characterId: (a as any).characterId,
    }));

    const jobId = `member-${job.membershipId}-${dungeonHash}`;

    // ========================================================================
    // KEY CHANGE: Split large activity sets into multiple jobs
    // ========================================================================

    if (activitiesPayload.length <= MAX_ACTIVITIES_PER_JOB) {
      // CASE 1: Small enough to process in one job
      const batches: any[][] = [];
      for (let i = 0; i < activitiesPayload.length; i += PGCR_BATCH_SIZE) {
        batches.push(activitiesPayload.slice(i, i + PGCR_BATCH_SIZE));
      }

      for (let batchIndex = 0; batchIndex < batches.length; batchIndex++) {
        try {
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
          totalQueued += batches[batchIndex].length;
          totalBatches++;
        } catch (err) {
          console.error(`[MemberJob] ❌ Failed to queue batch for ${job.displayName}/${dungeonName}:`, err);
        }
      }

    } else {
      // CASE 2: Too large - split into multiple independent jobs
      const numJobs = Math.ceil(activitiesPayload.length / MAX_ACTIVITIES_PER_JOB);
      
      console.log(
        `[MemberJob] ⚠️ Large activity set for ${job.displayName}/${dungeonName}: ` +
        `${activitiesPayload.length} activities → split into ${numJobs} jobs ` +
        `(max ${MAX_ACTIVITIES_PER_JOB} per job)`
      );

      for (let jobIndex = 0; jobIndex < numJobs; jobIndex++) {
        const jobStart = jobIndex * MAX_ACTIVITIES_PER_JOB;
        const jobEnd = Math.min(jobStart + MAX_ACTIVITIES_PER_JOB, activitiesPayload.length);
        const jobActivities = activitiesPayload.slice(jobStart, jobEnd);

        // Each sub-job gets its own jobId and BatchCoordinator instance
        const subJobId = `${jobId}-part${jobIndex}`;

        // Split this sub-job into batches
        const batches: any[][] = [];
        for (let i = 0; i < jobActivities.length; i += PGCR_BATCH_SIZE) {
          batches.push(jobActivities.slice(i, i + PGCR_BATCH_SIZE));
        }

        // Send all batches for this sub-job
        for (let batchIndex = 0; batchIndex < batches.length; batchIndex++) {
          try {
            await env.STATS_QUEUE.send({
              clanId: job.clanId,
              membershipId: job.membershipId,
              membershipType: job.membershipType,
              dungeonHash,
              activities: batches[batchIndex],
              jobId: subJobId,
              batchIndex,
              totalBatches: batches.length,
              // Metadata to track this is part of a larger set
              isPartialJob: true,
              partIndex: jobIndex,
              totalParts: numJobs,
            });
            totalQueued += batches[batchIndex].length;
            totalBatches++;
          } catch (err) {
            console.error(
              `[MemberJob] ❌ Failed to queue batch ${batchIndex} of sub-job ${jobIndex}/${dungeonName}:`, 
              err
            );
          }
        }
      }
    }
  }

  const duration = Date.now() - startTime;
  console.log(
    `[MemberJob] COMPLETE: ${job.displayName} - ` +
    `Queued ${totalQueued} activities in ${totalBatches} batch(es) - ` +
    `${Math.round(duration / 1000)}s`
  );
}

/**
 * Replacement for the original fetchAllActivities that preserves behavior but:
 * - Filters pages immediately to only activity referenceIds in ACTIVITY_REFERENCE_MAP
 * - Stores a much smaller "minimal" object per activity
 * - Dedupe per-character by instanceId while streaming pages (avoids duplicated pages)
 * - DOES NOT impose any page cap (continues until API returns less than page size)
 */
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

  // Build a set of tracked referenceIds for fast filtering
  const trackedRefs = new Set<string>();
  for (const dungeon of ACTIVITY_REFERENCE_MAP) {
    for (const ref of dungeon.referenceIds) {
      trackedRefs.add(String(ref));
    }
  }

  // Helper: fetch all pages for a single character & mode
  async function fetchAllPagesForCharacter(charId: string, mode: number, seenInstanceIds: Set<string>) {
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
        console.error(`[FetchActivities] Error fetching char ${charId} mode ${mode} page ${page}:`, err);
        return [];
      });

      if (activities && activities.length > 0) {
        for (const a of activities) {
          try {
            const refId = String(a?.activityDetails?.referenceId || a.referenceId || '');
            if (!trackedRefs.has(refId)) continue; // skip unrelated activities

            const instanceId = a.activityDetails?.instanceId || a.instanceId;
            if (!instanceId) continue;

            // Dedupe per-character (avoids storing duplicates across pages/modes)
            if (seenInstanceIds.has(instanceId)) continue;
            seenInstanceIds.add(instanceId);

            // Keep only the minimal fields required by downstream logic
            const minimal = {
              activityDetails: {
                instanceId,
                referenceId: refId,
              },
              period: a.period,
              // only keep the few numeric values we actually use (avoid full nested object)
              values: {
                completed: !!(a.values?.completed?.basic?.value === 1),
                activityDurationSeconds: Number(a.values?.activityDurationSeconds?.basic?.value || 0),
                timePlayedSeconds: Number(a.values?.timePlayedSeconds?.basic?.value || 0),
              },
              // preserve instance-level fields used elsewhere if needed
              characterId: charId,
            };

            out[charId].push(minimal);
            charTotal++;
            totalFetched++;
          } catch {
            // ignore single-item processing errors
            continue;
          }
        }
      }

      // If less than a full page, we're done for this character/mode
      if (!activities || activities.length < pageSize) {
        break;
      }

      page++;
      await sleep(200);
    }

    // accumulate charTotal if needed (we track totalFetched globally)
  }

  // Fetch modes sequentially with parallel per-character workers
  for (const mode of modes) {
    // Pass a fresh seenInstanceIds per character that persists across pages and modes
    const workers = characterIds.map((charId) => {
      const seenInstanceIds = new Set<string>();
      return fetchAllPagesForCharacter(charId, mode, seenInstanceIds);
    });
    await Promise.all(workers);
    await sleep(250);
  }

  console.log(`[FetchActivities] Fetched ${totalFetched} relevant activities for ${displayName}`);
  return out;
}