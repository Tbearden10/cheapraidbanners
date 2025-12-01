// ============================================================================
// FILE: src/processors/memberJobProcessor.ts
// REWRITTEN FROM SCRATCH: Ultra-simple queue pipeline
// 
// Flow: For each member, we queue ONE job per dungeon (not one per member)
// This ensures each queue invocation completes fast (< 2 min typically)
// 
// Phase 1: processMemberJob - fetches activities, queues DungeonJob messages
// Phase 2: processDungeonJob - fetches PGCRs for ONE dungeon, writes to DB
// ============================================================================

import type { Env, MemberJob } from '../types';
import { ACTIVITY_REFERENCE_MAP } from '../constants/activityReferenceMap';
import {
  fetchCharactersForMember,
  fetchActivitiesForCharacter,
  fetchPGCR,
  withRateLimit,
} from '../api/bungieApi';
import { upsertMemberDungeonStats } from '../db/queries';
import { applyClanAggregateDelta } from '../db/aggregateHelpers';
import { trackRunProgress } from '../kv/runTracker';

const sleep = (ms: number) => new Promise(r => setTimeout(r, ms));

// ============================================================================
// JOB TYPES
// ============================================================================

export interface DungeonJob {
  type: 'dungeon';
  clanId: string;
  membershipId: string;
  membershipType: number;
  displayName: string;
  dungeonHash: string;
  dungeonName: string;
  // Activity instance IDs to process (already filtered to new/completed only)
  activities: Array<{ instanceId: string; period: string }>;
  runId?: string | null;
}

// ============================================================================
// CONSTANTS
// ============================================================================

const BEYOND_LIGHT_START_MS = Date.parse('2020-11-10T17:00:00.000Z');
const WITCH_QUEEN_START_MS = Date.parse('2022-02-22T17:00:00.000Z');
const HAUNTED_START_MS = Date.parse('2022-05-24T17:00:00.000Z');

function determineClearType(pgcr: any, period: string): boolean {
  const timestamp = Date.parse(period);
  if (timestamp >= HAUNTED_START_MS) {
    return Boolean(pgcr.activityWasStartedFromBeginning);
  }
  if (timestamp < BEYOND_LIGHT_START_MS) {
    return pgcr.startingPhaseIndex === 0;
  }
  if (timestamp >= WITCH_QUEEN_START_MS) {
    return Boolean(pgcr.activityWasStartedFromBeginning);
  }
  return true;
}

// ============================================================================
// PHASE 1: Process Member - fetch activities, queue dungeon jobs
// ============================================================================

export async function processMemberJob(env: Env, job: MemberJob): Promise<void> {
  const startTime = Date.now();
  console.log(`[Member] START ${job.displayName} (${job.membershipId})`);

  // 1. Fetch characters
  const characters = await fetchCharactersForMember(
    job.membershipId,
    job.membershipType,
    env.BUNGIE_API_KEY
  ).catch((err) => {
    console.error(`[Member] ❌ Failed to fetch characters:`, err);
    return [];
  });

  if (!characters || characters.length === 0) {
    console.log(`[Member] No characters - skipping`);
    if (job.runId) {
      await trackRunProgress(env.RUN_TRACKING_KV, job.runId, { processed: 1 }).catch(() => {});
    }
    return;
  }

  // 2. Fetch activities for all characters (quick - just activity list, no PGCRs)
  const allActivities = await fetchMemberActivities(
    job.membershipType,
    job.membershipId,
    characters.map((c: any) => c.characterId),
    env.BUNGIE_API_KEY
  );

  // 3. Group by dungeon and filter to new activities only
  let dungeonJobsQueued = 0;

  for (const dungeon of ACTIVITY_REFERENCE_MAP) {
    const dungeonActivities = allActivities.filter(a => 
      dungeon.referenceIds.includes(a.referenceId) && a.completed
    );

    if (dungeonActivities.length === 0) continue;

    // Check what we've already processed
    const prevRow = await env.DB.prepare(`
      SELECT last_processed_date, total_clears
      FROM member_dungeon_stats
      WHERE clan_id = ? AND membership_id = ? AND dungeon_hash = ?
    `).bind(job.clanId, job.membershipId, dungeon.hash).first();

    const dbClears = prevRow ? Number((prevRow as any).total_clears || 0) : 0;
    
    // Quick skip if counts match
    if (dungeonActivities.length <= dbClears) continue;

    // Filter to new activities using cutoff date
    let cutoffDate: Date | null = null;
    if (prevRow && (prevRow as any).last_processed_date) {
      cutoffDate = new Date((prevRow as any).last_processed_date);
    }

    const newActivities = cutoffDate 
      ? dungeonActivities.filter(a => new Date(a.period).getTime() > cutoffDate!.getTime())
      : dungeonActivities;

    if (newActivities.length === 0) continue;

    // Queue a dungeon job
    const dungeonJob: DungeonJob = {
      type: 'dungeon',
      clanId: job.clanId,
      membershipId: job.membershipId,
      membershipType: job.membershipType,
      displayName: job.displayName,
      dungeonHash: dungeon.hash,
      dungeonName: dungeon.displayName,
      activities: newActivities.map(a => ({ instanceId: a.instanceId, period: a.period })),
      runId: job.runId,
    };

    await env.MEMBER_STATS_QUEUE.send(dungeonJob);
    dungeonJobsQueued++;
    console.log(`[Member] Queued ${dungeon.displayName}: ${newActivities.length} activities`);
  }

  // Mark member as processed if no dungeon jobs were queued
  if (dungeonJobsQueued === 0 && job.runId) {
    await trackRunProgress(env.RUN_TRACKING_KV, job.runId, { processed: 1 }).catch(() => {});
  }

  const duration = Date.now() - startTime;
  console.log(`[Member] DONE ${job.displayName} - queued ${dungeonJobsQueued} dungeon jobs (${Math.round(duration / 1000)}s)`);
}

// ============================================================================
// PHASE 2: Process Dungeon - fetch PGCRs, write stats
// ============================================================================

export async function processDungeonJob(env: Env, job: DungeonJob): Promise<void> {
  const startTime = Date.now();
  console.log(`[Dungeon] START ${job.displayName}/${job.dungeonName}: ${job.activities.length} activities`);

  // Fetch PGCRs in batches
  const BATCH_SIZE = 5;  // Small batches to stay well under limits
  const DELAY_MS = 250;

  let fullClears = 0;
  let totalPlaytime = 0;
  let successCount = 0;
  let latestDate: string | null = null;

  for (let i = 0; i < job.activities.length; i += BATCH_SIZE) {
    const batch = job.activities.slice(i, i + BATCH_SIZE);

    const results = await Promise.allSettled(
      batch.map(async (activity) => {
        const pgcr = await fetchPGCR(activity.instanceId, env.BUNGIE_API_KEY);
        if (!pgcr) return null;

        const isFullClear = determineClearType(pgcr, activity.period);

        // Find this player's playtime
        let playtime = 0;
        const entry = (pgcr.entries || []).find((e: any) =>
          e?.player?.destinyUserInfo?.membershipId === job.membershipId
        );
        if (entry?.values?.timePlayedSeconds?.basic?.value) {
          playtime = Number(entry.values.timePlayedSeconds.basic.value);
        }

        return { isFullClear, playtime, period: activity.period };
      })
    );

    for (const result of results) {
      if (result.status === 'fulfilled' && result.value) {
        successCount++;
        if (result.value.isFullClear) fullClears++;
        totalPlaytime += result.value.playtime;
        if (!latestDate || result.value.period > latestDate) {
          latestDate = result.value.period;
        }
      }
    }

    // Rate limit
    if (i + BATCH_SIZE < job.activities.length) {
      await sleep(DELAY_MS);
    }
  }

  if (successCount === 0) {
    console.log(`[Dungeon] No PGCRs succeeded - skipping DB write`);
    return;
  }

  // Write to DB
  const prevRow = await env.DB.prepare(`
    SELECT total_clears, total_full_clears, total_playtime_seconds, last_processed_date
    FROM member_dungeon_stats
    WHERE clan_id = ? AND membership_id = ? AND dungeon_hash = ?
  `).bind(job.clanId, job.membershipId, job.dungeonHash).first();

  const isNew = !prevRow;
  const prevClears = prevRow ? Number((prevRow as any).total_clears || 0) : 0;
  const prevFullClears = prevRow ? Number((prevRow as any).total_full_clears || 0) : 0;
  const prevPlaytime = prevRow ? Number((prevRow as any).total_playtime_seconds || 0) : 0;
  const prevDate = prevRow ? (prevRow as any).last_processed_date : null;

  const newClears = prevClears + successCount;
  const newFullClears = prevFullClears + fullClears;
  const newPlaytime = prevPlaytime + totalPlaytime;
  const finalDate = latestDate && (!prevDate || latestDate > prevDate) ? latestDate : prevDate;

  try {
    await upsertMemberDungeonStats(env.DB, {
      clanId: job.clanId,
      membershipId: job.membershipId,
      membershipType: job.membershipType,
      dungeonHash: job.dungeonHash,
      totalClears: newClears,
      totalFullClears: newFullClears,
      totalPlaytimeSeconds: newPlaytime,
      lastProcessedDate: finalDate,
    });

    await applyClanAggregateDelta(
      env.DB,
      job.clanId,
      job.dungeonHash,
      successCount,
      fullClears,
      totalPlaytime,
      isNew
    );

    console.log(`[Dungeon] ✅ ${job.dungeonName}: +${successCount} clears, +${fullClears} full`);
  } catch (err) {
    console.error(`[Dungeon] ❌ DB write failed:`, err);
    throw err; // Will trigger retry
  }

  // Track progress
  if (job.runId) {
    await trackRunProgress(env.RUN_TRACKING_KV, job.runId, { processed: 1 }).catch(() => {});
  }

  const duration = Date.now() - startTime;
  console.log(`[Dungeon] DONE ${job.displayName}/${job.dungeonName} (${Math.round(duration / 1000)}s)`);
}

// ============================================================================
// HELPER: Fetch all dungeon activities for a member (fast, no PGCRs)
// ============================================================================

interface MinimalActivity {
  instanceId: string;
  referenceId: string;
  period: string;
  completed: boolean;
}

async function fetchMemberActivities(
  membershipType: number,
  membershipId: string,
  characterIds: string[],
  apiKey: string
): Promise<MinimalActivity[]> {
  const trackedRefs = new Set<string>();
  for (const dungeon of ACTIVITY_REFERENCE_MAP) {
    for (const ref of dungeon.referenceIds) {
      trackedRefs.add(String(ref));
    }
  }

  const seenInstances = new Set<string>();
  const activities: MinimalActivity[] = [];

  // Fetch dungeon mode (82) for each character
  for (const charId of characterIds) {
    let page = 0;
    const pageSize = 250;

    while (true) {
      const pageActivities = await withRateLimit(
        () => fetchActivitiesForCharacter(membershipType, membershipId, charId, page, 82, pageSize, apiKey),
        2
      ).catch(() => []);

      for (const a of pageActivities) {
        const refId = String(a?.activityDetails?.referenceId || '');
        const instanceId = String(a?.activityDetails?.instanceId || '');
        
        if (!trackedRefs.has(refId) || !instanceId || seenInstances.has(instanceId)) continue;
        seenInstances.add(instanceId);

        activities.push({
          instanceId,
          referenceId: refId,
          period: a.period || '',
          completed: a.values?.completed?.basic?.value === 1,
        });
      }

      if (pageActivities.length < pageSize) break;
      page++;
      await sleep(150);
    }
  }

  return activities;
}