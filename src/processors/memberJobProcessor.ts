// ============================================================================
// FILE: src/processors/memberJobProcessor.ts
// SIMPLIFIED: Processes a single member completely within one worker invocation
// - Fetches characters and activities
// - Fetches PGCRs for new activities  
// - Writes stats directly to DB
// - No intermediate queuing or Durable Object coordination
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

// Constants for PGCR full clear determination
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
    if (job.runId) {
      await trackRunProgress(env.RUN_TRACKING_KV, job.runId, { processed: 1 }).catch(() => {});
    }
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

  // 4. Process each dungeon's activities DIRECTLY (no intermediate queue)
  let totalProcessed = 0;
  let totalWritten = 0;

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

    // Check DB for previous stats
    const prevRow = await env.DB.prepare(`
      SELECT last_processed_date, total_clears, total_full_clears, total_playtime_seconds
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

    // Quick skip if no new activities
    if (completed.length <= dbTotalClears) {
      continue;
    }

    // Sort by period ascending (oldest first)
    completed.sort((a, b) => {
      const ta = a.period ? new Date(a.period).getTime() : 0;
      const tb = b.period ? new Date(b.period).getTime() : 0;
      return ta - tb;
    });

    // Filter to new activities using cutoffDate
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

    console.log(`[MemberJob] ${dungeonName}: Processing ${newActivities.length} new activities`);

    // ========================================================================
    // DIRECT PROCESSING: Fetch PGCRs and calculate stats inline
    // ========================================================================
    const PGCR_BATCH_SIZE = 10;
    const DELAY_MS = 300;

    let fullClearsFound = 0;
    let totalPlaytime = 0;
    let latestActivityDate: string | null = null;
    let pgcrSuccessCount = 0;

    for (let i = 0; i < newActivities.length; i += PGCR_BATCH_SIZE) {
      const batch = newActivities.slice(i, i + PGCR_BATCH_SIZE);

      const results = await Promise.allSettled(
        batch.map(async (activity) => {
          const instanceId = activity.activityDetails?.instanceId || activity.instanceId;
          const pgcr = await fetchPGCR(instanceId, env.BUNGIE_API_KEY);
          if (!pgcr) return null;

          const isFullClear = determineClearType(pgcr, activity.period || '');

          let playtime = 0;
          try {
            const entries = pgcr.entries || [];
            const match = entries.find((e: any) => 
              e?.player?.destinyUserInfo?.membershipId === job.membershipId
            );
            if (match && Number.isFinite(Number(match.values?.timePlayedSeconds?.basic?.value))) {
              playtime = Number(match.values.timePlayedSeconds.basic.value);
            }
          } catch {
            playtime = 0;
          }

          return { isFullClear, playtime, date: activity.period };
        })
      );

      for (const result of results) {
        if (result.status === 'fulfilled' && result.value) {
          pgcrSuccessCount++;
          if (result.value.isFullClear) {
            fullClearsFound++;
          }
          totalPlaytime += result.value.playtime || 0;
          if (result.value.date && (!latestActivityDate || result.value.date > latestActivityDate)) {
            latestActivityDate = result.value.date;
          }
        }
      }

      // Rate limit between batches
      if (i + PGCR_BATCH_SIZE < newActivities.length) {
        await sleep(DELAY_MS);
      }
    }

    totalProcessed += pgcrSuccessCount;

    if (pgcrSuccessCount === 0) {
      console.log(`[MemberJob] ${dungeonName}: No PGCRs fetched, skipping DB write`);
      continue;
    }

    // ========================================================================
    // WRITE STATS DIRECTLY TO DB
    // ========================================================================
    const isNewRecord = !prevRow;
    const prevClears = prevRow ? Number((prevRow as any).total_clears || 0) : 0;
    const prevFullClears = prevRow ? Number((prevRow as any).total_full_clears || 0) : 0;
    const prevPlaytime = prevRow ? Number((prevRow as any).total_playtime_seconds || 0) : 0;
    const prevLastProcessedDate = prevRow ? (prevRow as any).last_processed_date : null;

    const newTotalClears = prevClears + pgcrSuccessCount;
    const newTotalFullClears = prevFullClears + fullClearsFound;
    const newTotalPlaytime = prevPlaytime + totalPlaytime;

    let finalLastProcessedDate = prevLastProcessedDate;
    if (latestActivityDate) {
      if (!finalLastProcessedDate || latestActivityDate > finalLastProcessedDate) {
        finalLastProcessedDate = latestActivityDate;
      }
    }

    try {
      await upsertMemberDungeonStats(env.DB, {
        clanId: job.clanId,
        membershipId: job.membershipId,
        membershipType: job.membershipType,
        dungeonHash,
        totalClears: newTotalClears,
        totalFullClears: newTotalFullClears,
        totalPlaytimeSeconds: newTotalPlaytime,
        lastProcessedDate: finalLastProcessedDate,
      });

      await applyClanAggregateDelta(
        env.DB,
        job.clanId,
        dungeonHash,
        pgcrSuccessCount,
        fullClearsFound,
        totalPlaytime,
        isNewRecord
      );

      totalWritten++;

      console.log(
        `[MemberJob] ✅ ${dungeonName}: clears=${newTotalClears} (+${pgcrSuccessCount}) ` +
        `fullClears=${newTotalFullClears} (+${fullClearsFound})`
      );
    } catch (err) {
      console.error(`[MemberJob] ❌ Failed to write stats for ${dungeonName}:`, err);
    }
  }

  // Track progress
  if (job.runId) {
    await trackRunProgress(env.RUN_TRACKING_KV, job.runId, { processed: 1 }).catch(() => {});
  }

  const duration = Date.now() - startTime;
  console.log(
    `[MemberJob] COMPLETE: ${job.displayName} - ` +
    `Processed ${totalProcessed} activities, wrote ${totalWritten} dungeon stats - ` +
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