// ============================================================================
// FILE: src/processors/statsQueueProcessor.ts
// Processes PGCR batches - EXACT COPY of dungeon-info-hub logic
// ============================================================================

import type { Env, StatsQueueJob } from '../types';
import { fetchPGCR } from '../api/bungieApi';
import { upsertMemberDungeonStats } from '../db/queries';
import { applyClanAggregateDelta } from '../db/aggregateHelpers';
import type { BatchResult } from '../durable-objects/BatchCoordinator';

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

export async function processStatsQueueJob(env: Env, job: StatsQueueJob): Promise<void> {
  const startTime = Date.now();
  const isMultiBatch = job.totalBatches && job.totalBatches > 1;
  const batchIndex = job.batchIndex ?? 0;

  console.log(`\n${'─'.repeat(80)}`);
  console.log(`[StatsQueue] START Processing batch`);
  console.log(`[StatsQueue] - Job ID: ${job.jobId}`);
  console.log(`[StatsQueue] - Membership ID: ${job.membershipId}`);
  console.log(`[StatsQueue] - Dungeon Hash: ${job.dungeonHash}`);
  console.log(`[StatsQueue] - Batch: ${batchIndex + 1}/${job.totalBatches || 1} ${isMultiBatch ? '(Multi-batch)' : '(Single-batch)'}`);
  console.log(`[StatsQueue] - Activities in batch: ${job.activities.length}`);
  console.log(`${'─'.repeat(80)}`);

  // Initialize Durable Object (first batch only)
  if (isMultiBatch && batchIndex === 0) {
    console.log(`[StatsQueue] Initializing BatchCoordinator for job ${job.jobId}...`);
    
    const doId = env.BATCH_COORDINATOR.idFromName(job.jobId);
    const stub = env.BATCH_COORDINATOR.get(doId);
    
    try {
      await stub.fetch('https://fake/init', {
        method: 'POST',
        body: JSON.stringify({
          membershipId: job.membershipId,
          membershipType: job.membershipType,
          dungeonHash: job.dungeonHash,
          totalBatches: job.totalBatches,
        }),
      });
      console.log(`[StatsQueue] ✓ BatchCoordinator initialized`);
    } catch (err) {
      console.error(`[StatsQueue] ❌ Failed to initialize BatchCoordinator:`, err);
      throw err;
    }
  }

  // Process activities (SEQUENTIAL PGCR fetches with rate limiting)
  const PGCR_BATCH_SIZE = 25;
  const DELAY_MS = 200;
  
  let fullClearsFound = 0;
  let totalPlaytime = 0;
  let latestActivityDate: string | null = null;
  let pgcrSuccessCount = 0;
  let pgcrFailureCount = 0;

  const totalIterations = Math.ceil(job.activities.length / PGCR_BATCH_SIZE);
  console.log(`[StatsQueue] Processing ${job.activities.length} activities in ${totalIterations} PGCR batch(es) of ${PGCR_BATCH_SIZE}...`);

  for (let i = 0; i < job.activities.length; i += PGCR_BATCH_SIZE) {
    const batch = job.activities.slice(i, i + PGCR_BATCH_SIZE);
    const currentBatch = Math.floor(i / PGCR_BATCH_SIZE) + 1;
    
    console.log(`[StatsQueue]   PGCR Batch ${currentBatch}/${totalIterations}: Fetching ${batch.length} PGCRs...`);
    
    const results = await Promise.allSettled(
      batch.map(async (activity, idx) => {
        const pgcr = await fetchPGCR(activity.instanceId, env.BUNGIE_API_KEY);
        
        if (!pgcr) {
          console.log(`[StatsQueue]     - Activity ${i + idx + 1}: ❌ PGCR not found for ${activity.instanceId}`);
          return null;
        }
        
        const isFullClear = determineClearType(pgcr, activity.date || '');
        
        // Extract playtime — use ONLY the player's timePlayedSeconds from the PGCR entries
        let playtime = 0;
        try {
          const entries = pgcr.entries || [];
          const match = entries.find((e: any) => 
            e?.player?.destinyUserInfo?.membershipId === job.membershipId
          );
          if (match && Number.isFinite(Number(match.values?.timePlayedSeconds?.basic?.value))) {
            const extractedPlaytime = Number(match.values.timePlayedSeconds.basic.value);
            // Log comparison with activity.seconds for visibility — but do not use activity.seconds as authoritative
            if (activity.seconds && Number(activity.seconds) !== extractedPlaytime) {
              console.log(`[StatsQueue]     - Activity ${i + idx + 1}: Using PGCR timePlayedSeconds ${extractedPlaytime}s (activity.seconds was ${activity.seconds})`);
            } else {
              console.log(`[StatsQueue]     - Activity ${i + idx + 1}: Using PGCR timePlayedSeconds ${extractedPlaytime}s`);
            }
            playtime = extractedPlaytime;
          } else {
            // No player-specific time found in PGCR — explicitly set to 0 and log for traceability
            console.log(`[StatsQueue]     - Activity ${i + idx + 1}: No player timePlayedSeconds found in PGCR for membership ${job.membershipId}; playtime set to 0`);
            playtime = 0;
          }
        } catch (err) {
          console.warn(`[StatsQueue]     - Activity ${i + idx + 1}: Failed to extract timePlayedSeconds from PGCR:`, err);
          playtime = 0;
        }

        const clearType = isFullClear ? 'Full Clear' : 'Checkpoint';
        console.log(`[StatsQueue]     - Activity ${i + idx + 1}: ✓ ${clearType} | ${playtime}s | ${activity.date}`);

        return { isFullClear, playtime, date: activity.date };
      })
    );

    let batchSuccesses = 0;
    let batchFailures = 0;
    let batchFullClears = 0;

    for (const result of results) {
      if (result.status === 'fulfilled' && result.value) {
        batchSuccesses++;
        if (result.value.isFullClear) {
          fullClearsFound++;
          batchFullClears++;
        }
        totalPlaytime += result.value.playtime || 0;
        if (result.value.date && (!latestActivityDate || result.value.date > latestActivityDate)) {
          latestActivityDate = result.value.date;
        }
      } else {
        batchFailures++;
      }
    }

    pgcrSuccessCount += batchSuccesses;
    pgcrFailureCount += batchFailures;

    console.log(`[StatsQueue]   PGCR Batch ${currentBatch}/${totalIterations}: Complete - ${batchSuccesses} success, ${batchFailures} failed, ${batchFullClears} full clears`);

    // Rate limit between PGCR batches
    if (i + PGCR_BATCH_SIZE < job.activities.length) {
      await new Promise(r => setTimeout(r, DELAY_MS));
    }
  }

  console.log(`[StatsQueue] PGCR Processing Complete:`);
  console.log(`[StatsQueue] - Successful PGCRs: ${pgcrSuccessCount}/${job.activities.length}`);
  console.log(`[StatsQueue] - Failed PGCRs: ${pgcrFailureCount}/${job.activities.length}`);
  console.log(`[StatsQueue] - Full clears found: ${fullClearsFound}`);
  console.log(`[StatsQueue] - Total playtime (this batch): ${totalPlaytime}s (${(totalPlaytime / 3600).toFixed(2)}h)`);
  console.log(`[StatsQueue] - Latest activity: ${latestActivityDate || 'N/A'}`);

  const batchResult: BatchResult = {
    batchIndex,
    activitiesCount: job.activities.length,
    fullClearsFound,
    // IMPORTANT: include per-batch playtime + latest activity so the DO can aggregate correctly
    playtimeSeconds: totalPlaytime,
    latestActivityDate,
  };

  // Send to Durable Object or write directly
  if (isMultiBatch) {
    console.log(`[StatsQueue] Sending results to BatchCoordinator (batch ${batchIndex + 1}/${job.totalBatches})...`);
    
    const doId = env.BATCH_COORDINATOR.idFromName(job.jobId);
    const stub = env.BATCH_COORDINATOR.get(doId);
    
    try {
      const doResponse = await stub.fetch('https://fake/batch', {
        method: 'POST',
        body: JSON.stringify(batchResult),
      });
      const doResult = await doResponse.json() as { complete: boolean; aggregated?: any };

      console.log(`[StatsQueue] ✓ BatchCoordinator response: complete=${doResult.complete}`);

      if (doResult.complete) {
        const agg = doResult.aggregated || {};
        console.log(`[StatsQueue] 🎉 All batches complete! Final aggregation:`);
        console.log(`[StatsQueue]   - Total activities: ${agg.totalActivities}`);
        console.log(`[StatsQueue]   - Total full clears: ${agg.totalFullClears}`);
        console.log(`[StatsQueue]   - Total playtime (aggregated): ${agg.totalPlaytimeSeconds}`);
        console.log(`[StatsQueue]   - Latest activity: ${agg.latestActivityDate}`);
        console.log(`[StatsQueue] Writing final stats to database...`);
        
        // Use the DO's aggregated totalPlaytimeSeconds (fallback to local totalPlaytime if missing)
        const aggregatedPlaytime = Number(agg.totalPlaytimeSeconds ?? 0);
        await writeStatsToDb(env, job, agg.totalActivities, agg.totalFullClears, aggregatedPlaytime, agg.latestActivityDate);
      } else {
        console.log(`[StatsQueue] Waiting for remaining batches...`);
      }
    } catch (err) {
      console.error(`[StatsQueue] ❌ Error communicating with BatchCoordinator:`, err);
      throw err;
    }
  } else {
    console.log(`[StatsQueue] Single batch - writing directly to database...`);
    await writeStatsToDb(env, job, job.activities.length, fullClearsFound, totalPlaytime, latestActivityDate);
  }

  const duration = Date.now() - startTime;
  console.log(`[StatsQueue] COMPLETE - Duration: ${duration}ms (${(duration / 1000).toFixed(2)}s)`);
  console.log(`${'─'.repeat(80)}\n`);
}

async function writeStatsToDb(
  env: Env,
  job: StatsQueueJob,
  totalClears: number,
  totalFullClears: number,
  totalPlaytime: number,
  lastProcessedDate: string | null
): Promise<void> {
  console.log(`[StatsQueue:DB] Writing stats to database...`);
  console.log(`[StatsQueue:DB] - Clan ID: ${job.clanId}`);
  console.log(`[StatsQueue:DB] - Membership ID: ${job.membershipId}`);
  console.log(`[StatsQueue:DB] - Dungeon Hash: ${job.dungeonHash}`);

  // Get previous stats
  console.log(`[StatsQueue:DB] Fetching previous stats...`);
  const prevRow = await env.DB.prepare(`
    SELECT total_clears, total_full_clears, total_playtime_seconds
    FROM member_dungeon_stats
    WHERE clan_id = ? AND membership_id = ? AND dungeon_hash = ?
  `).bind(job.clanId, job.membershipId, job.dungeonHash).first();

  const prevClears = prevRow ? Number((prevRow as any).total_clears || 0) : 0;
  const prevFullClears = prevRow ? Number((prevRow as any).total_full_clears || 0) : 0;
  const prevPlaytime = prevRow ? Number((prevRow as any).total_playtime_seconds || 0) : 0;

  const isNewRecord = !prevRow;
  
  if (isNewRecord) {
    console.log(`[StatsQueue:DB] No previous record found - creating new entry`);
  } else {
    console.log(`[StatsQueue:DB] Previous stats:`);
    console.log(`[StatsQueue:DB]   - Clears: ${prevClears}`);
    console.log(`[StatsQueue:DB]   - Full clears: ${prevFullClears}`);
    console.log(`[StatsQueue:DB]   - Playtime: ${prevPlaytime}s`);
  }

  const newTotalClears = prevClears + totalClears;
  const newTotalFullClears = prevFullClears + totalFullClears;
  const newTotalPlaytime = prevPlaytime + totalPlaytime;

  console.log(`[StatsQueue:DB] Delta being applied:`);
  console.log(`[StatsQueue:DB]   + Clears: ${totalClears}`);
  console.log(`[StatsQueue:DB]   + Full clears: ${totalFullClears}`);
  console.log(`[StatsQueue:DB]   + Playtime: ${totalPlaytime}s`);

  console.log(`[StatsQueue:DB] New totals:`);
  console.log(`[StatsQueue:DB]   = Clears: ${prevClears} + ${totalClears} = ${newTotalClears}`);
  console.log(`[StatsQueue:DB]   = Full clears: ${prevFullClears} + ${totalFullClears} = ${newTotalFullClears}`);
  console.log(`[StatsQueue:DB]   = Playtime: ${prevPlaytime}s + ${totalPlaytime}s = ${newTotalPlaytime}s (${(newTotalPlaytime / 3600).toFixed(2)}h)`);

  // Write to DB
  console.log(`[StatsQueue:DB] Upserting member_dungeon_stats...`);
  try {
    await upsertMemberDungeonStats(env.DB, {
      clanId: job.clanId,
      membershipId: job.membershipId,
      membershipType: job.membershipType,
      dungeonHash: job.dungeonHash,
      totalClears: newTotalClears,
      totalFullClears: newTotalFullClears,
      totalPlaytimeSeconds: newTotalPlaytime,
      lastProcessedDate,
    });
    console.log(`[StatsQueue:DB] ✓ Successfully upserted member_dungeon_stats`);
  } catch (err) {
    console.error(`[StatsQueue:DB] ❌ Failed to upsert member_dungeon_stats:`, err);
    throw err;
  }

  // Update clan aggregates (incremental)
  console.log(`[StatsQueue:DB] Applying delta to clan aggregates...`);
  try {
    await applyClanAggregateDelta(
      env.DB,
      job.clanId,
      job.dungeonHash,
      totalClears,
      totalFullClears,
      totalPlaytime,
      isNewRecord
    );
    console.log(`[StatsQueue:DB] ✓ Successfully updated clan aggregates`);
  } catch (err) {
    console.error(`[StatsQueue:DB] ❌ Failed to update clan aggregates:`, err);
    throw err;
  }

  console.log(`[StatsQueue:DB] ✅ Database write complete: ${newTotalFullClears} full clears (${newTotalClears} total)`);
}