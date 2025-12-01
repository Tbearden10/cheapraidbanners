// ============================================================================
// FILE: src/processors/statsQueueProcessor.ts
// Processes PGCR batches with support for partial jobs (split large activity sets)
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
  const isPartialJob = job.isPartialJob || false;

  const jobLabel = isPartialJob 
    ? `${job.jobId} [part ${(job.partIndex ?? 0) + 1}/${job.totalParts}]`
    : job.jobId;

  console.log(
    `[StatsQueue] START job=${jobLabel} ` +
    `membership=${job.membershipId} dungeon=${job.dungeonHash} ` +
    `batch=${batchIndex + 1}/${job.totalBatches || 1}`
  );

  // Initialize Durable Object (first batch only)
  if (isMultiBatch && batchIndex === 0) {
    try {
      const doId = env.BATCH_COORDINATOR.idFromName(job.jobId);
      const stub = env.BATCH_COORDINATOR.get(doId);
      const initResp = await stub.fetch('https://fake/init', {
        method: 'POST',
        body: JSON.stringify({
          membershipId: job.membershipId,
          membershipType: job.membershipType,
          dungeonHash: job.dungeonHash,
          totalBatches: job.totalBatches,
        }),
      });

      // Drain the response body
      try {
        await initResp.text();
      } catch (e) {
        // ignore body read errors
      }
    } catch (err) {
      console.error(`[StatsQueue] ❌ Failed to initialize BatchCoordinator for ${job.jobId}:`, err);
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

  // Add timeout warning
  const WARN_THRESHOLD = 10 * 60 * 1000; // 10 minutes
  let lastWarning = startTime;

  for (let i = 0; i < job.activities.length; i += PGCR_BATCH_SIZE) {
    const batch = job.activities.slice(i, i + PGCR_BATCH_SIZE);

    const results = await Promise.allSettled(
      batch.map(async (activity) => {
        const pgcr = await fetchPGCR(activity.instanceId, env.BUNGIE_API_KEY);
        if (!pgcr) {
          return null;
        }

        const isFullClear = determineClearType(pgcr, activity.date || '');

        // Extract playtime from PGCR
        let playtime = 0;
        try {
          const entries = pgcr.entries || [];
          const match = entries.find((e: any) => 
            e?.player?.destinyUserInfo?.membershipId === job.membershipId
          );
          if (match && Number.isFinite(Number(match.values?.timePlayedSeconds?.basic?.value))) {
            playtime = Number(match.values.timePlayedSeconds.basic.value);
          } else {
            playtime = 0;
          }
        } catch {
          playtime = 0;
        }

        return { isFullClear, playtime, date: activity.date };
      })
    );

    let batchSuccesses = 0;
    let batchFailures = 0;

    for (const result of results) {
      if (result.status === 'fulfilled' && result.value) {
        batchSuccesses++;
        if (result.value.isFullClear) {
          fullClearsFound++;
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

    // Periodic timeout warning
    const elapsed = Date.now() - startTime;
    if (elapsed > WARN_THRESHOLD && elapsed - lastWarning > 60000) {
      console.warn(
        `[StatsQueue] ⚠️ Job ${jobLabel} running for ${Math.round(elapsed / 1000)}s ` +
        `(${Math.round((i / job.activities.length) * 100)}% complete)`
      );
      lastWarning = Date.now();
    }

    // Rate limit between PGCR batches
    if (i + PGCR_BATCH_SIZE < job.activities.length) {
      await new Promise(r => setTimeout(r, DELAY_MS));
    }
  }

  console.log(
    `[StatsQueue] PGCR results for ${jobLabel}: ` +
    `success=${pgcrSuccessCount}/${job.activities.length} ` +
    `failed=${pgcrFailureCount} fullClears=${fullClearsFound} playtime=${totalPlaytime}s`
  );

  const batchResult: BatchResult = {
    batchIndex,
    activitiesCount: job.activities.length,
    fullClearsFound,
    playtimeSeconds: totalPlaytime,
    latestActivityDate,
  };

  // Send to Durable Object or write directly
  if (isMultiBatch) {
    try {
      const doId = env.BATCH_COORDINATOR.idFromName(job.jobId);
      const stub = env.BATCH_COORDINATOR.get(doId);
      const doResponse = await stub.fetch('https://fake/batch', {
        method: 'POST',
        body: JSON.stringify(batchResult),
      });
      const doResult = await doResponse.json() as { complete: boolean; aggregated?: any };

      if (doResult.complete) {
        const agg = doResult.aggregated || {};
        const aggregatedPlaytime = Number(agg.totalPlaytimeSeconds ?? 0);
        await writeStatsToDb(
          env, 
          job, 
          agg.totalActivities, 
          agg.totalFullClears, 
          aggregatedPlaytime, 
          agg.latestActivityDate
        );
      }
    } catch (err) {
      console.error(`[StatsQueue] ❌ Error communicating with BatchCoordinator for ${job.jobId}:`, err);
      throw err;
    }
  } else {
    // Single batch - write directly
    await writeStatsToDb(
      env, 
      job, 
      job.activities.length, 
      fullClearsFound, 
      totalPlaytime, 
      latestActivityDate
    );
  }

  const duration = Date.now() - startTime;
  console.log(`[StatsQueue] COMPLETE job=${jobLabel} - duration=${Math.round(duration / 1000)}s`);
}

async function writeStatsToDb(
  env: Env,
  job: StatsQueueJob,
  totalClears: number,
  totalFullClears: number,
  totalPlaytime: number,
  lastProcessedDate: string | null
): Promise<void> {
  
  const isPartialJob = job.isPartialJob || false;
  
  if (isPartialJob) {
    console.log(
      `[StatsQueue:DB] Partial job ${(job.partIndex ?? 0) + 1}/${job.totalParts} ` +
      `for ${job.membershipId}/${job.dungeonHash}`
    );
  }

  // Get previous stats
  const prevRow = await env.DB.prepare(`
    SELECT total_clears, total_full_clears, total_playtime_seconds, last_processed_date
    FROM member_dungeon_stats
    WHERE clan_id = ? AND membership_id = ? AND dungeon_hash = ?
  `).bind(job.clanId, job.membershipId, job.dungeonHash).first();

  const prevClears = prevRow ? Number((prevRow as any).total_clears || 0) : 0;
  const prevFullClears = prevRow ? Number((prevRow as any).total_full_clears || 0) : 0;
  const prevPlaytime = prevRow ? Number((prevRow as any).total_playtime_seconds || 0) : 0;
  const prevLastProcessedDate = prevRow ? (prevRow as any).last_processed_date : null;

  const isNewRecord = !prevRow;

  // CRITICAL: For partial jobs OR incremental updates, always ADD to existing totals
  const newTotalClears = prevClears + totalClears;
  const newTotalFullClears = prevFullClears + totalFullClears;
  const newTotalPlaytime = prevPlaytime + totalPlaytime;

  // Update lastProcessedDate to the LATEST date seen
  let finalLastProcessedDate = prevLastProcessedDate;
  if (lastProcessedDate) {
    if (!finalLastProcessedDate || lastProcessedDate > finalLastProcessedDate) {
      finalLastProcessedDate = lastProcessedDate;
    }
  }

  // Write to DB
  try {
    await upsertMemberDungeonStats(env.DB, {
      clanId: job.clanId,
      membershipId: job.membershipId,
      membershipType: job.membershipType,
      dungeonHash: job.dungeonHash,
      totalClears: newTotalClears,
      totalFullClears: newTotalFullClears,
      totalPlaytimeSeconds: newTotalPlaytime,
      lastProcessedDate: finalLastProcessedDate,
    });
  } catch (err) {
    console.error(
      `[StatsQueue:DB] ❌ Failed to upsert stats for ${job.membershipId}/${job.dungeonHash}:`,
      err
    );
    throw err;
  }

  // Update clan aggregates (incremental - delta from THIS job only)
  try {
    await applyClanAggregateDelta(
      env.DB,
      job.clanId,
      job.dungeonHash,
      totalClears,      // Delta from THIS job
      totalFullClears,  // Delta from THIS job
      totalPlaytime,    // Delta from THIS job
      isNewRecord
    );
  } catch (err) {
    console.error(
      `[StatsQueue:DB] ❌ Failed to update clan aggregates for ${job.clanId}/${job.dungeonHash}:`,
      err
    );
    throw err;
  }

  console.log(
    `[StatsQueue:DB] Updated stats: ${job.membershipId}/${job.dungeonHash} ` +
    `totalClears=${newTotalClears} (+${totalClears}) ` +
    `fullClears=${newTotalFullClears} (+${totalFullClears}) ` +
    `playtime=${newTotalPlaytime}s (+${totalPlaytime}s)` +
    (isPartialJob ? ` [partial ${(job.partIndex ?? 0) + 1}/${job.totalParts}]` : '')
  );
}