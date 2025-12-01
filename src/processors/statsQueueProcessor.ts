// ============================================================================
// FILE: src/processors/statsQueueProcessor.ts
// OPTIMIZED: Parallel PGCR fetching with rate limiting (10 concurrent)
// Logging reduced to only key steps and summaries
// ============================================================================

import type { Env, StatsQueueJob } from '../types';
import { fetchPGCR } from '../api/bungieApi';
import { bungieRateLimiter, processWithRateLimit } from '../utils/rateLimiter';

const BEYOND_LIGHT_START_MS = Date.parse('2020-11-10T17:00:00.000Z');
const WITCH_QUEEN_START_MS = Date.parse('2022-02-22T17:00:00.000Z');
const HAUNTED_START_MS = Date.parse('2022-05-24T17:00:00.000Z');

// Parallel fetch configuration
const PARALLEL_BATCH_SIZE = 10; // Fetch 10 PGCRs at a time

// Retry configuration
const MAX_RETRIES = 3;
const RETRY_DELAY_MS = 1000;
const RETRY_BACKOFF_MULTIPLIER = 2;

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

async function fetchPGCRWithRetry(
  instanceId: string,
  apiKey: string,
  retries = MAX_RETRIES
): Promise<any> {
  let lastError: Error | null = null;
  
  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      const pgcr = await fetchPGCR(instanceId, apiKey);
      return pgcr;
    } catch (error) {
      lastError = error as Error;
      
      if (attempt < retries) {
        const delay = RETRY_DELAY_MS * Math.pow(RETRY_BACKOFF_MULTIPLIER, attempt);
        await new Promise(resolve => setTimeout(resolve, delay));
      }
    }
  }
  
  console.error(`[StatsQueue] All ${retries + 1} attempts failed for ${instanceId}:`, lastError);
  return null;
}

function extractPlaytime(pgcr: any, membershipId: string): number {
  try {
    const entries = pgcr.entries || [];
    const match = entries.find((e: any) => 
      e?.player?.destinyUserInfo?.membershipId === membershipId
    );
    
    if (match && Number.isFinite(Number(match.values?.timePlayedSeconds?.basic?.value))) {
      return Number(match.values.timePlayedSeconds.basic.value);
    }
  } catch (err) {
    console.warn('[StatsQueue] Failed to extract playtime:', err);
  }
  return 0;
}

async function writeIncremental(env: Env, data: {
  clanId: string;
  membershipId: string;
  membershipType: number;
  dungeonHash: string;
  clearsDelta: number;
  fullClearsDelta: number;
  playtimeDelta: number;
  lastProcessedDate: string | null;
}): Promise<void> {
  const now = Date.now();
  
  // Upsert member_dungeon_stats with incremental deltas
  await env.DB.prepare(`
    INSERT INTO member_dungeon_stats (
      clan_id, membership_id, membership_type, dungeon_hash,
      total_clears, total_full_clears, total_playtime_seconds,
      last_processed_date, created_at, updated_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT (clan_id, membership_id, dungeon_hash) DO UPDATE SET
      total_clears = total_clears + excluded.total_clears,
      total_full_clears = total_full_clears + excluded.total_full_clears,
      total_playtime_seconds = total_playtime_seconds + excluded.total_playtime_seconds,
      last_processed_date = CASE 
        WHEN excluded.last_processed_date IS NOT NULL AND 
             (last_processed_date IS NULL OR excluded.last_processed_date > last_processed_date)
        THEN excluded.last_processed_date
        ELSE last_processed_date
      END,
      updated_at = excluded.updated_at
  `).bind(
    data.clanId,
    data.membershipId,
    data.membershipType,
    data.dungeonHash,
    data.clearsDelta,
    data.fullClearsDelta,
    data.playtimeDelta,
    data.lastProcessedDate,
    now,
    now
  ).run();
  
  // Incrementally update clan aggregates
  await env.DB.prepare(`
    INSERT INTO clan_aggregate_stats (
      clan_id, dungeon_hash,
      total_clears, total_full_clears, total_playtime_seconds,
      active_member_count, last_updated
    ) VALUES (?, ?, ?, ?, ?, 1, ?)
    ON CONFLICT (clan_id, dungeon_hash) DO UPDATE SET
      total_clears = total_clears + excluded.total_clears,
      total_full_clears = total_full_clears + excluded.total_full_clears,
      total_playtime_seconds = total_playtime_seconds + excluded.total_playtime_seconds,
      last_updated = excluded.last_updated
  `).bind(
    data.clanId,
    data.dungeonHash,
    data.clearsDelta,
    data.fullClearsDelta,
    data.playtimeDelta,
    now
  ).run();
}

export async function processStatsQueueJob(env: Env, job: StatsQueueJob): Promise<void> {
  const startTime = Date.now();

  console.log(`\n${'─'.repeat(80)}`);
  console.log(`[StatsQueue] START Processing batch - Job ID: ${job.jobId} | Membership: ${job.membershipId} | Dungeon: ${job.dungeonHash}`);
  console.log(`[StatsQueue] Activities in batch: ${job.activities.length}`);
  console.log(`${'─'.repeat(80)}`);

  // Check rate limiter status
  try {
    const status = bungieRateLimiter.getStatus();
    console.log(`[StatsQueue] Rate limiter: ${status.available}/${status.capacity} tokens available`);
  } catch {
    // silent if rate limiter not available
  }

  // PARALLEL PGCR fetching with rate limiting
  let fullClearsFound = 0;
  let totalPlaytime = 0;
  let latestActivityDate: string | null = null;
  let pgcrSuccessCount = 0;
  let pgcrFailureCount = 0;

  console.log(`[StatsQueue] Fetching ${job.activities.length} PGCRs in parallel (batches of ${PARALLEL_BATCH_SIZE})...`);
  
  // Process activities in parallel with rate limiting
  const results = await processWithRateLimit(
    job.activities,
    async (activity) => {
      try {
        const pgcr = await fetchPGCRWithRetry(activity.instanceId, env.BUNGIE_API_KEY);
        
        if (!pgcr) {
          return null;
        }
        
        const isFullClear = determineClearType(pgcr, activity.date || '');
        const playtime = extractPlaytime(pgcr, job.membershipId);
        
        return {
          isFullClear,
          playtime,
          date: activity.date,
        };
      } catch (err) {
        // return null and count as failure (aggregated below)
        return null;
      }
    },
    {
      rateLimiter: bungieRateLimiter,
      batchSize: PARALLEL_BATCH_SIZE,
      onProgress: (completed, total) => {
        // Only log coarse-grained progress (every 50 or on completion)
        if (completed % 50 === 0 || completed === total) {
          console.log(`[StatsQueue]   Progress: ${completed}/${total}`);
        }
      },
    }
  );

  // Aggregate results
  for (const result of results) {
    if (result) {
      pgcrSuccessCount++;
      if (result.isFullClear) fullClearsFound++;
      totalPlaytime += result.playtime || 0;
      if (result.date && (!latestActivityDate || result.date > latestActivityDate)) {
        latestActivityDate = result.date;
      }
    } else {
      pgcrFailureCount++;
    }
  }

  console.log(`[StatsQueue] Processing Summary: success=${pgcrSuccessCount} failed=${pgcrFailureCount} fullClears=${fullClearsFound} totalPlaytime=${totalPlaytime}s (${(totalPlaytime/3600).toFixed(2)}h)`);

  // Write incremental deltas to database immediately
  try {
    console.log(`[StatsQueue] Writing incremental deltas to database...`);
    
    await writeIncremental(env, {
      clanId: job.clanId,
      membershipId: job.membershipId,
      membershipType: job.membershipType,
      dungeonHash: job.dungeonHash,
      clearsDelta: pgcrSuccessCount,
      fullClearsDelta: fullClearsFound,
      playtimeDelta: totalPlaytime,
      lastProcessedDate: latestActivityDate,
    });
    
    console.log(`[StatsQueue] ✓ Database write complete`);
    
  } catch (err) {
    console.error(`[StatsQueue] ✗ Database write failed:`, err);
    throw err;
  }

  const duration = Date.now() - startTime;
  console.log(`[StatsQueue] COMPLETE - Duration: ${duration}ms (${(duration / 1000).toFixed(2)}s)`);
  console.log(`${'─'.repeat(80)}\n`);
}