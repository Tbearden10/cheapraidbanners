// Stats queue processor - fetches PGCRs and updates DB

import type { Env, StatsQueueJob } from '../types';
import { fetchPGCR } from '../api/bungieApi';
import { recomputeClanAggregateStats } from '../db/aggregateHelpers';

const BEYOND_LIGHT_START_MS = Date.parse('2020-11-10T17:00:00.000Z');
const WITCH_QUEEN_START_MS = Date.parse('2022-02-22T17:00:00.000Z');
const HAUNTED_START_MS = Date.parse('2022-05-24T17:00:00.000Z');

// Match statsProcessor.ts batch size and delay
const PGCR_BATCH_SIZE = 30;
const DELAY_MS = 50;

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

function extractPlaytime(pgcr: any, membershipId: string): number {
  try {
    const entries = pgcr.entries || [];
    const match = entries.find((e: any) => 
      e?.player?.destinyUserInfo?.membershipId === membershipId
    );
    
    if (match && Number.isFinite(Number(match.values?.timePlayedSeconds?.basic?.value))) {
      return Number(match.values.timePlayedSeconds.basic.value);
    }
  } catch {}
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
  lastProcessedInstanceId: string | null;
}): Promise<void> {
  const now = Date.now();
  
  await env.DB.prepare(`
    INSERT INTO member_dungeon_stats (
      clan_id, membership_id, membership_type, dungeon_hash,
      total_clears, total_full_clears, total_playtime_seconds,
      last_processed_date, last_processed_instance_id, created_at, updated_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
      last_processed_instance_id = CASE 
        WHEN excluded.last_processed_instance_id IS NOT NULL AND 
             excluded.last_processed_date IS NOT NULL AND
             (last_processed_date IS NULL OR excluded.last_processed_date > last_processed_date)
        THEN excluded.last_processed_instance_id
        ELSE last_processed_instance_id
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
    data.lastProcessedInstanceId,
    now,
    now
  ).run();
  
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

  let fullClearsFound = 0;
  let totalPlaytime = 0;
  let latestActivityDate: string | null = null;
  let latestInstanceId: string | null = null;
  let successCount = 0;

  // Process in batches of 30 (matching statsProcessor.ts)
  for (let i = 0; i < job.activities.length; i += PGCR_BATCH_SIZE) {
    const batch = job.activities.slice(i, Math.min(i + PGCR_BATCH_SIZE, job.activities.length));
    
    // Fetch batch concurrently using Promise.allSettled
    const batchResults = await Promise.allSettled(
      batch.map(async (activity) => {
        const pgcr = await fetchPGCR(activity.instanceId, env.BUNGIE_API_KEY);
        
        if (!pgcr) return null;
        
        return {
          isFullClear: determineClearType(pgcr, activity.date || ''),
          playtime: extractPlaytime(pgcr, job.membershipId),
          date: activity.date,
          instanceId: activity.instanceId,
        };
      })
    );
    
    // Aggregate batch results
    for (const result of batchResults) {
      if (result.status === 'fulfilled' && result.value) {
        successCount++;
        if (result.value.isFullClear) fullClearsFound++;
        totalPlaytime += result.value.playtime;
        
        // Track most recent activity
        if (result.value.date && (!latestActivityDate || result.value.date > latestActivityDate)) {
          latestActivityDate = result.value.date;
          latestInstanceId = result.value.instanceId;
        }
      }
    }
    
    // Delay between batches (matching statsProcessor.ts)
    if (i + PGCR_BATCH_SIZE < job.activities.length) {
      await new Promise(resolve => setTimeout(resolve, DELAY_MS));
    }
  }

  // Write to database
  await writeIncremental(env, {
    clanId: job.clanId,
    membershipId: job.membershipId,
    membershipType: job.membershipType,
    dungeonHash: job.dungeonHash,
    clearsDelta: successCount,
    fullClearsDelta: fullClearsFound,
    playtimeDelta: totalPlaytime,
    lastProcessedDate: latestActivityDate,
    lastProcessedInstanceId: latestInstanceId,
  });

  // Report to MemberCoordinator if coordinatorId provided
  if (job.coordinatorId) {
    try {
      const coordinatorId = env.MEMBER_COORDINATOR.idFromName(job.coordinatorId);
      const coordinator = env.MEMBER_COORDINATOR.get(coordinatorId);
      
      const response = await coordinator.fetch('https://internal/batch', {
        method: 'POST',
        body: JSON.stringify({
          batchId: job.jobId,
          dungeonHash: job.dungeonHash,
          batchIndex: parseInt(job.jobId.split('-').pop() || '0'),
          clearsDelta: successCount,
          fullClearsDelta: fullClearsFound,
          playtimeDelta: totalPlaytime,
          lastProcessedDate: latestActivityDate,
        }),
        headers: { 'Content-Type': 'application/json' },
      });

      const result = await response.json() as { complete: boolean; aggregated?: any };
      
      if (result.complete) {
        console.log(`[StatsQueue] All batches complete for ${job.membershipId}`);
        
        // Trigger aggregate recompute for the clan
        try {
          await recomputeClanAggregateStats(env.DB, job.clanId);
          console.log(`[StatsQueue] Aggregate stats recomputed for clan ${job.clanId}`);
        } catch (err) {
          console.warn('[StatsQueue] Failed to recompute aggregate stats:', err);
        }
      }
    } catch (err) {
      console.warn('[StatsQueue] Failed to report to MemberCoordinator:', err);
    }
  }

  const duration = Date.now() - startTime;
  const failureCount = job.activities.length - successCount;
  
  if (failureCount > 0) {
    console.log(`[StatsQueue] ${successCount}/${job.activities.length} PGCRs processed in ${(duration/1000).toFixed(1)}s (${failureCount} failed)`);
  }
}