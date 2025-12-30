// ============================================================================
// FILE: src/processors/statsQueueProcessor.ts
// OPTIMIZED: Fast wave-based processing matching statsProcessor.ts pattern
// 30 activities per wave, 50ms delay, clean and simple
// ============================================================================

import type { Env, StatsQueueJob } from '../types';
import { fetchPGCR } from '../api/bungieApi';

// Match statsProcessor.ts batch size and delay
const PGCR_BATCH_SIZE = 30;
const DELAY_MS = 50;

function determineClearType(pgcr: any, period: string): boolean {
  // All completed dungeon activities should count as full clears
  // We're already filtering to only completed activities (completed.basic.value === 1)
  // Character swaps, player joins/leaves during runs are normal and don't invalidate the clear
  // The activityWasStartedFromBeginning field can be unreliable for instances with character swaps
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
}): Promise<void> {
  const now = Date.now();
  
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
  console.log(`[StatsQueue] Processing ${job.activities.length} PGCRs (${job.jobId})`);

  let fullClearsFound = 0;
  let totalPlaytime = 0;
  let latestActivityDate: string | null = null;
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
        };
      })
    );
    
    // Aggregate batch results
    for (const result of batchResults) {
      if (result.status === 'fulfilled' && result.value) {
        successCount++;
        if (result.value.isFullClear) fullClearsFound++;
        totalPlaytime += result.value.playtime;
        if (result.value.date && (!latestActivityDate || result.value.date > latestActivityDate)) {
          latestActivityDate = result.value.date;
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
        console.log(`[StatsQueue] ✓ All batches complete for member ${job.membershipId}`);
        // Optionally: trigger additional processing when all member batches are done
      }
    } catch (err) {
      console.warn('[StatsQueue] Failed to report to MemberCoordinator:', err);
    }
  }

  const duration = Date.now() - startTime;
  const reqPerSec = ((successCount) / (duration / 1000)).toFixed(1);
  const failureCount = job.activities.length - successCount;
  
  console.log(
    `[StatsQueue] ✓ ${successCount}/${job.activities.length} in ${(duration/1000).toFixed(1)}s ` +
    `(${reqPerSec} req/s) | Clears: ${fullClearsFound} | Failed: ${failureCount}`
  );
}