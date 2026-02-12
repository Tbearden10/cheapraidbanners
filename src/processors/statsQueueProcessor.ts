// Stats queue processor - fetches PGCRs and updates DB

import type { Env, StatsQueueJob } from '../types';
import { fetchPGCR } from '../api/bungieApi';

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
  
  // Only write to member_dungeon_stats - aggregates are calculated on-demand
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
}

// In statsQueueProcessor.ts - remove coordinator logic
export async function processStatsQueueJob(env: Env, job: StatsQueueJob): Promise<void> {
  const startTime = Date.now();

  let fullClearsFound = 0;
  let totalPlaytime = 0;
  let latestActivityDate: string | null = null;
  let latestInstanceId: string | null = null;
  let successCount = 0;

  // Process in batches of 30
  for (let i = 0; i < job.activities.length; i += PGCR_BATCH_SIZE) {
    const batch = job.activities.slice(i, Math.min(i + PGCR_BATCH_SIZE, job.activities.length));
    
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
    
    for (const result of batchResults) {
      if (result.status === 'fulfilled' && result.value) {
        successCount++;
        if (result.value.isFullClear) fullClearsFound++;
        totalPlaytime += result.value.playtime;
        
        if (result.value.date && (!latestActivityDate || result.value.date > latestActivityDate)) {
          latestActivityDate = result.value.date;
          latestInstanceId = result.value.instanceId;
        }
      }
    }
    
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

  const duration = Date.now() - startTime;
  const failureCount = job.activities.length - successCount;
  
  if (failureCount > 0) {
    console.log(`[StatsQueue] ${successCount}/${job.activities.length} PGCRs processed in ${(duration/1000).toFixed(1)}s (${failureCount} failed)`);
  }
}