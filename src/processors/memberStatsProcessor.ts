// Production-ready member stats processor with SEQUENTIAL PGCR processing
// Matches dungeon-info-hub architecture for safe rate limit compliance

import type { Env, MemberJob } from '../types';
import { ACTIVITY_REFERENCE_MAP } from '../constants/activityReferenceMap';
import { fetchCharactersForMember, fetchActivitiesForCharacter, fetchPGCR } from '../api/bungieApi';
import { upsertMemberDungeonStats } from '../db/queries';
import { applyClanAggregateDelta } from '../db/aggregateHelpers';

// Worker pool for parallel execution
export async function promisePool<T, R>(items: T[], worker: (item: T) => Promise<R>, concurrency: number) {
  const results: R[] = new Array(items.length);
  let i = 0;
  const runners = new Array(Math.min(concurrency, items.length)).fill(0).map(async () => {
    while (true) {
      const idx = i++;
      if (idx >= items.length) return;
      try {
        results[idx] = await worker(items[idx]);
      } catch (err) {
        (results as any)[idx] = undefined as any;
      }
    }
  });
  await Promise.all(runners);
  return results;
}

function getEnvNumber(env: any, key: string, def: number) {
  const v = Number(env?.[key]);
  return Number.isFinite(v) && v > 0 ? v : def;
}

function determineClearType(pgcrResponse: any, period: string): boolean {
  if (!pgcrResponse || !period) return false;
  const timestamp = Date.parse(period);
  if (isNaN(timestamp)) return false;
  const HAUNTED_START = Date.parse('2022-05-24T17:00:00.000Z');
  const WITCH_QUEEN_START = Date.parse('2022-02-22T17:00:00.000Z');
  const BEYOND_LIGHT_START = Date.parse('2020-11-10T17:00:00.000Z');

  if (timestamp >= HAUNTED_START) return Boolean(pgcrResponse.activityWasStartedFromBeginning);
  if (timestamp < BEYOND_LIGHT_START) return pgcrResponse.startingPhaseIndex === 0;
  if (timestamp >= WITCH_QUEEN_START) return Boolean(pgcrResponse.activityWasStartedFromBeginning);
  return true;
}

function isCompletedSearchContext(a: any): boolean {
  return !!(a?.values?.completed?.basic?.value === 1);
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/**
 * Public: entrypoint used by the queue/job runner
 * Processes dungeons in parallel with proper concurrency limits
 */
export async function processMemberStats(env: Env, job: MemberJob): Promise<void> {
  const start = Date.now();
  console.log(`\n=== processMemberStats ${job.displayName} ${job.membershipId} ===`);

  // 1) Fetch characters (do NOT filter out deleted)
  const characters = await fetchCharactersForMember(job.membershipId, job.membershipType, env.BUNGIE_API_KEY);
  if (!characters || characters.length === 0) {
    console.log('No characters found, skipping');
    return;
  }
  const characterIds = characters.map((c: any) => String(c.characterId));
  console.log(`Found characters: ${characterIds.join(',')}`);

  // 2) Fetch ALL activities across all characters (parallel, per-character pagination)
  const perCharActivities = await fetchAllActivitiesForAllChars(job.membershipType, job.membershipId, characterIds, env.BUNGIE_API_KEY);

  // 3) Merge and annotate with characterId
  const merged: any[] = [];
  for (const charId of Object.keys(perCharActivities)) {
    const arr = perCharActivities[charId] || [];
    for (const a of arr) {
      merged.push({ ...a, characterId: a.characterId ?? charId });
    }
  }
  console.log(`Merged activities across characters: ${merged.length}`);

  // 4) Build dungeon buckets and dedupe per-dungeon by instanceId
  const activitiesByDungeon: Record<string, any[]> = {};
  for (const d of ACTIVITY_REFERENCE_MAP) activitiesByDungeon[String(d.hash)] = [];

  for (const a of merged) {
    const ref = a?.activityDetails?.referenceId || a?.activityHash || a?.referenceId;
    if (!ref) continue;
    for (const d of ACTIVITY_REFERENCE_MAP) {
      if ((d.referenceIds || []).includes(String(ref))) {
        activitiesByDungeon[String(d.hash)].push(a);
        break;
      }
    }
  }

  // Deduplicate per-dungeon by instanceId (prefer completed)
  for (const hash of Object.keys(activitiesByDungeon)) {
    const map = new Map<string, any>();
    for (const act of activitiesByDungeon[hash]) {
      const id = act.activityDetails?.instanceId || act.instanceId;
      if (!id) continue;
      const existing = map.get(id);
      if (!existing) map.set(id, act);
      else {
        const existingCompleted = !!(existing?.values?.completed?.basic?.value === 1);
        const newCompleted = !!(act?.values?.completed?.basic?.value === 1);
        if (!existingCompleted && newCompleted) map.set(id, act);
      }
    }
    activitiesByDungeon[hash] = Array.from(map.values());
  }

  // 5) Process dungeons in PARALLEL (with concurrency limit)
  const dungeonHashes = Object.keys(activitiesByDungeon).filter(h => activitiesByDungeon[h].length > 0);
  console.log(`Processing ${dungeonHashes.length} dungeon(s) in parallel`);

  const dungeonConcurrency = getEnvNumber(env, 'DUNGEON_PROCESSING_CONCURRENCY', 1);
  
  await promisePool(
    dungeonHashes,
    async (dungeonHash) => {
      await processOneDungeon(env, job, dungeonHash, activitiesByDungeon[dungeonHash]);
      return null;
    },
    dungeonConcurrency
  );

  const elapsed = ((Date.now() - start) / 1000).toFixed(2);
  console.log(`=== Completed member ${job.displayName} in ${elapsed}s ===\n`);
}

/**
 * Core per-dungeon processor with BatchCoordinator integration
 * Uses SEQUENTIAL PGCR processing like dungeon-info-hub
 */
async function processOneDungeon(env: Env, job: MemberJob, dungeonHash: string, activities: any[]) {
  const dungeonName = ACTIVITY_REFERENCE_MAP.find(d => String(d.hash) === dungeonHash)?.displayName || dungeonHash;

  // Read previous DB row
  const prevRow = await env.DB.prepare(`
    SELECT total_clears, total_full_clears, total_playtime_seconds, last_processed_date, last_processed_instance_id
    FROM member_dungeon_stats
    WHERE clan_id = ? AND membership_id = ? AND dungeon_hash = ?
  `).bind(job.clanId, job.membershipId, dungeonHash).first();

  const prevClears = prevRow ? Number((prevRow as any).total_clears || 0) : 0;
  const prevFullClears = prevRow ? Number((prevRow as any).total_full_clears || 0) : 0;
  const prevPlaytime = prevRow ? Number((prevRow as any).total_playtime_seconds || 0) : 0;
  const prevLastProcessed = prevRow ? (prevRow as any).last_processed_date : null;
  const prevLastInstanceId = prevRow ? (prevRow as any).last_processed_instance_id ?? null : null;

  let cutoffDate: Date | null = null;
  let cutoffInstanceId: string | null = null;
  if (prevLastProcessed) {
    cutoffDate = new Date(prevLastProcessed);
    cutoffInstanceId = prevLastInstanceId ? String(prevLastInstanceId) : null;
  } else if (job.lastProcessedDate) {
    cutoffDate = new Date(job.lastProcessedDate);
    cutoffInstanceId = null;
  }

  console.log(`  ${dungeonName}: activities=${activities.length}, cutoffDate=${cutoffDate?.toISOString() || 'none'}, cutoffInstance=${cutoffInstanceId ?? 'none'}`);

  // Trim to completed runs
  const completed = activities.filter(a => isCompletedSearchContext(a));
  console.log(`  ${dungeonName}: completed=${completed.length}`);
  if (completed.length === 0) {
    console.log(`  ${dungeonName}: no completed activities, skipping`);
    return;
  }

  // Normalize and validate
  const normalized = completed.map(a => ({
    instanceId: String(a.activityDetails?.instanceId || a.instanceId),
    period: a.period || a.activityDetails?.period,
    playtimeMeta: a.values?.timePlayedSeconds?.basic?.value ?? (a as any).seconds ?? 0,
    characterId: a.characterId ?? null,
    raw: a,
  })).filter(x => x.instanceId && x.period);

  if (normalized.length === 0) {
    console.log(`  ${dungeonName}: no valid normalized activities, skipping`);
    return;
  }

  // Sort deterministically
  normalized.sort((a, b) => {
    const ta = Date.parse(a.period);
    const tb = Date.parse(b.period);
    if (ta !== tb) return ta - tb;
    if (a.instanceId < b.instanceId) return -1;
    if (a.instanceId > b.instanceId) return 1;
    return 0;
  });

  // Apply resume cutoff
  let startIndex = 0;
  if (cutoffInstanceId) {
    const idx = normalized.findIndex(n => n.instanceId === cutoffInstanceId);
    if (idx >= 0) startIndex = idx + 1;
    else if (cutoffDate) {
      startIndex = normalized.findIndex(n => Date.parse(n.period) > cutoffDate.getTime());
      if (startIndex === -1) startIndex = normalized.length;
      console.warn(`  ${dungeonName}: cutoff instance not found, falling back to date cutoff (startIndex=${startIndex})`);
    }
  } else if (cutoffDate) {
    startIndex = normalized.findIndex(n => Date.parse(n.period) > cutoffDate.getTime());
    if (startIndex === -1) startIndex = normalized.length;
  }

  const items = normalized.slice(startIndex);
  console.log(`  ${dungeonName}: new items to process=${items.length}`);
  if (items.length === 0) return;

  const latestItem = items[items.length - 1];
  const lastProcessedDate = latestItem ? latestItem.period : prevLastProcessed;
  const lastProcessedInstanceId = latestItem ? latestItem.instanceId : prevLastInstanceId;

  const newClears = items.length;

  // ===== PGCR VERIFICATION WITH BATCH COORDINATOR (SEQUENTIAL) =====
  const runId = `member-${job.membershipId}-${dungeonHash}-${Date.now()}`;
  const jobId = `member-${job.membershipId}`;
  
  const PGCR_BATCH_SIZE = getEnvNumber(env, 'PGCR_BATCH_SIZE', 30);
  const batches: any[][] = [];
  for (let i = 0; i < items.length; i += PGCR_BATCH_SIZE) {
    batches.push(items.slice(i, i + PGCR_BATCH_SIZE));
  }

  console.log(`  ${dungeonName}: created ${batches.length} PGCR batches`);

  // Initialize BatchCoordinator with retry
  let coordinator: any;
  try {
    const coordinatorId = env.BATCH_COORDINATOR.idFromName(jobId);
    coordinator = env.BATCH_COORDINATOR.get(coordinatorId);

    await retryCoordinatorFetch(coordinator, '/init', {
      method: 'POST',
      body: JSON.stringify({
        jobId,
        runId,
        dungeonHash,
        totalBatches: batches.length,
      }),
      headers: { 'Content-Type': 'application/json' },
    }, 5);

    console.log(`  ${dungeonName}: initialized BatchCoordinator with ${batches.length} batches`);
  } catch (err) {
    console.error(`  ${dungeonName}: failed to init BatchCoordinator`, err);
    throw err;
  }

  // Process batches with configurable batch-level concurrency.
  // PGCRs within each batch are still processed SEQUENTIALLY (to preserve ordering and reduce burst).
  const PGCR_DELAY_MS = getEnvNumber(env, 'PGCR_DELAY_MS', 50);
  const batchConcurrency = getEnvNumber(env, 'PGCR_BATCH_CONCURRENCY', 1);
  console.log(`  ${dungeonName}: processing batches with concurrency=${batchConcurrency}, pgcrDelayMs=${PGCR_DELAY_MS}`);

  const batchItems = batches.map((b, idx) => ({ batch: b, batchIdx: idx }));

  await promisePool(
    batchItems,
    async (bi) => {
      const { batch, batchIdx } = bi;
      const batchResults: any[] = [];

      // Process each PGCR in this batch SEQUENTIALLY with delay
      for (const item of batch) {
        try {
          const pgcr = await fetchPGCR(item.instanceId, env.BUNGIE_API_KEY);
          
          if (!pgcr) {
            batchResults.push({
              instanceId: item.instanceId,
              period: item.period,
              playtimeFromPgcr: null,
              playtimeFallback: item.playtimeMeta || 0,
              isFullClear: false,
              verified: false,
            });
          } else {
            const isFull = determineClearType(pgcr, item.period);
            let pTime: number | null = null;
            
            try {
              const entries = pgcr.entries || [];
              const match = entries.find((e: any) => {
                const pid = e?.player?.destinyUserInfo?.membershipId;
                const cid = e?.player?.destinyUserInfo?.characterId;
                return (pid && String(pid) === String(job.membershipId)) || 
                       (cid && String(cid) === String(item.characterId));
              });
              if (match) pTime = Number(match?.values?.timePlayedSeconds?.basic?.value ?? null);
            } catch {
              pTime = null;
            }

            batchResults.push({
              instanceId: item.instanceId,
              period: item.period,
              playtimeFromPgcr: pTime,
              playtimeFallback: item.playtimeMeta || 0,
              isFullClear: !!isFull,
              verified: true,
            });
          }
        } catch (err) {
          console.warn(`  ${dungeonName}: PGCR fetch failed for ${item.instanceId}:`, err);
          batchResults.push({
            instanceId: item.instanceId,
            period: item.period,
            playtimeFromPgcr: null,
            playtimeFallback: item.playtimeMeta || 0,
            isFullClear: false,
            verified: false,
          });
        }

        // Small delay between PGCR requests to avoid rate limits
        if (PGCR_DELAY_MS > 0) {
          await sleep(PGCR_DELAY_MS);
        }
      }

      // Aggregate batch results
      let fullClears = 0;
      let playtimeSeconds = 0;
      let lastActivityDate: string | null = null;

      for (const r of batchResults) {
        if (r.playtimeFromPgcr !== null && Number.isFinite(r.playtimeFromPgcr)) {
          playtimeSeconds += Number(r.playtimeFromPgcr);
        } else {
          playtimeSeconds += Number(r.playtimeFallback || 0);
        }
        if (r.verified && r.isFullClear) fullClears++;
        if (!lastActivityDate || r.period > lastActivityDate) {
          lastActivityDate = r.period;
        }
      }

      // Report to BatchCoordinator with retry
      try {
        await retryCoordinatorFetch(coordinator, '/batch', {
          method: 'POST',
          body: JSON.stringify({
            runId,
            dungeonHash,
            batchIndex: batchIdx,
            fullClears,
            playtimeSeconds,
            lastActivityDate,
          }),
          headers: { 'Content-Type': 'application/json' },
        }, 5);
        console.log(`  ${dungeonName}: reported batch ${batchIdx + 1}/${batches.length} to coordinator`);
      } catch (err) {
        console.warn(`  ${dungeonName}: failed to report batch ${batchIdx} to coordinator after retries`, err);
      }

      return null;
    },
    Math.max(1, Math.min(batchConcurrency, batchItems.length))
  );

  // Get aggregated results from BatchCoordinator
  let aggregated: any;
  try {
    aggregated = await pollCoordinatorResult(coordinator, runId, dungeonHash, batches.length);
  } catch (err) {
    console.error(`  ${dungeonName}: failed to get BatchCoordinator results after retries`, err);
    throw err;
  }

  const newFullClears = Number(aggregated.fullClears || 0);
  const newPlaytime = Number(aggregated.playtimeSeconds || 0);

  console.log(`  ${dungeonName}: aggregated results - fullClears=${newFullClears}, playtime=${newPlaytime}s`);

  // Persist totals
  const totalClears = prevClears + newClears;
  const totalFullClears = prevFullClears + newFullClears;
  const totalPlaytime = prevPlaytime + newPlaytime;

  await upsertMemberDungeonStats(env.DB, {
    clanId: job.clanId,
    membershipId: job.membershipId,
    membershipType: job.membershipType,
    dungeonHash,
    totalClears,
    totalFullClears,
    totalPlaytimeSeconds: totalPlaytime,
    lastProcessedDate,
    lastProcessedInstanceId,
  });

  console.log(`  ${dungeonName}: DB updated - total_clears=${totalClears} (+${newClears}), total_full_clears=${totalFullClears} (+${newFullClears}), playtime=${totalPlaytime}s`);

  await applyClanAggregateDelta(env.DB, job.clanId, dungeonHash, newClears, newFullClears, newPlaytime, !prevRow);
}

/**
 * Fetch activities for all characters with pagination (mode-specific)
 */
async function fetchAllActivitiesForCharacters(
  membershipType: number,
  membershipId: string,
  characterIds: string[],
  mode: number,
  apiKey: string
) {
  const out: Record<string, any[]> = {};
  for (const id of characterIds) out[id] = [];

  let active = [...characterIds];
  let page = 0;
  const pageSize = 250;
  const maxPages = 100;

  while (active.length > 0 && page < maxPages) {
    const promises = active.map(charId =>
      fetchActivitiesForCharacter(membershipType, membershipId, charId, page, mode, pageSize, apiKey)
        .catch(err => {
          console.warn(`fetch activities failed for ${charId} page ${page}:`, err?.message || err);
          return [];
        })
    );
    
    const results = await Promise.all(promises);
    const nextActive: string[] = [];
    
    for (let i = 0; i < active.length; i++) {
      const charId = active[i];
      const activities = results[i] || [];
      if (activities.length > 0) {
        const withChar = activities.map((a: any) => ({ ...a, characterId: charId }));
        out[charId].push(...withChar);
        if (activities.length === pageSize) nextActive.push(charId);
      }
    }
    
    active = nextActive;
    page++;
  }

  // Per-character dedupe by instanceId (prefer completed)
  for (const id of Object.keys(out)) {
    const map = new Map<string, any>();
    for (const a of out[id]) {
      const inst = a.activityDetails?.instanceId || a.instanceId;
      if (!inst) continue;
      const ex = map.get(inst);
      if (!ex) map.set(inst, a);
      else {
        const exC = !!(ex?.values?.completed?.basic?.value === 1);
        const newC = !!(a?.values?.completed?.basic?.value === 1);
        if (!exC && newC) map.set(inst, a);
      }
    }
    out[id] = Array.from(map.values());
  }

  return out;
}

/**
 * Fetch all activities for all characters (combines dungeon mode 82 and story mode 2)
 */
async function fetchAllActivitiesForAllChars(
  membershipType: number,
  membershipId: string,
  characterIds: string[],
  apiKey: string
) {
  const [dungeon, story] = await Promise.all([
    fetchAllActivitiesForCharacters(membershipType, membershipId, characterIds, 82, apiKey),
    fetchAllActivitiesForCharacters(membershipType, membershipId, characterIds, 2, apiKey),
  ]);

  const out: Record<string, any[]> = {};
  for (const id of characterIds) {
    out[id] = [...(dungeon[id] || []), ...(story[id] || [])];
    
    // Final dedupe after merging modes
    const map = new Map<string, any>();
    for (const a of out[id]) {
      const inst = a.activityDetails?.instanceId || a.instanceId;
      if (!inst) continue;
      if (!map.has(inst)) map.set(inst, a);
      else {
        const existing = map.get(inst);
        const exC = !!(existing?.values?.completed?.basic?.value === 1);
        const newC = !!(a?.values?.completed?.basic?.value === 1);
        if (!exC && newC) map.set(inst, a);
      }
    }
    out[id] = Array.from(map.values());
  }

  return out;
}

/**
 * Helper to retry coordinator.fetch calls with exponential backoff
 */
async function retryCoordinatorFetch(coordinator: any, path: string, opts: any, maxAttempts = 5) {
  let attempt = 0;
  let lastErr: any = null;
  while (attempt < maxAttempts) {
    try {
      const url = `https://internal${path}`;
      const res = await coordinator.fetch(url, opts);
      if (res.ok) {
        return res;
      }
      if (res.status >= 400 && res.status < 500 && res.status !== 408) {
        const body = await safeJson(res);
        throw new Error(`Coordinator ${path} failed: ${res.status} ${JSON.stringify(body)}`);
      }
      lastErr = new Error(`Coordinator ${path} returned status ${res.status}`);
    } catch (err) {
      lastErr = err;
    }
    attempt++;
    const backoff = Math.min(1000 * Math.pow(2, attempt), 10000);
    console.log(`  coordinator ${path} retrying in ${backoff}ms (attempt ${attempt}/${maxAttempts})`);
    await sleep(backoff);
  }
  throw lastErr || new Error('Coordinator request failed');
}

async function safeJson(res: Response) {
  try {
    return await res.json();
  } catch {
    try {
      return await res.text();
    } catch {
      return null;
    }
  }
}

/**
 * Poll the coordinator for aggregated results
 */
async function pollCoordinatorResult(coordinator: any, runId: string, dungeonHash: string, totalBatches: number) {
  const minWait = 30_000;
  const waitPerBatch = 1000;
  const maxWait = Math.max(minWait, totalBatches * waitPerBatch, 30_000);
  const start = Date.now();
  let attempt = 0;
  let pollInterval = 200;

  while (Date.now() - start < maxWait) {
    attempt++;
    try {
      const res = await coordinator.fetch(`https://internal/result?runId=${encodeURIComponent(runId)}&dungeonHash=${encodeURIComponent(dungeonHash)}`, { method: 'GET' });
      if (res.ok) {
        const json = await res.json();
        return json;
      } else if (res.status === 408) {
        const elapsed = Date.now() - start;
        console.log(`  pollCoordinatorResult: timeout from coordinator, elapsed=${elapsed}ms, will retry (attempt ${attempt})`);
        await sleep(pollInterval);
        pollInterval = Math.min(1000, Math.floor(pollInterval * 1.5));
        continue;
      } else {
        const body = await safeJson(res);
        console.warn(`  pollCoordinatorResult: coordinator returned status ${res.status}: ${JSON.stringify(body)}`);
        await sleep(Math.min(1000 * attempt, 5000));
        continue;
      }
    } catch (err) {
      console.warn('  pollCoordinatorResult: fetch error, retrying', err);
      await sleep(Math.min(1000 * attempt, 5000));
    }
  }

  // Final attempt
  try {
    console.warn(`  pollCoordinatorResult: maxWait exceeded (${maxWait}ms), doing final fetch`);
    const final = await coordinator.fetch(`https://internal/result?runId=${encodeURIComponent(runId)}&dungeonHash=${encodeURIComponent(dungeonHash)}`, { method: 'GET' });
    if (final.ok) return await final.json();
    const body = await safeJson(final);
    throw new Error(`Final coordinator fetch failed: ${final.status} ${JSON.stringify(body)}`);
  } catch (err) {
    throw new Error(`pollCoordinatorResult failed after ${Date.now() - start}ms: ${err}`);
  }
}