// Hybrid member stats processor (patched with coordinator result retries and better logging)
// - Inline for small runs
// - DO for heavy runs
// - Retries GET /result on coordinator when it returns 408 (timeout)

import type { Env, MemberJob } from '../types';
import { ACTIVITY_REFERENCE_MAP } from '../constants/activityReferenceMap';
import { fetchCharactersForMember, fetchActivitiesForCharacter, fetchPGCR } from '../api/bungieApi';
import { upsertMemberDungeonStats } from '../db/queries';
import { applyClanAggregateDelta } from '../db/aggregateHelpers';
import { withRateLimit } from './rateLimit';

// Promise pool helper (unchanged)
export async function promisePool<T, R>(items: T[], worker: (item: T, idx: number) => Promise<R>, concurrency: number): Promise<R[]> {
  const results: R[] = new Array(items.length);
  let idx = 0;
  const runners = new Array(Math.min(concurrency, items.length)).fill(0).map(async () => {
    while (true) {
      const i = idx++;
      if (i >= items.length) return;
      try {
        results[i] = await worker(items[i], i);
      } catch (err) {
        (results as any)[i] = undefined;
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
  const HAUNTED_START_MS = Date.parse('2022-05-24T17:00:00.000Z');
  const WITCH_QUEEN_START_MS = Date.parse('2022-02-22T17:00:00.000Z');
  const BEYOND_LIGHT_START_MS = Date.parse('2020-11-10T17:00:00.000Z');
  if (timestamp >= HAUNTED_START_MS) return Boolean(pgcrResponse.activityWasStartedFromBeginning);
  if (timestamp < BEYOND_LIGHT_START_MS) {
    const startingPhaseIndex = pgcrResponse.startingPhaseIndex;
    return startingPhaseIndex === 0;
  }
  if (timestamp >= WITCH_QUEEN_START_MS) return Boolean(pgcrResponse.activityWasStartedFromBeginning);
  return true;
}

async function fetchPgcrWithRetries(instanceId: string, env: Env) {
  const baseBackoff = getEnvNumber(env, 'BUNGIE_FETCH_BACKOFF_MS', 500);
  const retries = getEnvNumber(env, 'BUNGIE_FETCH_RETRIES', 2);
  return withRateLimit(() => fetchPGCR(instanceId, env.BUNGIE_API_KEY), baseBackoff, retries).catch((err) => {
    console.warn(`fetchPGCR ${instanceId} failed:`, err?.message ?? err);
    return null;
  });
}

/** Helper to build a deterministic DO name for a member (per-member DO) */
function makeCoordinatorName(membershipId: string) {
  return `member-${membershipId}`;
}

/** Process a batch using prefetch map or network-only */
async function processBatchUsingMap(batchItems: { instanceId: string; period: string; playtime: number }[], env: Env, prefetchMap: Map<string, any | null> | undefined, concurrency: number) {
  let requested = 0, success = 0, totalLatency = 0;
  const results = await promisePool(
    batchItems,
    async (item) => {
      requested++;
      const start = Date.now();
      let pgcr = prefetchMap && prefetchMap.has(item.instanceId) ? prefetchMap.get(item.instanceId) : undefined;
      if (pgcr === undefined) {
        pgcr = await fetchPgcrWithRetries(item.instanceId, env);
      }
      const took = Date.now() - start;
      if (pgcr) { success++; totalLatency += took; }
      const isFull = pgcr ? determineClearType(pgcr, item.period) : false;
      return { instanceId: item.instanceId, isFull, playtime: item.playtime, period: item.period };
    },
    concurrency
  );
  const avgLatency = success ? Math.round(totalLatency / success) : 0;
  console.log(`PGCR batch requested=${requested} success=${success} avgLatency=${avgLatency}ms`);
  let fullClears = 0, playtimeSeconds = 0, lastActivityDate: string | null = null;
  for (const r of results) {
    if (!r) continue;
    if (r.isFull) fullClears++;
    playtimeSeconds += r.playtime || 0;
    if (!lastActivityDate || (r.period && r.period > lastActivityDate)) lastActivityDate = r.period;
  }
  return { fullClears, playtimeSeconds, lastActivityDate };
}

async function processBatchNetworkOnly(batchItems: { instanceId: string; period: string; playtime: number }[], env: Env, concurrency: number) {
  return processBatchUsingMap(batchItems, env, undefined as any, concurrency);
}

/** Prefetch helper (unchanged) */
async function prefetchInstanceWindow(instanceIds: string[], env: Env, outMap: Map<string, any | null>, concurrency: number, maxItems: number) {
  const toFetch = instanceIds.slice(0, maxItems);
  await promisePool(
    toFetch,
    async (instanceId) => {
      if (outMap.has(instanceId)) return null;
      const pgcr = await fetchPgcrWithRetries(instanceId, env);
      outMap.set(instanceId, pgcr ?? null);
      return null;
    },
    concurrency
  );
}

/** Helper: fetch coordinator result with retries on 408 */
async function fetchCoordinatorResultWithRetries(coordinator: any, runId: string, dungeonHash: string, attempts = 3) {
  const url = `https://internal/result?runId=${encodeURIComponent(runId)}&dungeonHash=${encodeURIComponent(dungeonHash)}`;
  for (let i = 0; i < attempts; i++) {
    const res = await coordinator.fetch(url, { method: 'GET' });
    if (res.status === 200) {
      try {
        return await res.json();
      } catch (e) {
        return null;
      }
    }
    if (res.status === 408) {
      // timeout waiting for batches; retry with backoff
      const wait = 200 * Math.pow(2, i);
      console.warn(`Coordinator returned 408, retrying in ${wait}ms (attempt ${i + 1}/${attempts})`);
      await new Promise((r) => setTimeout(r, wait));
      continue;
    }
    // other statuses: try to parse body and return
    try {
      return await res.json();
    } catch (e) {
      return null;
    }
  }
  return null;
}

/**
 * HYBRID PROCESSING:
 * - Inline path (no DO) when workload is small (batches <= DO_THRESHOLD OR items <= ACTIVITY_THRESHOLD)
 * - DO path when workload is heavy (batches > DO_THRESHOLD AND items > ACTIVITY_THRESHOLD)
 */
export async function processMemberStats(env: Env, job: MemberJob): Promise<void> {
  const BATCH_SIZE = getEnvNumber(env, 'BATCH_SIZE', 25);
  const PREFETCH_BATCH_WINDOW = getEnvNumber(env, 'PREFETCH_BATCH_WINDOW', 2);
  const MAX_PREFETCH_ITEMS_PER_WINDOW = getEnvNumber(env, 'MAX_PREFETCH_ITEMS_PER_WINDOW', 200);
  const BUNGIE_FETCH_CONCURRENCY = getEnvNumber(env, 'BUNGIE_FETCH_CONCURRENCY', 6);
  const BATCH_PROCESS_CONCURRENCY = getEnvNumber(env, 'BATCH_PROCESS_CONCURRENCY', 2);
  const DUNGEON_PROCESS_CONCURRENCY = getEnvNumber(env, 'DUNGEON_PROCESS_CONCURRENCY', 2);

  // HYBRID knobs
  const DO_THRESHOLD = getEnvNumber(env, 'DO_THRESHOLD', 1); // use DO when batches > DO_THRESHOLD
  const ACTIVITY_THRESHOLD = getEnvNumber(env, 'ACTIVITY_THRESHOLD', 100); // or when items > this

  const startTime = Date.now();
  console.log(`Processing ${job.displayName} (${job.membershipId})`);

  // STEP 1: characters
  const characters = await fetchCharactersForMember(job.membershipId, job.membershipType, env.BUNGIE_API_KEY);
  if (!characters || characters.length === 0) {
    console.log(`[${job.displayName}] No characters, skipping`);
    return;
  }
  const characterIds = characters.map((c: any) => c.characterId);

  // STEP 2: activities (dungeon + story)
  const dungeonActivities = await fetchAllActivitiesForCharacters(job.membershipType, job.membershipId, characterIds, 82, env.BUNGIE_API_KEY, job.displayName);
  const storyActivities = await fetchAllActivitiesForCharacters(job.membershipType, job.membershipId, characterIds, 2, env.BUNGIE_API_KEY, job.displayName);

  // Merge + dedupe per character
  const mergedByChar: Record<string, any[]> = {};
  for (const charId of characterIds) {
    mergedByChar[charId] = [...(dungeonActivities[charId] || []), ...(storyActivities[charId] || [])];
    const map = new Map<string, any>();
    mergedByChar[charId].forEach((a) => {
      const id = a.activityDetails?.instanceId ?? a.instanceId;
      if (!id) return;
      const existing = map.get(id);
      if (!existing) map.set(id, a);
      else {
        const exComp = existing?.values?.completed?.basic?.value === 1;
        const newComp = a?.values?.completed?.basic?.value === 1;
        if (!exComp && newComp) map.set(id, a);
      }
    });
    mergedByChar[charId] = Array.from(map.values());
  }

  // Group by dungeon
  const activitiesByDungeon: Record<string, any[]> = {};
  for (const d of ACTIVITY_REFERENCE_MAP) activitiesByDungeon[String(d.hash)] = [];
  Object.values(mergedByChar).forEach((arr) => {
    arr.forEach((activity) => {
      const ref = extractActivityReferenceId(activity);
      const instanceId = activity.activityDetails?.instanceId ?? activity.instanceId;
      if (!ref || !instanceId) return;
      for (const d of ACTIVITY_REFERENCE_MAP) {
        if (d.referenceIds.includes(String(ref))) {
          activitiesByDungeon[String(d.hash)].push(activity);
          break;
        }
      }
    });
  });

  // Process dungeons in parallel (bounded)
  const dungeonEntries = Object.entries(activitiesByDungeon).filter(([, acts]) => (acts || []).length > 0);

  await promisePool(
    dungeonEntries,
    async ([dungeonHash, activities]) => {
      // previous DB row (to compute delta)
      const prevRow = await env.DB.prepare(`
        SELECT total_full_clears, total_playtime_seconds, last_processed_date
        FROM member_dungeon_stats
        WHERE clan_id = ? AND membership_id = ? AND dungeon_hash = ?
        LIMIT 1
      `).bind(job.clanId, job.membershipId, dungeonHash).first();

      const prevFull = prevRow ? Number((prevRow as any).total_full_clears ?? 0) : null;
      const prevPlay = prevRow ? Number((prevRow as any).total_playtime_seconds ?? 0) : null;
      const prevLastProcessed = prevRow ? (prevRow as any).last_processed_date ?? null : null;

      const globalCutoff = job.lastProcessedDate ?? null;
      let cutoffTs: number | null = null;
      const cutoffStr = prevLastProcessed ?? globalCutoff;
      if (cutoffStr) {
        const t = Date.parse(cutoffStr);
        if (!Number.isNaN(t)) cutoffTs = t;
      }

      // Filter completed activities and cutoff
      let completed = activities.filter((a) => a.values?.completed?.basic?.value === 1);
      if (cutoffTs) {
        completed = completed.filter((a) => {
          const p = a.period ?? a.activityDetails?.period;
          if (!p) return true;
          const t = Date.parse(p);
          return isNaN(t) ? true : t > cutoffTs!;
        });
      }

      if (!completed || completed.length === 0) return null;

      // Normalize and sort by playtime (small -> large)
      const items = completed
        .map((a) => ({
          instanceId: String(a.activityDetails?.instanceId ?? a.instanceId),
          period: a.period ?? a.activityDetails?.period,
          playtime: a.values?.timePlayedSeconds?.basic?.value || 0,
        }))
        .filter(Boolean)
        .sort((x, y) => (x.playtime || 0) - (y.playtime || 0));

      // split into batches
      const batches: any[][] = [];
      for (let i = 0; i < items.length; i += BATCH_SIZE) batches.push(items.slice(i, i + BATCH_SIZE));

      console.log(`[${job.displayName}] ${dungeonHash}: items=${items.length} batches=${batches.length}`);

      // Decide path: INLINE or DO
      const useDO = (batches.length > DO_THRESHOLD) && (items.length > ACTIVITY_THRESHOLD);

      // Prefetch small window regardless (helps both paths)
      let prefetchMap: Map<string, any | null> | undefined;
      if (PREFETCH_BATCH_WINDOW > 0 && batches.length > 0) {
        const windowIds: string[] = [];
        for (let w = 0; w < Math.min(PREFETCH_BATCH_WINDOW, batches.length); w++) {
          for (const it of batches[w]) {
            if (windowIds.length >= MAX_PREFETCH_ITEMS_PER_WINDOW) break;
            windowIds.push(it.instanceId);
          }
          if (windowIds.length >= MAX_PREFETCH_ITEMS_PER_WINDOW) break;
        }
        if (windowIds.length > 0) {
          prefetchMap = new Map<string, any | null>();
          await prefetchInstanceWindow(windowIds, env, prefetchMap, BUNGIE_FETCH_CONCURRENCY, MAX_PREFETCH_ITEMS_PER_WINDOW);
          console.log(`[${job.displayName}] Prefetched ${prefetchMap.size}/${windowIds.length} PGCRs`);
        }
      }

      const processOneBatch = async (batchItems: any[]) => {
        if (prefetchMap) return processBatchUsingMap(batchItems, env, prefetchMap, BUNGIE_FETCH_CONCURRENCY);
        return processBatchNetworkOnly(batchItems, env, BUNGIE_FETCH_CONCURRENCY);
      };

      if (!useDO) {
        // INLINE MULTI-BATCH: process all batches locally (no DO) using bounded concurrency and aggregate locally
        console.log(`[${job.displayName}] Using INLINE path for ${dungeonHash} (batches=${batches.length}, items=${items.length})`);
        const perBatchResults = await promisePool(
          batches,
          async (batchItems) => {
            return await processOneBatch(batchItems);
          },
          Math.max(1, Math.min(BATCH_PROCESS_CONCURRENCY, batches.length))
        );

        // Aggregate locally
        let totalFull = 0, totalPlay = 0, lastActivityDate: string | null = null;
        for (const r of perBatchResults) {
          if (!r) continue;
          totalFull += Number(r.fullClears || 0);
          totalPlay += Number(r.playtimeSeconds || 0);
          if (r.lastActivityDate && (!lastActivityDate || r.lastActivityDate > lastActivityDate)) lastActivityDate = r.lastActivityDate;
        }

        // Upsert and apply deltas
        const newFull = totalFull;
        const newPlay = totalPlay;
        const wasNewRow = prevRow == null;

        await upsertMemberDungeonStats(env.DB, {
          clanId: job.clanId,
          membershipId: job.membershipId,
          membershipType: job.membershipType,
          dungeonHash,
          totalFullClears: newFull,
          totalPlaytimeSeconds: newPlay,
          lastProcessedDate: lastActivityDate ?? null,
        });

        const fullDelta = newFull - (prevFull ?? 0);
        const playDelta = newPlay - (prevPlay ?? 0);
        if (fullDelta !== 0 || playDelta !== 0 || wasNewRow) {
          await applyClanAggregateDelta(env.DB, job.clanId, dungeonHash, fullDelta, playDelta, !!wasNewRow);
        }

        return null;
      }

      // DO path (heavy run) - use per-member DO coordinator
      const doName = makeCoordinatorName(job.membershipId);
      const runId = `${Date.now().toString(36)}-${Math.random().toString(36).slice(2,8)}`;
      const coordinatorId = env.BATCH_COORDINATOR.idFromName(doName);
      const coordinator = env.BATCH_COORDINATOR.get(coordinatorId);
      console.log(`[${job.displayName}] Using DO path for ${dungeonHash}. DO=${doName} id=${(coordinatorId as any).toString?.() || String(coordinatorId)}`);

      // init run/dungeon
      await coordinator.fetch('https://internal/init', {
        method: 'POST',
        body: JSON.stringify({ jobId: doName, runId, dungeonHash, totalBatches: batches.length }),
        headers: { 'Content-Type': 'application/json' },
      });

      // post batches (each includes runId + dungeonHash)
      await promisePool(
        batches,
        async (batchItems, index) => {
          const result = await processOneBatch(batchItems);
          try {
            await coordinator.fetch('https://internal/batch', {
              method: 'POST',
              body: JSON.stringify({
                runId,
                dungeonHash,
                batchIndex: index,
                fullClears: result.fullClears,
                playtimeSeconds: result.playtimeSeconds,
                lastActivityDate: result.lastActivityDate,
              }),
              headers: { 'Content-Type': 'application/json' },
            });
          } catch (err) {
            console.error(`[${job.displayName}] Failed to post batch to coordinator`, err);
          }
          return null;
        },
        Math.max(1, Math.min(BATCH_PROCESS_CONCURRENCY, batches.length))
      );

      // request aggregated result for this dungeon/run with retries
      const aggregated = await fetchCoordinatorResultWithRetries(coordinator, runId, dungeonHash);
      const newFull = Number(aggregated?.fullClears ?? 0);
      const newPlay = Number(aggregated?.playtimeSeconds ?? 0);
      const wasNewRow = prevRow == null;

      await upsertMemberDungeonStats(env.DB, {
        clanId: job.clanId,
        membershipId: job.membershipId,
        membershipType: job.membershipType,
        dungeonHash,
        totalFullClears: newFull,
        totalPlaytimeSeconds: newPlay,
        lastProcessedDate: aggregated?.lastActivityDate ?? null,
      });

      const fullDelta = newFull - (prevFull ?? 0);
      const playDelta = newPlay - (prevPlay ?? 0);
      if (fullDelta !== 0 || playDelta !== 0 || wasNewRow) {
        await applyClanAggregateDelta(env.DB, job.clanId, dungeonHash, fullDelta, playDelta, !!wasNewRow);
      }

      return null;
    },
    DUNGEON_PROCESS_CONCURRENCY
  );

  const duration = ((Date.now() - startTime) / 1000).toFixed(2);
  console.log(`Completed ${job.displayName} in ${duration}s`);
  return;
}

/** fetchAllActivitiesForCharacters unchanged - included for completeness */
async function fetchAllActivitiesForCharacters(
  membershipType: number,
  membershipId: string,
  characterIds: string[],
  mode: number,
  apiKey: string,
  displayName: string
): Promise<Record<string, any[]>> {
  const allActivities: Record<string, any[]> = {};
  characterIds.forEach((id) => (allActivities[id] = []));
  let activeCharacters = [...characterIds];
  let page = 0;
  const pageSize = 250;
  while (activeCharacters.length > 0) {
    const results = await Promise.allSettled(
      activeCharacters.map((characterId) =>
        fetchActivitiesForCharacter(membershipType, membershipId, characterId, page, mode, pageSize, apiKey)
      )
    );
    const newActive: string[] = [];
    for (let i = 0; i < results.length; i++) {
      const characterId = activeCharacters[i];
      const result = results[i];
      if (result.status === 'fulfilled' && result.value.length > 0) {
        allActivities[characterId].push(...result.value);
        if (result.value.length === pageSize) newActive.push(characterId);
      }
    }
    activeCharacters = newActive;
    page++;
    if (page > 100) {
      console.log(`[${displayName}] Hit page limit (100)`);
      break;
    }
  }
  return allActivities;
}

function extractActivityReferenceId(activity: any): string | null {
  if (!activity) return null;
  const ref = activity?.activityDetails?.referenceId ?? activity?.activityHash ?? activity?.referenceId ?? null;
  return ref != null ? String(ref) : null;
}