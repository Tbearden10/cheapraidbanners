// Lightweight Bungie API helper used by the Durable Object to fetch profiles and activity history,
// plus computeClearsForMember and computeClearsForMembers which compute dungeon clears (including Prophecy).
//
// Memory-safe, streaming approach:
// - Each activity page is processed immediately and not retained in memory.
// - No instance deduplication or Sets for counting — every qualifying activity entry counts unless merged
//   with AggregateActivityStats which we use to recover small missing counts.
// - computeClearsForMember now records a mostRecentActivity (instanceId + period + activityHash + characterId)
//   while streaming pages (keeps only one object per member).
//
// Environment notes:
// - env.BUNGIE_API_KEY is used if present
// - env.BUNGIE_PER_REQUEST_TIMEOUT_MS, BUNGIE_FETCH_RETRIES, BUNGIE_FETCH_BACKOFF_MS, BUNGIE_ACTIVITY_MAX_PAGES can be used to tune behavior

export const ACTIVITY_REFERENCE_MAP = [
  { hash: "3834447244", displayName: "Sundered Doctrine", referenceIds: ["247869137","3834447244","3521648250"] },
  { hash: "300092127", displayName: "Vesper's Host", referenceIds: ["1915770060","300092127","4293676253"] },
  { hash: "2004855007", displayName: "Warlord's Ruin", referenceIds: ["2004855007","2534833093"] },
  { hash: "313828469", displayName: "Ghosts of the Deep", referenceIds: ["313828469","124340010","4190119662","1094262727","2961030534","2716998124"] },
  { hash: "1262462921", displayName: "Spire of the Watcher", referenceIds: ["1262462921","3339002067","1225969316","943878085","4046934917","2296818662"] },
  { hash: "2823159265", displayName: "Duality", referenceIds: ["2823159265","3012587626","1668217731"] },
  { hash: "4078656646", displayName: "Grasp of Avarice", referenceIds: ["4078656646","1112917203","3774021532"] },
  { hash: "1077850348", displayName: "Prophecy", referenceIds: ["715153594","3637651331","1077850348","3193125350","1788465402","4148187374"] },
  { hash: "2582501063", displayName: "Pit of Heresy", referenceIds: ["2582501063","1375089621"] },
  { hash: "2032534090", displayName: "The Shattered Throne", referenceIds: ["2032534090"] },
];

// Build reference sets for fast checks (strings)
const DUNGEON_REFERENCE_SET = new Set();
const PROPHECY_REFERENCE_SET = new Set();
for (const entry of ACTIVITY_REFERENCE_MAP) {
  const isProphecy = String(entry.hash) === "1077850348" || (entry.displayName && entry.displayName.toLowerCase().includes('prophecy'));
  for (const r of entry.referenceIds || []) {
    const s = String(r);
    DUNGEON_REFERENCE_SET.add(s);
    if (isProphecy) PROPHECY_REFERENCE_SET.add(s);
  }
}

// https://github.com/Tbearden10/dungeon-info-hub/blob/main/src/lib/bungieApi.js
export async function fetchAggregateActivityStats(membershipType, membershipId, characterId, env = {}, opts = {}) {
  if (!membershipType || !membershipId || !characterId) return null;
  const template = `https://www.bungie.net/Platform/Destiny2/${encodeURIComponent(String(membershipType))}/Account/${encodeURIComponent(String(membershipId))}/Character/${encodeURIComponent(String(characterId))}/Stats/AggregateActivityStats/`;
  const timeoutMs = Number(opts.timeoutMs || env.BUNGIE_PER_REQUEST_TIMEOUT_MS || 8000);
  const retries = Number(opts.retries ?? (env.BUNGIE_FETCH_RETRIES ? Number(env.BUNGIE_FETCH_RETRIES) : 2));
  const backoffBaseMs = Number(opts.backoffBaseMs ?? (env.BUNGIE_FETCH_BACKOFF_MS ? Number(env.BUNGIE_FETCH_BACKOFF_MS) : 500));
  try {
    // reuse fetchJson helper if present in the file; otherwise fetch + json
    if (typeof fetchJson === 'function') {
      const payload = await fetchJson(template, env, timeoutMs, retries, backoffBaseMs);
      return payload || null;
    } else {
      const headers = { Accept: 'application/json' };
      if (env && env.BUNGIE_API_KEY) headers['X-API-Key'] = env.BUNGIE_API_KEY;
      const res = await fetchWithRetries(template, { method: 'GET', headers }, timeoutMs, retries, backoffBaseMs);
      if (!res || !res.ok) return null;
      const json = await res.json().catch(() => null);
      return (json && json.Response) ? json.Response : null;
    }
  } catch (err) {
    // bubble up for caller to handle / log
    throw err;
  }
}

export async function _fetchWithTimeout(url, opts = {}, timeoutMs = 8000) {
  const ctrl = new AbortController();
  const id = setTimeout(() => ctrl.abort(), timeoutMs);
  try {
    const res = await fetch(url, { ...opts, signal: ctrl.signal });
    return res;
  } finally {
    clearTimeout(id);
  }
}

function _makeHeaders(env) {
  const headers = { Accept: 'application/json' };
  if (env && env.BUNGIE_API_KEY) headers['X-API-Key'] = env.BUNGIE_API_KEY;
  return headers;
}

/**
 * fetchWithRetries: retries transient failures (429, 5xx, network/timeouts) with exponential backoff.
 */
export async function fetchWithRetries(url, opts = {}, timeoutMs = 8000, retries = 2, backoffBaseMs = 500) {
  let attempt = 0;
  let lastErr = null;
  while (attempt <= retries) {
    try {
      const res = await _fetchWithTimeout(url, opts, timeoutMs);
      if (res && res.ok) return res;
      // treat 429 and 5xx as transient
      const status = res ? res.status : null;
      if (status === 429 || (status >= 500 && status < 600)) {
        lastErr = new Error(`transient_status_${status}`);
      } else {
        // non-transient non-ok (e.g., 404): return it for caller to handle
        return res;
      }
    } catch (err) {
      lastErr = err;
    }
    attempt += 1;
    if (attempt > retries) break;
    const sleepMs = backoffBaseMs * Math.pow(2, attempt - 1);
    const jitter = Math.round((Math.random() * 0.4 - 0.2) * sleepMs);
    const wait = Math.max(50, sleepMs + jitter);
    await new Promise((resolve) => setTimeout(resolve, wait));
  }
  throw lastErr || new Error('fetchWithRetries: exhausted retries');
}

function extractActivityReferenceId(activity) {
  if (!activity) return null;
  const ref = activity?.activityDetails?.referenceId ?? activity?.activityHash ?? activity?.referenceId ?? null;
  return ref != null ? String(ref) : null;
}
function activityIsCompleted(activity) {
  if (!activity) return false;
  const completed =
    activity?.values?.completed?.basic?.value ??
    activity?.values?.completed?.value ??
    activity?.isCompleted ??
    activity?.completed ??
    null;
  if (completed === 1 || completed === true) return true;
  const success =
    activity?.values?.success?.basic?.value ??
    activity?.values?.success?.value ??
    null;
  if (success === 1 || success === true) return true;
  return false;
}

/**
 * fetchActivityPage - fetches a single page of activities (mode optional).
 * Returns array of activities or null on non-recoverable error.
 */
async function fetchActivityPage(membershipType, membershipId, characterId, env, { page, pageSize = 250, mode, timeoutMs, retries, backoffBaseMs } = {}) {
  const modePart = mode ? `&mode=${encodeURIComponent(mode)}` : '';
  const url = `https://www.bungie.net/Platform/Destiny2/${membershipType}/Account/${encodeURIComponent(membershipId)}/Character/${encodeURIComponent(characterId)}/Stats/Activities/?count=${pageSize}&page=${page}${modePart}`;
  const headers = _makeHeaders(env);
  try {
    const res = await fetchWithRetries(url, { method: 'GET', headers }, Number(timeoutMs || env.BUNGIE_PER_REQUEST_TIMEOUT_MS || 8000), Number(retries ?? 2), Number(backoffBaseMs ?? 500));
    if (!res || !res.ok) return null;
    const payload = await res.json().catch(() => null);
    if (!payload || !payload.Response) return null;
    const activities = payload.Response?.activities ?? payload.Response?.data?.activities ?? payload.Response?.activities?.data ?? null;
    const arr = Array.isArray(activities) ? activities : (Array.isArray(payload.Response) ? payload.Response : null);
    return Array.isArray(arr) ? arr : [];
  } catch (err) {
    // on repeated failures return null so caller can stop pagination for this character
    return null;
  }
}

/**
 * computeClearsForCharacter
 * Stream pages for a single character and return { membershipId, membershipType, characterId, clears, prophecyClears, lastActivityAt, mostRecentActivity }.
 * This is designed to be small/fast: it only processes one character's activity history.
 *
 * opts:
 *  - pageSize, maxPages, timeoutMs, retries, backoffBaseMs
 *  - mode(s) will be 'dungeon' and 'story' filtered by dungeon reference set
 */
export async function computeClearsForCharacter(membershipType, membershipId, characterId, env = {}, opts = {}) {
  const out = { membershipId: String(membershipId || ''), membershipType: Number(membershipType || 0), characterId: String(characterId || ''), clears: 0, prophecyClears: 0, lastActivityAt: null, mostRecentActivity: null };

  if (!membershipId || !membershipType || !characterId) return out;

  const pageSize = Number(opts.pageSize || 250);
  const maxPages = Number(opts.maxPages || (env.BUNGIE_ACTIVITY_MAX_PAGES ? Number(env.BUNGIE_ACTIVITY_MAX_PAGES) : 1000));
  const timeoutMs = Number(opts.timeoutMs || env.BUNGIE_PER_REQUEST_TIMEOUT_MS || 8000);
  const retries = Number(opts.retries ?? (env.BUNGIE_FETCH_RETRIES ? Number(env.BUNGIE_FETCH_RETRIES) : 2));
  const backoffBaseMs = Number(opts.backoffBaseMs ?? (env.BUNGIE_FETCH_BACKOFF_MS ? Number(env.BUNGIE_FETCH_BACKOFF_MS) : 500));

  let latestTs = 0;
  let clears = 0;
  let prophecyClears = 0;
  let mostRecentActivity = null;

  const ALL_DUNGEON_REFERENCE_IDS = DUNGEON_REFERENCE_SET;

  const processActivities = (activities) => {
    if (!Array.isArray(activities)) return;
    for (const act of activities) {
      try {
        const ref = extractActivityReferenceId(act);
        if (!ref) continue;
        if (!ALL_DUNGEON_REFERENCE_IDS.has(String(ref))) continue;
        if (!activityIsCompleted(act)) continue;
        // increase counts
        clears += 1;
        if (PROPHECY_REFERENCE_SET.has(String(ref))) prophecyClears += 1;
        // track latest ts
        const period = act?.period ?? act?.periodStart ?? null;
        if (period) {
          const t = Date.parse(period);
          if (!Number.isNaN(t) && t > latestTs) latestTs = t;
        }
        // track mostRecentActivity for this character if it has instanceId
        const inst = act?.activityDetails?.instanceId ?? act?.activityDetails?.instanceIdHash ?? null;
        if (inst && period) {
          const t = Date.parse(period) || 0;
          if (!mostRecentActivity || t > (Date.parse(mostRecentActivity.period || '') || 0)) {
            mostRecentActivity = {
              instanceId: String(inst),
              period,
              activityHash: String(ref),
              characterId: String(characterId),
              raw: {
                referenceId: ref,
                instanceId: inst,
                period,
                values: act?.values ?? null
              }
            };
          }
        }
      } catch (e) {
        continue;
      }
    }
  };

  // dungeon mode pages
  let page = 0;
  while (page < maxPages) {
    const activities = await fetchActivityPage(membershipType, membershipId, characterId, env, { page, pageSize, mode: 'dungeon', timeoutMs, retries, backoffBaseMs });
    if (!activities || activities.length === 0) break;
    processActivities(activities);
    if (activities.length < pageSize) break;
    page += 1;
  }

  // story mode pages (filtered to dungeon reference ids)
  page = 0;
  while (page < maxPages) {
    const activities = await fetchActivityPage(membershipType, membershipId, characterId, env, { page, pageSize, mode: 'story', timeoutMs, retries, backoffBaseMs });
    if (!activities || activities.length === 0) break;
    processActivities(activities);
    if (activities.length < pageSize) break;
    page += 1;
  }

  out.clears = clears;
  out.prophecyClears = prophecyClears;
  out.lastActivityAt = latestTs ? new Date(latestTs).toISOString() : null;
  out.mostRecentActivity = mostRecentActivity || null;
  return out;
}

/**
 * computeClearsForMember
 * Process a single member and return { membershipId, membershipType, clears, prophecyClears, lastActivityAt, mostRecentActivity }.
 * Stream-pages activity history: for each page process entries immediately and don't hold them in memory.
 * We now keep a perActivity tally from paged results and merge that with AggregateActivityStats per-character,
 * using max(pagedCount, aggregateCount) per activityHash to capture small missing clears.
 *
 * While streaming, we also track the single most recent activity that has an instanceId and completed.
 * This is returned as mostRecentActivity: { instanceId, period, activityHash, characterId, raw }.
 *
 * opts:
 *  - pageSize, maxPages, timeoutMs, retries, backoffBaseMs
 *  - includeDeletedCharacters (bool) to include deleted characters if desired
 *  - fetchAllActivitiesForCharacter (bool) -> if true, also fetch pages with no mode filter (may catch odd placements)
 */
export async function computeClearsForMember(member, env = {}, opts = {}) {
  const membershipId = String(member.membershipId || '');
  const membershipType = Number(member.membershipType || 0);
  const out = { membershipId, membershipType, clears: 0, prophecyClears: 0, lastActivityAt: null, mostRecentActivity: null };

  if (!membershipId || !membershipType) return out;

  const pageSize = Number(opts.pageSize || 250);
  const maxPagesCeiling = Number(opts.maxPages || (env.BUNGIE_ACTIVITY_MAX_PAGES ? Number(env.BUNGIE_ACTIVITY_MAX_PAGES) : 1000));
  const timeoutMs = Number(opts.timeoutMs || env.BUNGIE_PER_REQUEST_TIMEOUT_MS || 8000);
  const retries = Number(opts.retries ?? (env.BUNGIE_FETCH_RETRIES ? Number(env.BUNGIE_FETCH_RETRIES) : 2));
  const backoffBaseMs = Number(opts.backoffBaseMs ?? (env.BUNGIE_FETCH_BACKOFF_MS ? Number(env.BUNGIE_FETCH_BACKOFF_MS) : 500));
  const includeDeleted = Boolean(opts.includeDeletedCharacters ?? false);
  const fetchAllForCharacter = Boolean(opts.fetchAllActivitiesForCharacter ?? false);

  // 1) get character ids via Stats endpoint (preferred)
  let characterIds = [];
  try {
    const stats = await fetchStatsDirect(membershipType, membershipId, env, timeoutMs);
    if (stats && Array.isArray(stats.characters) && stats.characters.length) {
      characterIds = stats.characters
        .filter((c) => includeDeleted ? true : !c.deleted)
        .map((c) => String(c.characterId));
    }
  } catch (e) {
    // ignore and fallback to profile
  }

  // 2) fallback to profile endpoint if needed
  if (!characterIds || characterIds.length === 0) {
    const prof = await fetchProfile(membershipType, membershipId, env, timeoutMs);
    if (prof && prof.ok && prof.payload) {
      const response = prof.payload.Response ?? {};
      const profileData = response?.profile?.data ?? {};
      const charactersData = response?.characters?.data ?? {};
      if (Array.isArray(profileData.characterIds) && profileData.characterIds.length) {
        characterIds = profileData.characterIds.slice();
        if (!includeDeleted && charactersData && typeof charactersData === 'object') {
          characterIds = characterIds.filter((cid) => {
            const ch = charactersData[cid];
            if (!ch) return true;
            if (ch.deleted === true || ch.deleted === 1) return false;
            if (ch.removed === true || ch.deactivated === true) return false;
            return true;
          });
        }
      } else if (charactersData && typeof charactersData === 'object') {
        characterIds = Object.keys(charactersData).filter((cid) => {
          if (includeDeleted) return true;
          const ch = charactersData[cid];
          if (!ch) return true;
          if (ch.deleted === true || ch.deleted === 1) return false;
          if (ch.removed === true || ch.deactivated === true) return false;
          return true;
        });
      }
    }
  }

  if (!characterIds || characterIds.length === 0) {
    return out;
  }

  // We'll keep per-activity tallies from paged results, keyed by referenceId string
  const perActivityPaged = Object.create(null);
  let latestActivityTs = 0;

  // NEW: track the single most recent activity (that has an instanceId) per member while streaming
  let mostRecentActivity = null; // { instanceId, period, activityHash, characterId, raw }

  const ALL_DUNGEON_REFERENCE_IDS = DUNGEON_REFERENCE_SET; // using set is fine for quick in-memory checks

  // For each character stream page-by-page (dungeon mode, then story mode filtered)
  for (const charId of characterIds) {
    // 1) dungeon mode
    let page = 0;
    while (page < maxPagesCeiling) {
      const activities = await fetchActivityPage(membershipType, membershipId, charId, env, {
        page,
        pageSize,
        mode: 'dungeon',
        timeoutMs,
        retries,
        backoffBaseMs
      });
      if (!activities) break; // page fetch failed after retries — stop paging this character
      if (!Array.isArray(activities) || activities.length === 0) break;

      // process page immediately into perActivityPaged and update mostRecentActivity
      for (const act of activities) {
        try {
          const refId = extractActivityReferenceId(act);
          if (!refId) continue;
          if (!ALL_DUNGEON_REFERENCE_IDS.has(String(refId))) continue;
          if (!activityIsCompleted(act)) continue;

          const period = act?.period ?? act?.periodStart ?? act?.periodDetails ?? null;
          if (period) {
            const parsed = Date.parse(period);
            if (!Number.isNaN(parsed) && parsed > latestActivityTs) latestActivityTs = parsed;
            // record most recent activity that contains an instanceId
            const inst = act?.activityDetails?.instanceId ?? act?.activityDetails?.instanceIdHash ?? null;
            if (inst) {
              const instTs = parsed || 0;
              if (!mostRecentActivity || instTs > (Date.parse(mostRecentActivity.period || '') || 0)) {
                mostRecentActivity = {
                  instanceId: String(inst),
                  period: period,
                  activityHash: String(refId),
                  characterId: String(charId),
                  raw: {
                    referenceId: refId,
                    instanceId: inst,
                    period,
                    values: act?.values ?? null
                  }
                };
              }
            }
          }

          perActivityPaged[String(refId)] = (perActivityPaged[String(refId)] || 0) + 1;
        } catch (err) {
          continue;
        }
      }

      if (activities.length < pageSize) break;
      page += 1;
    }

    // 2) story mode (filter by dungeon reference ids)
    page = 0;
    while (page < maxPagesCeiling) {
      const activities = await fetchActivityPage(membershipType, membershipId, charId, env, {
        page,
        pageSize,
        mode: 'story',
        timeoutMs,
        retries,
        backoffBaseMs
      });
      if (!activities) break;
      if (!Array.isArray(activities) || activities.length === 0) break;

      for (const act of activities) {
        try {
          const refId = extractActivityReferenceId(act);
          if (!refId) continue;
          if (!ALL_DUNGEON_REFERENCE_IDS.has(String(refId))) continue;
          if (!activityIsCompleted(act)) continue;
          const period = act?.period ?? act?.periodStart ?? act?.periodDetails ?? null;
          if (period) {
            const parsed = Date.parse(period);
            if (!Number.isNaN(parsed) && parsed > latestActivityTs) latestActivityTs = parsed;
            const inst = act?.activityDetails?.instanceId ?? act?.activityDetails?.instanceIdHash ?? null;
            if (inst) {
              const instTs = parsed || 0;
              if (!mostRecentActivity || instTs > (Date.parse(mostRecentActivity.period || '') || 0)) {
                mostRecentActivity = {
                  instanceId: String(inst),
                  period: period,
                  activityHash: String(refId),
                  characterId: String(charId),
                  raw: {
                    referenceId: refId,
                    instanceId: inst,
                    period,
                    values: act?.values ?? null
                  }
                };
              }
            }
          }
          perActivityPaged[String(refId)] = (perActivityPaged[String(refId)] || 0) + 1;
        } catch (err) {
          continue;
        }
      }

      if (activities.length < pageSize) break;
      page += 1;
    }

    // 3) optional: fetch pages with no mode filter and filter by dungeon reference ids.
    if (fetchAllForCharacter) {
      page = 0;
      while (page < maxPagesCeiling) {
        const activities = await fetchActivityPage(membershipType, membershipId, charId, env, {
          page,
          pageSize,
          mode: undefined,
          timeoutMs,
          retries,
          backoffBaseMs
        });
        if (!activities) break;
        if (!Array.isArray(activities) || activities.length === 0) break;

        for (const act of activities) {
          try {
            const refId = extractActivityReferenceId(act);
            if (!refId) continue;
            if (!ALL_DUNGEON_REFERENCE_IDS.has(String(refId))) continue;
            if (!activityIsCompleted(act)) continue;
            const period = act?.period ?? act?.periodStart ?? act?.periodDetails ?? null;
            if (period) {
              const parsed = Date.parse(period);
              if (!Number.isNaN(parsed) && parsed > latestActivityTs) latestActivityTs = parsed;
              const inst = act?.activityDetails?.instanceId ?? act?.activityDetails?.instanceIdHash ?? null;
              if (inst) {
                const instTs = parsed || 0;
                if (!mostRecentActivity || instTs > (Date.parse(mostRecentActivity.period || '') || 0)) {
                  mostRecentActivity = {
                    instanceId: String(inst),
                    period: period,
                    activityHash: String(refId),
                    characterId: String(charId),
                    raw: {
                      referenceId: refId,
                      instanceId: inst,
                      period,
                      values: act?.values ?? null
                    }
                  };
                }
              }
            }
            perActivityPaged[String(refId)] = (perActivityPaged[String(refId)] || 0) + 1;
          } catch (err) {
            continue;
          }
        }

        if (activities.length < pageSize) break;
        page += 1;
      }
    } // end per-character paging
  } // end for each character

  // At this point perActivityPaged holds counts from paged history.
  // Now fetch AggregateActivityStats per character and merge counts (use max per activityHash)
  const perActivityAgg = Object.create(null);
  for (const charId of characterIds) {
    try {
      const agg = await fetchAggregateActivityStats(membershipType, membershipId, charId, env, {
        timeoutMs,
        retries,
        backoffBaseMs
      });
      if (agg && Array.isArray(agg.activities)) {
        for (const a of agg.activities) {
          try {
            const ah = String(a.activityHash);
            const completions = Number(a?.values?.activityCompletions?.basic?.value || 0);
            if (!completions) continue;
            // only consider activity hashes that are in dungeon set (we'll filter later; but counting now)
            perActivityAgg[ah] = Math.max(perActivityAgg[ah] || 0, completions);
          } catch (err) {
            continue;
          }
        }
      }
    } catch (err) {
      // if aggregate fails for this character, continue — we still have paged data
      // don't treat as fatal; log at caller that invoked this function if desired
      // console.warn('[computeClearsForMember] aggregate fetch failed', membershipId, charId, err && err.message ? err.message : err);
    }
  }

  // Merge paged and aggregate per-activity counts using max(paged, aggregate)
  const mergedActivityCounts = Object.create(null);
  const allKeys = new Set([...Object.keys(perActivityPaged), ...Object.keys(perActivityAgg)]);
  for (const k of allKeys) {
    const p = Number(perActivityPaged[k] || 0);
    const a = Number(perActivityAgg[k] || 0);
    mergedActivityCounts[k] = Math.max(p, a);
  }

  // Reduce merged counts into clears and prophecyClears based on known reference sets
  let memberClears = 0;
  let memberProphecyClears = 0;
  for (const [ref, count] of Object.entries(mergedActivityCounts)) {
    const refStr = String(ref);
    if (!ALL_DUNGEON_REFERENCE_IDS.has(refStr)) continue;
    const n = Number(count || 0);
    memberClears += n;
    if (PROPHECY_REFERENCE_SET.has(refStr)) memberProphecyClears += n;
  }

  out.clears = memberClears;
  out.prophecyClears = memberProphecyClears;
  out.lastActivityAt = latestActivityTs ? new Date(latestActivityTs).toISOString() : null;
  out.mostRecentActivity = mostRecentActivity || null; // <-- added field
  return out;
}

/**
 * computeClearsForMembers
 * Process first `userLimit` users (opts.userLimit), in controlled concurrency.
 * This uses computeClearsForMember internally (which is streaming and memory-safe).
 *
 * opts:
 *  - userLimit (number)
 *  - concurrency (parallel member workers, default 2)
 *  - pageSize, maxPages, timeoutMs, retries, backoffBaseMs, includeDeletedCharacters
 *  - fetchAllActivitiesForCharacter (bool) forwarded to computeClearsForMember
 */
export async function computeClearsForMembers(members = [], env = {}, opts = {}) {
  const fetchedAt = new Date().toISOString();
  if (!Array.isArray(members) || members.length === 0) {
    return { fetchedAt, clears: 0, prophecyClears: 0, perMember: [], totalMembers: 0, processedCount: 0 };
  }

  const userLimit = Number(opts.userLimit ?? (env.BUNGIE_MEMBER_PROCESS_LIMIT ? Number(env.BUNGIE_MEMBER_PROCESS_LIMIT) : members.length));
  const limit = Math.max(0, Math.min(members.length, userLimit));
  const slice = members.slice(0, limit);

  const concurrency = Math.max(1, Number(opts.concurrency || (env.BUNGIE_FETCH_CONCURRENCY ? Number(env.BUNGIE_FETCH_CONCURRENCY) : 2)));

  async function workerPool(items, worker, parallel) {
    const results = new Array(items.length);
    let i = 0;
    const next = async () => {
      while (true) {
        const idx = i++;
        if (idx >= items.length) return;
        try {
          results[idx] = await worker(items[idx], idx);
        } catch (err) {
          results[idx] = { error: err && err.message ? err.message : String(err) };
        }
      }
    };
    const pool = [];
    for (let p = 0; p < Math.min(parallel, items.length); p++) pool.push(next());
    await Promise.all(pool);
    return results;
  }

  const perMember = await workerPool(slice, async (m) => {
    // call single-member worker with streaming options forwarded
    return computeClearsForMember(m, env, {
      pageSize: opts.pageSize,
      maxPages: opts.maxPages,
      timeoutMs: opts.timeoutMs,
      retries: opts.retries,
      backoffBaseMs: opts.backoffBaseMs,
      includeDeletedCharacters: opts.includeDeletedCharacters,
      fetchAllActivitiesForCharacter: opts.fetchAllActivitiesForCharacter
    });
  }, concurrency);

  let totalClears = 0;
  let totalProphecyClears = 0;
  const per = [];
  for (const r of perMember) {
    if (!r || r.error) continue;
    totalClears += Number(r.clears || 0);
    totalProphecyClears += Number(r.prophecyClears || 0);
    // keep only small per-member summary
    per.push({
      membershipId: r.membershipId,
      membershipType: r.membershipType,
      clears: r.clears,
      prophecyClears: r.prophecyClears,
      lastActivityAt: r.lastActivityAt,
      mostRecentActivity: r.mostRecentActivity || null
    });
  }

  return {
    fetchedAt,
    clears: totalClears,
    prophecyClears: totalProphecyClears,
    perMember: per,
    totalMembers: members.length,
    processedCount: per.length
  };
}