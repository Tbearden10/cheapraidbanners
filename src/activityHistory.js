// Streaming activity history pager used by MemberWorker and computeClearsForMember.
// Exports:
// - fetchActivityPage(membershipType, membershipId, characterId, page, pageSize, env, opts)
// - computePerMemberHistory(membershipType, membershipId, characterId, env, opts)

function _makeHeaders(env) {
  const headers = { Accept: 'application/json' };
  if (env && env.BUNGIE_API_KEY) headers['X-API-Key'] = env.BUNGIE_API_KEY;
  return headers;
}

async function _fetchWithTimeout(url, opts = {}, timeoutMs = 8000) {
  const ctrl = new AbortController();
  const id = setTimeout(() => ctrl.abort(), timeoutMs);
  try {
    return await fetch(url, { ...opts, signal: ctrl.signal });
  } finally {
    clearTimeout(id);
  }
}

async function fetchWithRetries(url, opts = {}, timeoutMs = 8000, retries = 2, backoffBaseMs = 500) {
  let attempt = 0;
  let lastErr = null;
  while (attempt <= retries) {
    try {
      const res = await _fetchWithTimeout(url, opts, timeoutMs);
      if (res && res.ok) return res;
      const status = res ? res.status : null;
      if (status === 429 || (status >= 500 && status < 600)) {
        lastErr = new Error(`transient_status_${status}`);
      } else {
        return res;
      }
    } catch (err) {
      lastErr = err;
    }
    attempt++;
    if (attempt > retries) break;
    const wait = backoffBaseMs * Math.pow(2, attempt - 1) + Math.floor(Math.random() * 200);
    await new Promise(r => setTimeout(r, Math.max(50, wait)));
  }
  throw lastErr || new Error('fetchWithRetries: exhausted');
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
  return success === 1 || success === true;
}

/**
 * fetchActivityPage
 * Fetch a single activity page for a specific character and return processed counts and lastActivityAt.
 * This is the chunk-friendly primitive processors will call.
 */
export async function fetchActivityPage(membershipType, membershipId, characterId, page = 0, pageSize = 250, env = {}, opts = {}) {
  const mode = opts.mode; // optional mode filter
  const modePart = mode ? `&mode=${encodeURIComponent(mode)}` : '';
  const url = `https://www.bungie.net/Platform/Destiny2/${encodeURIComponent(String(membershipType))}/Account/${encodeURIComponent(String(membershipId))}/Character/${encodeURIComponent(String(characterId))}/Stats/Activities/?count=${pageSize}&page=${page}${modePart}`;
  const headers = _makeHeaders(env);
  const timeoutMs = Number(opts.timeoutMs || env.BUNGIE_PER_REQUEST_TIMEOUT_MS || 8000);
  const retries = Number(opts.retries ?? (env.BUNGIE_FETCH_RETRIES ? Number(env.BUNGIE_FETCH_RETRIES) : 2));
  const backoffBaseMs = Number(opts.backoffBaseMs ?? (env.BUNGIE_FETCH_BACKOFF_MS ? Number(env.BUNGIE_FETCH_BACKOFF_MS) : 500));
  const res = await fetchWithRetries(url, { method: 'GET', headers }, timeoutMs, retries, backoffBaseMs);
  if (!res || !res.ok) return { ok: false, activitiesCount: 0, perActivity: {}, lastActivityAt: null, status: res ? res.status : 'no-res' };
  const payload = await res.json().catch(() => null);
  const activities = payload?.Response?.activities ?? payload?.Response?.data?.activities ?? payload?.Response?.activities?.data ?? [];
  const perActivity = {};
  let latestTs = 0;
  if (Array.isArray(activities)) {
    for (const act of activities) {
      try {
        const ref = extractActivityReferenceId(act);
        if (!ref) continue;
        if (!activityIsCompleted(act)) continue;
        const period = act?.period ?? act?.periodStart ?? null;
        if (period) {
          const t = Date.parse(period);
          if (!Number.isNaN(t) && t > latestTs) latestTs = t;
        }
        perActivity[String(ref)] = (perActivity[String(ref)] || 0) + 1;
      } catch (e) {}
    }
  }
  return { ok: true, activitiesCount: Array.isArray(activities) ? activities.length : 0, perActivity, lastActivityAt: latestTs ? new Date(latestTs).toISOString() : null, activities };
}

/**
 * computePerMemberHistory
 * Stream pages for a single character and return per-activity completions and lastActivityAt.
 * This reproduces the earlier behavior and is used by computeClearsForMember for canonical results.
 */
export async function computePerMemberHistory(membershipType, membershipId, characterId, env = {}, opts = {}) {
  const out = { perActivity: {}, lastActivityAt: null };
  if (!membershipType || !membershipId || !characterId) return out;

  const pageSize = Number(opts.pageSize || 250);
  const maxPages = Number(opts.maxPages || (env.BUNGIE_ACTIVITY_MAX_PAGES ? Number(env.BUNGIE_ACTIVITY_MAX_PAGES) : 1000));
  const timeoutMs = Number(opts.timeoutMs || env.BUNGIE_PER_REQUEST_TIMEOUT_MS || 8000);
  const retries = Number(opts.retries ?? (env.BUNGIE_FETCH_RETRIES ? Number(env.BUNGIE_FETCH_RETRIES) : 2));
  const backoffBaseMs = Number(opts.backoffBaseMs ?? (env.BUNGIE_FETCH_BACKOFF_MS ? Number(env.BUNGIE_FETCH_BACKOFF_MS) : 500));
  const modes = Array.isArray(opts.modes) && opts.modes.length ? opts.modes : ['dungeon', 'story'];
  const includeUnfiltered = Boolean(opts.fetchAllActivitiesForCharacter ?? false);

  let latestTs = 0;

  const processPage = (activities) => {
    if (!Array.isArray(activities)) return;
    for (const act of activities) {
      try {
        const ref = extractActivityReferenceId(act);
        if (!ref) continue;
        if (!activityIsCompleted(act)) continue;
        const period = act?.period ?? act?.periodStart ?? null;
        if (period) {
          const t = Date.parse(period);
          if (!Number.isNaN(t) && t > latestTs) latestTs = t;
        }
        out.perActivity[String(ref)] = (out.perActivity[String(ref)] || 0) + 1;
      } catch (e) {}
    }
  };

  async function fetchPage(page, mode) {
    const modePart = mode ? `&mode=${encodeURIComponent(mode)}` : '';
    const url = `https://www.bungie.net/Platform/Destiny2/${encodeURIComponent(String(membershipType))}/Account/${encodeURIComponent(String(membershipId))}/Character/${encodeURIComponent(String(characterId))}/Stats/Activities/?count=${pageSize}&page=${page}${modePart}`;
    const headers = _makeHeaders(env);
    const res = await fetchWithRetries(url, { method: 'GET', headers }, timeoutMs, retries, backoffBaseMs);
    if (!res || !res.ok) return null;
    const payload = await res.json().catch(() => null);
    if (!payload || !payload.Response) return null;
    const activities = payload.Response?.activities ?? payload.Response?.data?.activities ?? payload.Response?.activities?.data ?? null;
    return Array.isArray(activities) ? activities : [];
  }

  for (const mode of modes) {
    let page = 0;
    while (page < maxPages) {
      const activities = await fetchPage(page, mode);
      if (activities === null) break; // repeated failure for this mode
      if (!activities || activities.length === 0) break;
      processPage(activities);
      if (activities.length < pageSize) break;
      page++;
    }
  }

  if (includeUnfiltered) {
    let page = 0;
    while (page < maxPages) {
      const activities = await fetchPage(page, undefined);
      if (activities === null) break;
      if (!activities || activities.length === 0) break;
      processPage(activities);
      if (activities.length < pageSize) break;
      page++;
    }
  }

  out.lastActivityAt = latestTs ? new Date(latestTs).toISOString() : null;
  return out;
}