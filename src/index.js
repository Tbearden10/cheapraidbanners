// Entrypoint worker: HTTP endpoints for frontend/admin and scheduled cron.
// Exports Durable Objects used by wrangler.
export { GlobalStats } from './global_stats.js';
export { MemberWorker } from './member_worker.js';

const DEFAULT_TIMEOUT = 8000;

async function fetchWithTimeout(url, opts = {}, timeoutMs = DEFAULT_TIMEOUT) {
  const ctrl = new AbortController();
  const id = setTimeout(() => ctrl.abort(), timeoutMs);
  try {
    const res = await fetch(url, { ...opts, signal: ctrl.signal });
    return res;
  } finally {
    clearTimeout(id);
  }
}

function normalizeClearsSnapshot(snap) {
  if (!snap) return snap;
  const out = { ...snap };
  if (Array.isArray(out.perMember)) {
    out.perMember = out.perMember.slice().sort((a, b) => String(a.membershipId).localeCompare(String(b.membershipId)));
  }
  return out;
}
function snapshotsEqual(a, b) {
  try {
    if (!a && !b) return true;
    if (!a || !b) return false;
    return JSON.stringify(normalizeClearsSnapshot(a)) === JSON.stringify(normalizeClearsSnapshot(b));
  } catch (e) {
    return false;
  }
}

function shouldWait(url, request) {
  try {
    const u = new URL(url);
    if (u.searchParams.has('sync') && u.searchParams.get('sync') === '1') return true;
  } catch (e) {}
  const waitHeader = request.headers.get('x-wait');
  if (waitHeader && (waitHeader === '1' || waitHeader.toLowerCase() === 'true')) return true;
  return false;
}

function jsonResponse(obj, status = 200) {
  return new Response(JSON.stringify(obj), { status, headers: { 'Content-Type': 'application/json' }});
}

// Build partial snapshot from per-member KV entries (member_clears:<id>)
async function buildPartialSnapshotFromMemberKV(env) {
  if (!env.BUNGIE_STATS) return null;
  try {
    const listed = await env.BUNGIE_STATS.list({ prefix: 'member_clears:' });
    const keys = (listed && listed.keys) ? listed.keys.map(k => k.name) : [];
    if (!keys.length) return null;
    const perMember = [];
    let total = 0;
    let totalProphecy = 0;
    for (const key of keys) {
      try {
        const raw = await env.BUNGIE_STATS.get(key);
        if (!raw) continue;
        const kv = JSON.parse(raw);
        const id = key.replace(/^member_clears:/, '');
        const c = Number(kv.clears || 0);
        const p = Number(kv.prophecyClears || 0);
        total += c;
        totalProphecy += p;
        perMember.push({
          membershipId: id,
          membershipType: Number(kv.membershipType || kv.membership_type || 0),
          clears: Number(c),
          prophecyClears: Number(p),
          lastActivityAt: kv.lastActivityAt || kv.fetchedAt || null,
          mostRecentActivity: kv.mostRecentActivity || null
        });
      } catch (e) {
        // skip malformed
      }
    }
    // build overall mostRecentClanActivity using same logic as GlobalStats.aggregate (simple)
    // For simplicity here we return partial perMember; GlobalStats DO will build canonical snapshot.
    return {
      fetchedAt: new Date().toISOString(),
      source: 'partial-aggregate',
      clears: total,
      prophecyClears: totalProphecy,
      perMember,
      memberCount: perMember.length,
      processedCount: perMember.length
    };
  } catch (e) {
    console.warn('[index] buildPartialSnapshotFromMemberKV failed', e && e.message ? e.message : e);
    return null;
  }
}

// Helper: fetch PGCR from Bungie and cache in KV
async function fetchPgcrAndCache(env, instanceId, cacheTtlSec = 86400) {
  if (!instanceId) throw new Error('missing_instanceId');
  const cacheKey = `pgcr:${instanceId}`;
  if (env.BUNGIE_STATS) {
    try {
      const cached = await env.BUNGIE_STATS.get(cacheKey);
      if (cached) {
        try { return JSON.parse(cached); } catch (e) { /* fallthrough to refetch */ }
      }
    } catch (e) {
      console.warn('[index] pgcr: KV read failed', e && e.message ? e.message : e);
    }
  }

  // Build Bungie PGCR URL (use endpoints per Bungie API)
  const apiKey = env.BUNGIE_API_KEY || '';
  const url = `https://www.bungie.net/Platform/Destiny2/Stats/PostGameCarnageReport/${encodeURIComponent(instanceId)}/`;
  const headers = { Accept: 'application/json' };
  if (apiKey) headers['X-API-Key'] = apiKey;

  const res = await fetch(url, { method: 'GET', headers, redirect: 'follow' });
  if (!res || !res.ok) {
    const text = await res.text().catch(() => null);
    const err = new Error(`bungie pgcr non-ok: ${res ? res.status : 'no_resp'} ${text ? text.slice(0,200) : ''}`);
    err.status = res ? res.status : null;
    throw err;
  }

  const payload = await res.json().catch(() => null);
  if (!payload) throw new Error('invalid_pgcr_json');

  // Normalize a small subset for frontend (avoid storing huge blob if desired) — but store full payload too
  const normalized = {
    fetchedAt: new Date().toISOString(),
    instanceId,
    raw: payload,
    // try to derive some friendly fields
    period: payload?.Response?.period ?? payload?.Response?.period ?? null,
    activityHash: payload?.Response?.activityDetails?.referenceId ?? payload?.Response?.activityDetails?.activityHash ?? null,
    mode: payload?.Response?.activityDetails?.mode ?? null,
    activityName: payload?.Response?.activityDetails?.name ?? null,
    entries: Array.isArray(payload?.Response?.entries) ? payload.Response.entries.map(e => ({
      characterId: e.characterId ?? e.character_id ?? null,
      membershipId: e.player?.destinyUserInfo?.membershipId ?? null,
      displayName: (e.player?.destinyUserInfo?.displayName ?? e.player?.destinyUserInfo?.bungieGlobalDisplayName ?? null) || null,
      values: e.values ?? null
    })) : []
  };

  // cache full payload in KV to allow more detailed inspection later
  if (env.BUNGIE_STATS) {
    try {
      await env.BUNGIE_STATS.put(cacheKey, JSON.stringify(normalized), { expirationTtl: Number(cacheTtlSec) });
    } catch (e) {
      console.warn('[index] pgcr: KV write failed', e && e.message ? e.message : e);
    }
  }

  return normalized;
}

async function readCachedMembersFromKV(env) {
  if (!env.BUNGIE_STATS) return null;
  try {
    const listed = await env.BUNGIE_STATS.list({ prefix: 'member:' });
    const keys = (listed && listed.keys) ? listed.keys.map(k => k.name) : [];
    const members = [];
    for (const key of keys) {
      try {
        const raw = await env.BUNGIE_STATS.get(key);
        if (!raw) continue;
        const parsed = JSON.parse(raw);
        members.push(parsed);
      } catch (e) {
        // skip malformed
      }
    }
    return { members, memberCount: members.length, fetchedAt: new Date().toISOString() };
  } catch (e) {
    console.warn('[index] readCachedMembersFromKV failed', e && e.message ? e.message : e);
    return null;
  }
}

export default {
  // HTTP handler for frontend & admin
  async fetch(request, env) {
    const url = new URL(request.url);
    const path = url.pathname;
    const search = url.searchParams;

    // GET /members
    if (request.method === 'GET' && path === '/members') {
      const wantFresh = search.has('fresh');
      const wantCached = search.has('cached') || (!wantFresh && !!env.BUNGIE_STATS);
      const wait = shouldWait(request.url, request);

      // Always attempt to read cached members first to return something quickly for UI.
      const cached = await readCachedMembersFromKV(env);

      // Fire an unconditional background refresh of members (user requested that "after checks from bungie no matter what too")
      // If caller requested synchronous wait, we'll forward and await below instead.
      try {
        const id = env.GLOBAL_STATS.idFromName('global');
        const stub = env.GLOBAL_STATS.get(id);
        if (!wait && cached && Array.isArray(cached.members)) {
          // Background refresh (fire-and-forget) but always attempted
          stub.fetch('https://globalstats.local/update-members', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({}) })
            .then(async (r) => { try { const t = await r.text().catch(() => null); console.log('[index] background update-members', r.status, (t||'').slice(0,200)); } catch(e){} })
            .catch(err => console.error('[index] bg update-members failed', err));
          // Return cached right away, annotating that a background refresh was kicked off.
          const out = { fetchedAt: cached.fetchedAt, members: cached.members, memberCount: cached.memberCount, _backgroundRefresh: true };
          return jsonResponse(out);
        }

        // If caller explicitly asked for fresh OR there's no cached data, forward to DO and (optionally) wait.
        if (wait) {
          // synchronous forwarding to DO; this will run update-members and return members array.
          const res = await stub.fetch('https://globalstats.local/update-members', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({}) });
          const text = await res.text().catch(() => null);
          // If DO returned JSON members, pass it through; otherwise return what it returned.
          try {
            return new Response(text || JSON.stringify({ ok: false }), { status: res.status, headers: { 'Content-Type': 'application/json' }});
          } catch (e) {
            return jsonResponse({ ok: false, reason: 'do_response_parse_failed', error: String(e) }, 502);
          }
        } else {
          // No cached data but not waiting: fire-and-forget and return accepted
          if (!cached || !Array.isArray(cached.members) || cached.members.length === 0) {
            stub.fetch('https://globalstats.local/update-members', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({}) })
              .then(async (r) => { try { const t = await r.text().catch(() => null); console.log('[index] background update-members (no-cached)', r.status, (t||'').slice(0,200)); } catch(e){} })
              .catch(err => console.error('[index] bg update-members failed', err));
            return jsonResponse({ ok: true, job: 'accepted', message: 'Members refresh dispatched' }, 202);
          }
          // This branch is unlikely because earlier we returned cached if present; kept for safety.
          return jsonResponse({ fetchedAt: cached.fetchedAt, members: cached.members, memberCount: cached.memberCount, _backgroundRefresh: true });
        }
      } catch (e) {
        console.error('[index] update-members forward error', e);
        // If forwarding to DO failed, still return cached if available; otherwise error.
        if (cached && Array.isArray(cached.members)) {
          const out = { fetchedAt: cached.fetchedAt, members: cached.members, memberCount: cached.memberCount, _backgroundRefresh: false, _refreshError: String(e) };
          return jsonResponse(out);
        }
        return jsonResponse({ ok: false, reason: 'do_forward_failed', error: String(e) }, 500);
      }
    }

    // GET /stats
    // Behavior:
    // - If ?cached=1 present -> return cached snapshot if valid OR partial aggregated snapshot -> DO NOT dispatch background compute here.
    // - If ?fresh=1 present -> return cached/partial immediately (annotated) and fire background compute (non-blocking).
    // - If neither param present -> behave like cached (no dispatch).
    if (request.method === 'GET' && path === '/stats') {
      const wantCached = search.has('cached');
      const wantFresh = search.has('fresh');

      // Helper: read cached snapshot if valid
      const readCachedSnapshot = async () => {
        if (!env.BUNGIE_STATS) return null;
        try {
          const raw = await env.BUNGIE_STATS.get('clears_snapshot');
          if (!raw) return null;
          let parsed = null;
          try { parsed = JSON.parse(raw); } catch (e) { parsed = null; }
          // Validate parsed shape
          if (parsed && (typeof parsed.clears !== 'undefined' || Array.isArray(parsed.perMember))) return parsed;
          // invalid shape stored in clears_snapshot
          console.warn('[index] clears_snapshot exists but does not look like a valid snapshot');
          return null;
        } catch (e) {
          console.warn('[index] KV read clears_snapshot failed', e && e.message ? e.message : e);
          return null;
        }
      };

      // If caller explicitly requested cached-only, return cached or partial and DO NOT dispatch
      if (wantCached && !wantFresh) {
        const cached = await readCachedSnapshot();
        if (cached) return jsonResponse(cached);
        const partial = await buildPartialSnapshotFromMemberKV(env);
        if (partial) return jsonResponse(partial);
        return jsonResponse({ ok: false, reason: 'no_snapshot' }, 404);
      }

      // If caller requested fresh, return cached/partial immediately and trigger background update
      if (wantFresh) {
        const cached = await readCachedSnapshot();
        if (cached) {
          // trigger background update (non-blocking)
          (async () => {
            try {
              const id = env.GLOBAL_STATS.idFromName('global');
              const stub = env.GLOBAL_STATS.get(id);
              await stub.fetch('https://globalstats.local/update-clears', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ includeDeletedCharacters: true }) });
            } catch (e) {}
          })();
          cached._backgroundRefresh = true;
          return jsonResponse(cached);
        }
        // no cached snapshot: try partial
        const partial = await buildPartialSnapshotFromMemberKV(env);
        if (partial) {
          (async () => {
            try {
              const id = env.GLOBAL_STATS.idFromName('global');
              const stub = env.GLOBAL_STATS.get(id);
              await stub.fetch('https://globalstats.local/update-clears', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ includeDeletedCharacters: true }) });
            } catch (e) {}
          })();
          partial._backgroundRefresh = true;
          return jsonResponse(partial);
        }

        // no cached or partial: trigger background update and return accepted
        try {
          const id = env.GLOBAL_STATS.idFromName('global');
          const stub = env.GLOBAL_STATS.get(id);
          stub.fetch('https://globalstats.local/update-clears', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ includeDeletedCharacters: true }) })
            .then(async (r) => { try { const t = await r.text().catch(() => null); console.log('[index] background update-clears', r.status, (t||'').slice(0,200)); } catch(e){} })
            .catch(err => console.error('[index] bg update-clears failed', err));
          return jsonResponse({ ok: true, job: 'accepted', message: 'Clears job dispatched' }, 202);
        } catch (e) {
          console.error('[index] update-clears forward error', e);
          return jsonResponse({ ok: false, reason: 'do_forward_failed', error: String(e) }, 500);
        }
      }

      // Default (no params): behave like cached read (do not dispatch)
      const cached = await readCachedSnapshot();
      if (cached) return jsonResponse(cached);
      const partial = await buildPartialSnapshotFromMemberKV(env);
      if (partial) return jsonResponse(partial);
      return jsonResponse({ ok: false, reason: 'no_snapshot' }, 404);
    }

    // GET /pgcr?instanceId=...
    if (request.method === 'GET' && path === '/pgcr') {
      const instanceId = search.get('instanceId') || null;
      if (!instanceId) return jsonResponse({ ok: false, error: 'missing_instanceId' }, 400);
      try {
        const pgcr = await fetchPgcrAndCache(env, instanceId, Number(env.PGCR_CACHE_TTL_SEC || 86400));
        return jsonResponse({ ok: true, pgcr });
      } catch (e) {
        console.error('[index] /pgcr fetch error', e && e.message ? e.message : e);
        return jsonResponse({ ok: false, error: e && e.message ? e.message : String(e) }, 502);
      }
    }

    // POST /run-update admin - forwards to GlobalStats DO (members or clears)
    if (request.method === 'POST' && path === '/run-update') {
      const token = request.headers.get('x-admin-token') || '';
      if (!env.ADMIN_TOKEN || token !== env.ADMIN_TOKEN) return jsonResponse({ ok: false, reason: 'unauthorized' }, 401);

      let body = null;
      try { body = await request.json().catch(() => null); } catch (e) { body = null; }

      const action = (body && body.action) || 'clears';
      const wait = Boolean(body && (body.wait || (request.headers.get('x-wait') === '1')));
      const opts = (body && body.opts) || {};

      try {
        const id = env.GLOBAL_STATS.idFromName('global');
        const stub = env.GLOBAL_STATS.get(id);
        const doPath = action === 'members' ? '/update-members' : '/update-clears';
        const payload = action === 'members' ? JSON.stringify(opts || {}) : JSON.stringify({ includeDeletedCharacters: true, ...(opts || {}) });

        if (!wait) {
          stub.fetch(`https://globalstats.local${doPath}`, { method: 'POST', headers: { 'Content-Type': 'application/json', 'x-admin-token': token }, body: payload })
            .then(async (r) => { try { const t = await r.text().catch(() => null); console.log('[index] admin bg do', doPath, r.status, (t||'').slice(0,200)); } catch(e){} })
            .catch(err => console.error('[index] admin bg do failed', err));
          return jsonResponse({ ok: true, job: 'accepted', action }, 202);
        } else {
          const res = await stub.fetch(`https://globalstats.local${doPath}`, { method: 'POST', headers: { 'Content-Type': 'application/json', 'x-admin-token': token }, body: payload });
          const text = await res.text().catch(() => null);
          return new Response(text || JSON.stringify({ ok: false }), { status: res.status, headers: { 'Content-Type': 'application/json' }});
        }
      } catch (e) {
        console.error('[index] admin run-update forward failed', e);
        return jsonResponse({ ok: false, reason: 'do_forward_failed', error: String(e) }, 500);
      }
    }

    // GET /clears-snapshot (reader)
    if (request.method === 'GET' && path === '/clears-snapshot') {
      if (!env.BUNGIE_STATS) return jsonResponse({ ok: false, reason: 'no_kv' }, 404);
      try {
        const raw = await env.BUNGIE_STATS.get('clears_snapshot');
        if (!raw) return jsonResponse({ ok: false, reason: 'no_snapshot' }, 404);
        return jsonResponse(JSON.parse(raw));
      } catch (e) {
        console.warn('[index] KV read clears_snapshot failed', e && e.message ? e.message : e);
        return jsonResponse({ ok: false, reason: 'kv_read_failed', error: String(e) }, 500);
      }
    }

    return new Response('Not found', { status: 404 });
  },

  // Cron/scheduled handler: kick off regular member & clears updates (fire-and-forget)
  async scheduled(event, env, ctx) {
    try {
      const id = env.GLOBAL_STATS.idFromName('global');
      const stub = env.GLOBAL_STATS.get(id);
      // Dispatch clears and members refresh in background; includeDeletedCharacters:true default
      stub.fetch('https://globalstats.local/update-clears', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ includeDeletedCharacters: true }) }).catch(() => {});
      // Stagger second call slightly
      setTimeout(() => {
        stub.fetch('https://globalstats.local/update-members', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({}) }).catch(() => {});
      }, 2500);
    } catch (e) {
      console.error('[index] scheduled handler error', e && e.message ? e.message : e);
    }
  }
};