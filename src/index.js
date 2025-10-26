// Entrypoint worker: HTTP endpoints for frontend & admin and scheduled cron.
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
    // Return a compact partial aggregate
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

  const normalized = {
    fetchedAt: new Date().toISOString(),
    instanceId,
    raw: payload,
    period: payload?.Response?.period ?? null,
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

  // cache to KV
  if (env.BUNGIE_STATS) {
    try {
      await env.BUNGIE_STATS.put(cacheKey, JSON.stringify(normalized), { expirationTtl: Number(cacheTtlSec) });
    } catch (e) {
      console.warn('[index] pgcr: KV write failed', e && e.message ? e.message : e);
    }
  }

  return normalized;
}

// read cached members (small helper used by members endpoint)
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

    // GET /members (cached-first, then unconditional background refresh; sync if requested)
    if (request.method === 'GET' && path === '/members') {
      const wantFresh = search.has('fresh');
      const wantCached = search.has('cached') || (!wantFresh && !!env.BUNGIE_STATS);
      const wait = shouldWait(request.url, request);

      const cached = await readCachedMembersFromKV(env);

      try {
        const id = env.GLOBAL_STATS.idFromName('global');
        const stub = env.GLOBAL_STATS.get(id);
        if (!wait && cached && Array.isArray(cached.members)) {
          // background update-members
          stub.fetch('https://globalstats.local/update-members', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({}) })
            .then(async (r) => { try { const t = await r.text().catch(() => null); console.log('[index] background update-members', r.status, (t||'').slice(0,200)); } catch(e){} })
            .catch(err => console.error('[index] bg update-members failed', err));
          const out = { fetchedAt: cached.fetchedAt, members: cached.members, memberCount: cached.memberCount, _backgroundRefresh: true };
          return jsonResponse(out);
        }

        if (wait) {
          const res = await stub.fetch('https://globalstats.local/update-members', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({}) });
          const text = await res.text().catch(() => null);
          try {
            return new Response(text || JSON.stringify({ ok: false }), { status: res.status, headers: { 'Content-Type': 'application/json' }});
          } catch (e) {
            return jsonResponse({ ok: false, reason: 'do_response_parse_failed', error: String(e) }, 502);
          }
        } else {
          if (!cached || !Array.isArray(cached.members) || cached.members.length === 0) {
            stub.fetch('https://globalstats.local/update-members', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({}) })
              .then(async (r) => { try { const t = await r.text().catch(() => null); console.log('[index] background update-members (no-cached)', r.status, (t||'').slice(0,200)); } catch(e){} })
              .catch(err => console.error('[index] bg update-members failed', err));
            return jsonResponse({ ok: true, job: 'accepted', message: 'Members refresh dispatched' }, 202);
          }
          return jsonResponse({ fetchedAt: cached.fetchedAt, members: cached.members, memberCount: cached.memberCount, _backgroundRefresh: true });
        }
      } catch (e) {
        console.error('[index] update-members forward error', e);
        if (cached && Array.isArray(cached.members)) {
          const out = { fetchedAt: cached.fetchedAt, members: cached.members, memberCount: cached.memberCount, _backgroundRefresh: false, _refreshError: String(e) };
          return jsonResponse(out);
        }
        return jsonResponse({ ok: false, reason: 'do_forward_failed', error: String(e) }, 500);
      }
    }

    // GET /stats
    // - cached-only returns cached or partial
    // - fresh with sync (x-wait or sync=1) does: fresh-members (sync) -> if online users then sync update-clears (waitTimeout configurable) -> return snapshot
    // - fresh without wait behaves as before (cached/partial + background update)
    if (request.method === 'GET' && path === '/stats') {
      const wantCached = search.has('cached');
      const wantFresh = search.has('fresh');

      const readCachedSnapshot = async () => {
        if (!env.BUNGIE_STATS) return null;
        try {
          const raw = await env.BUNGIE_STATS.get('clears_snapshot');
          if (!raw) return null;
          let parsed = null;
          try { parsed = JSON.parse(raw); } catch (e) { parsed = null; }
          if (parsed && (typeof parsed.clears !== 'undefined' || Array.isArray(parsed.perMember))) return parsed;
          console.warn('[index] clears_snapshot exists but does not look like a valid snapshot');
          return null;
        } catch (e) {
          console.warn('[index] KV read clears_snapshot failed', e && e.message ? e.message : e);
          return null;
        }
      };

      if (wantCached && !wantFresh) {
        const cached = await readCachedSnapshot();
        if (cached) return jsonResponse(cached);
        const partial = await buildPartialSnapshotFromMemberKV(env);
        if (partial) return jsonResponse(partial);
        return jsonResponse({ ok: false, reason: 'no_snapshot' }, 404);
      }

      if (wantFresh) {
        const cached = await readCachedSnapshot();

        const clientWait = shouldWait(request.url, request);
        const forceSync = (env.FORCE_STATS_FRESH_SYNC && String(env.FORCE_STATS_FRESH_SYNC) === '1');

        // If client requested synchronous fresh stats or server forced it, do two-step:
        // 1) fetch fresh members synchronously (update-members)
        // 2) if any online users, forward synchronous update-clears that will try to produce a fresh snapshot
        if (clientWait || forceSync) {
          const id = env.GLOBAL_STATS.idFromName('global');
          const stub = env.GLOBAL_STATS.get(id);

          // 1) Synchronously refresh members (so the DO knows current roster and we detect online members)
          let freshMembers = null;
          try {
            const memRes = await stub.fetch('https://globalstats.local/update-members', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({}) });
            const text = await memRes.text().catch(() => null);
            try { freshMembers = text ? JSON.parse(text) : null; } catch (e) { freshMembers = null; }
          } catch (e) {
            console.warn('[index] sync update-members failed', e && e.message ? e.message : e);
            // fall through and try to use cached members if available
          }

          // Determine online users from the freshMembers result (or cached)
          let membersList = (freshMembers && Array.isArray(freshMembers.members)) ? freshMembers.members : (cached && Array.isArray(cached.perMember) ? cached.perMember : null);
          // If GlobalStats returned top-level members array (different shapes), normalize
          if (!membersList && freshMembers && Array.isArray(freshMembers)) membersList = freshMembers;
          if (!membersList && cached && Array.isArray(cached.perMember)) {
            // cached.perMember is aggregated items; map to shallow member entries (membershipId only)
            membersList = cached.perMember.map(pm => ({ membershipId: pm.membershipId, membershipType: pm.membershipType || 0, isOnline: false }));
          }

          // Compute online count
          let onlineMembershipIds = [];
          try {
            if (Array.isArray(membersList)) {
              for (const m of membersList) {
                if (m && (m.isOnline || m.is_online || m.online)) {
                  if (m.membershipId) onlineMembershipIds.push(String(m.membershipId));
                  else if (m.membership_id) onlineMembershipIds.push(String(m.membership_id));
                }
              }
            }
          } catch (e) { onlineMembershipIds = []; }

          // If any online users, forward synchronous update-clears and wait for the aggregated snapshot
          if (onlineMembershipIds.length > 0) {
            // respect configured caps
            const waitMs = Number(env.STATS_SYNC_WAIT_MS ? Number(env.STATS_SYNC_WAIT_MS) : 120000);
            const maxUsers = Number(env.STATS_MAX_SYNC_USER_LIMIT ? Number(env.STATS_MAX_SYNC_USER_LIMIT) : 200);
            const userLimit = Math.min(maxUsers, onlineMembershipIds.length || maxUsers);

            // We'll request update-clears with a userLimit equal to online count OR let GlobalStats handle the dispatch.
            // NOTE: GlobalStats currently slices by member order; if you want it to process particular membershipIds,
            // we should extend GlobalStats to accept membershipIds. For now we use userLimit to prefer faster runs.
            try {
              const payload = JSON.stringify({ userLimit: userLimit || undefined, waitForCompletion: true, waitTimeoutMs: waitMs, opts: { includeDeletedCharacters: true } });
              const res = await stub.fetch('https://globalstats.local/update-clears', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: payload });
              const text = await res.text().catch(() => null);
              try {
                return new Response(text || JSON.stringify({ ok: false }), { status: res.status, headers: { 'Content-Type': 'application/json' }});
              } catch (e) {
                return jsonResponse({ ok: false, reason: 'do_response_parse_failed', error: String(e) }, 502);
              }
            } catch (e) {
              console.error('[index] sync update-clears forward error', e && e.message ? e.message : e);
              const partial = await buildPartialSnapshotFromMemberKV(env);
              if (partial) {
                partial._backgroundRefresh = true;
                partial._refreshError = String(e);
                return jsonResponse(partial);
              }
              return jsonResponse({ ok: false, reason: 'do_forward_failed', error: String(e) }, 500);
            }
          }

          // No online users detected — return cached or partial (but we already refreshed members above)
          if (cached) {
            cached._backgroundRefresh = false;
            return jsonResponse(cached);
          }
          const partial = await buildPartialSnapshotFromMemberKV(env);
          if (partial) return jsonResponse(partial);
          return jsonResponse({ ok: true, message: 'no_online_members' }, 200);
        }

        // Non-waiting path (existing behavior): return cached/partial immediately and trigger background update
        if (cached) {
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
      const cached2 = await readCachedSnapshot();
      if (cached2) return jsonResponse(cached2);
      const partial2 = await buildPartialSnapshotFromMemberKV(env);
      if (partial2) return jsonResponse(partial2);
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