// Entrypoint (simplified): cached reads + single "fresh members" flow that diffs KV and triggers clears jobs.
// Changes: members?fresh=1 now returns the fresh roster (synchronous). stats?fresh=1 only starts the job and returns 202 immediately.
//
// Exports DO bindings
export { GlobalStats } from './global_stats.js';
export { MemberWorker } from './member_worker.js';

function jsonResponse(obj, status = 200) {
  return new Response(JSON.stringify(obj), { status, headers: { 'Content-Type': 'application/json' }});
}

function shouldWait(url, request) {
  try {
    const u = new URL(url);
    if (u.searchParams.get('sync') === '1') return true;
  } catch (e) {}
  const h = request.headers.get('x-wait');
  return !!(h && (h === '1' || h.toLowerCase() === 'true'));
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
        members.push(JSON.parse(raw));
      } catch (e) { /* skip malformed */ }
    }
    return { members, memberCount: members.length, fetchedAt: new Date().toISOString() };
  } catch (e) {
    console.warn('[index] readCachedMembersFromKV failed', e && e.message ? e.message : e);
    return null;
  }
}

async function writeMemberKVChanges(env, freshMembers) {
  if (!env.BUNGIE_STATS) return { added: 0, updated: 0, removed: 0 };

  const freshMap = new Map();
  for (const m of (freshMembers || [])) freshMap.set(String(m.membershipId), m);

  const listed = await env.BUNGIE_STATS.list({ prefix: 'member:' });
  const existingKeys = (listed && listed.keys) ? listed.keys.map(k => k.name) : [];
  const existingMap = new Map();
  for (const key of existingKeys) {
    const id = key.replace(/^member:/, '');
    try {
      const raw = await env.BUNGIE_STATS.get(key);
      if (!raw) continue;
      existingMap.set(id, JSON.parse(raw));
    } catch (e) { /* ignore */ }
  }

  const added = [];
  const updated = [];
  const removed = [];

  for (const [id, m] of freshMap.entries()) {
    const small = {
      membershipId: String(m.membershipId),
      membershipType: Number(m.membershipType || m.membership_type || 0),
      supplementalDisplayName: m.supplementalDisplayName ?? m.displayName ?? m.display_name ?? '',
      isOnline: !!(m.isOnline ?? m.online ?? m.is_online),
      joinDate: m.joinDate ?? m.join_date ?? null,
      memberType: m.memberType ?? m.member_type ?? null,
      emblemPath: m.emblemPath ?? m.emblemBackgroundPath ?? m.emblem_url ?? null,
      emblemBackgroundPath: m.emblemBackgroundPath ?? null,
      emblemHash: m.emblemHash ?? null
    };

    const prev = existingMap.get(id);
    const keyName = `member:${id}`;
    if (!prev) {
      try {
        await env.BUNGIE_STATS.put(keyName, JSON.stringify(small));
        added.push(id);
      } catch (e) {
        console.warn('[index] failed writing new member KV', keyName, e && e.message ? e.message : e);
      }
    } else {
      const changed = (
        prev.supplementalDisplayName !== small.supplementalDisplayName ||
        prev.emblemPath !== small.emblemPath ||
        prev.joinDate !== small.joinDate ||
        prev.isOnline !== small.isOnline
      );
      if (changed) {
        try {
          await env.BUNGIE_STATS.put(keyName, JSON.stringify(small));
          updated.push(id);
        } catch (e) {
          console.warn('[index] failed updating member KV', keyName, e && e.message ? e.message : e);
        }
      }
    }
  }

  for (const id of existingMap.keys()) {
    if (!freshMap.has(id)) {
      const keyName = `member:${id}`;
      try {
        await env.BUNGIE_STATS.delete(keyName);
        removed.push(id);
      } catch (e) {
        console.warn('[index] failed deleting stale member key', keyName, e && e.message ? e.message : e);
      }
    }
  }

  return { added: added.length, updated: updated.length, removed: removed.length, addedIds: added, updatedIds: updated, removedIds: removed };
}

async function readCachedSnapshot(env) {
  if (!env.BUNGIE_STATS) return null;
  try {
    const raw = await env.BUNGIE_STATS.get('clears_snapshot');
    if (!raw) return null;
    return JSON.parse(raw);
  } catch (e) {
    console.warn('[index] readCachedSnapshot failed', e && e.message ? e.message : e);
    return null;
  }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const path = url.pathname;
    const search = url.searchParams;

    // GET /members
    if (request.method === 'GET' && path === '/members') {
      const wantFresh = search.has('fresh');
      const wantCached = search.has('cached') || (!wantFresh && !!env.BUNGIE_STATS);

      if (wantCached && !wantFresh) {
        const cached = await readCachedMembersFromKV(env);
        if (cached) return jsonResponse(cached);
        return jsonResponse({ ok: false, reason: 'no_cached_members' }, 404);
      }

      // If client requested fresh: return fresh roster (synchronously)
      if (wantFresh) {
        const id = env.GLOBAL_STATS.idFromName('global');
        const stub = env.GLOBAL_STATS.get(id);

        // 1) ask DO for fresh roster (synchronous) and return it
        let freshMembers = [];
        try {
          const res = await stub.fetch('https://globalstats.local/update-members', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({}) });
          const text = await res.text().catch(() => null);
          let parsed = null;
          try { parsed = text ? JSON.parse(text) : null; } catch (e) { parsed = null; }
          if (parsed && Array.isArray(parsed.members)) freshMembers = parsed.members;
          else if (Array.isArray(parsed)) freshMembers = parsed;
          else freshMembers = Array.isArray(parsed) ? parsed : (parsed && parsed.members ? parsed.members : []);
        } catch (e) {
          console.warn('[index] update-members DO call failed', e && e.message ? e.message : e);
          // fall back to cached if available
          const cached = await readCachedMembersFromKV(env);
          if (cached) return jsonResponse({ ok: true, members: cached.members, note: 'fresh_failed_returned_cached' });
          return jsonResponse({ ok: false, reason: 'members_fetch_failed', error: String(e) }, 502);
        }

        // 2) diff vs KV and write changes
        const diffResult = await writeMemberKVChanges(env, freshMembers);

        // 3) trigger clears processing for online members in background (do not wait)
        const onlineMembers = (Array.isArray(freshMembers) ? freshMembers.filter(m => !!(m.isOnline ?? m.is_online ?? m.online)) : []).map(m => ({
          membershipId: String(m.membershipId ?? m.membership_id ?? ''),
          membershipType: Number(m.membershipType ?? m.membership_type ?? 0)
        })).filter(m => m.membershipId);

        const maxUsers = Number(env.STATS_MAX_SYNC_USER_LIMIT ? Number(env.STATS_MAX_SYNC_USER_LIMIT) : 200);
        const toProcess = onlineMembers.slice(0, maxUsers);

        try {
          if (toProcess.length > 0) {
            const payloadObj = { membershipIds: toProcess, waitForCompletion: false, opts: { includeDeletedCharacters: true } };
            // fire-and-forget background clears job; don't await
            stub.fetch('https://globalstats.local/update-clears', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payloadObj) }).catch((e) => {
              console.warn('[index] background update-clears dispatch failed', e && e.message ? e.message : e);
            });
          }
        } catch (e) {
          console.warn('[index] failed dispatching background clears job', e && e.message ? e.message : e);
        }

        // 4) return fresh members and diff result immediately
        return jsonResponse({ ok: true, members: freshMembers, diff: diffResult, clearsDispatchedFor: toProcess.length });
      }

      // default cached path
      const cached = await readCachedMembersFromKV(env);
      if (cached) return jsonResponse(cached);
      return jsonResponse({ ok: false, reason: 'no_members' }, 404);
    }

    // GET /stats
    if (request.method === 'GET' && path === '/stats') {
      const wantCached = search.has('cached');
      const wantFresh = search.has('fresh');

      if (wantCached && !wantFresh) {
        const snap = await readCachedSnapshot(env);
        if (snap) return jsonResponse(snap);
        return jsonResponse({ ok: false, reason: 'no_snapshot' }, 404);
      }

      // For stats?fresh=1: only start the job and return 202 immediately (do not wait)
      if (wantFresh) {
        const id = env.GLOBAL_STATS.idFromName('global');
        const stub = env.GLOBAL_STATS.get(id);

        try {
          const payloadObj = { opts: { includeDeletedCharacters: true } };
          // Always fire-and-forget; return accepted immediately.
          stub.fetch('https://globalstats.local/update-clears', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payloadObj) }).catch((e) => {
            console.warn('[index] background update-clears dispatch failed', e && e.message ? e.message : e);
          });
          return jsonResponse({ ok: true, job: 'accepted', message: 'Clears job dispatched' }, 202);
        } catch (e) {
          console.warn('[index] update-clears dispatch error', e && e.message ? e.message : e);
          // If dispatch itself failed, still return accepted (best-effort) but include error
          return jsonResponse({ ok: false, reason: 'dispatch_failed', error: String(e) }, 500);
        }
      }

      // default
      const snap = await readCachedSnapshot(env);
      if (snap) return jsonResponse(snap);
      return jsonResponse({ ok: false, reason: 'no_snapshot' }, 404);
    }

    // GET /pgcr unchanged
    if (request.method === 'GET' && path === '/pgcr') {
      const instanceId = url.searchParams.get('instanceId') || null;
      if (!instanceId) return jsonResponse({ ok: false, error: 'missing_instanceId' }, 400);
      try {
        const apiKey = env.BUNGIE_API_KEY || '';
        const pgcrUrl = `https://www.bungie.net/Platform/Destiny2/Stats/PostGameCarnageReport/${encodeURIComponent(instanceId)}/`;
        const headers = { Accept: 'application/json' };
        if (apiKey) headers['X-API-Key'] = apiKey;
        const res = await fetch(pgcrUrl, { method: 'GET', headers });
        if (!res || !res.ok) {
          const text = await res.text().catch(()=>null);
          return jsonResponse({ ok: false, error: `bungie ${res ? res.status : 'no_resp'} ${text ? text.slice(0,200) : ''}` }, 502);
        }
        const payload = await res.json().catch(()=>null);
        if (!payload) return jsonResponse({ ok: false, error: 'invalid_pgcr' }, 502);
        const normalized = {
          fetchedAt: new Date().toISOString(),
          instanceId,
          raw: payload,
          period: payload?.Response?.period ?? null,
          activityName: payload?.Response?.activityDetails?.name ?? null,
          mode: payload?.Response?.activityDetails?.mode ?? null,
          entries: Array.isArray(payload?.Response?.entries) ? payload.Response.entries.map(e => ({ characterId: e.characterId ?? e.character_id ?? null, membershipId: e.player?.destinyUserInfo?.membershipId ?? null, displayName: e.player?.destinyUserInfo?.displayName ?? null, values: e.values ?? null })) : []
        };
        if (env.BUNGIE_STATS) {
          try { await env.BUNGIE_STATS.put(`pgcr:${instanceId}`, JSON.stringify(normalized), { expirationTtl: Number(env.PGCR_CACHE_TTL_SEC || 86400) }); } catch (e) {}
        }
        return jsonResponse({ ok: true, pgcr: normalized });
      } catch (e) {
        return jsonResponse({ ok: false, error: String(e) }, 502);
      }
    }

    // POST /run-update admin (unchanged)
    if (request.method === 'POST' && path === '/run-update') {
      const token = request.headers.get('x-admin-token') || '';
      if (!env.ADMIN_TOKEN || token !== env.ADMIN_TOKEN) return jsonResponse({ ok: false, reason: 'unauthorized' }, 401);
      let body = null;
      try { body = await request.json().catch(()=>null); } catch (e) { body = null; }
      const action = (body && body.action) || 'clears';
      const wait = Boolean(body && body.wait);
      try {
        const id = env.GLOBAL_STATS.idFromName('global');
        const stub = env.GLOBAL_STATS.get(id);
        const doPath = action === 'members' ? '/update-members' : '/update-clears';
        const payload = action === 'members' ? JSON.stringify(body.opts || {}) : JSON.stringify({ ...(body.opts || {}), includeDeletedCharacters: true });
        if (wait) {
          const res = await stub.fetch(`https://globalstats.local${doPath}`, { method: 'POST', headers: { 'Content-Type': 'application/json', 'x-admin-token': token }, body: payload });
          const text = await res.text().catch(()=>null);
          try { return new Response(text || JSON.stringify({ ok: false }), { status: res.status, headers: { 'Content-Type': 'application/json' }}); } catch (e) { return jsonResponse({ ok: false, error: String(e) }, 502); }
        } else {
          stub.fetch(`https://globalstats.local${doPath}`, { method: 'POST', headers: { 'Content-Type': 'application/json', 'x-admin-token': token }, body: payload }).catch(()=>{});
          return jsonResponse({ ok: true, job: 'accepted', action }, 202);
        }
      } catch (e) {
        return jsonResponse({ ok: false, reason: 'do_forward_failed', error: String(e) }, 500);
      }
    }

    return new Response('Not found', { status: 404 });
  }
};