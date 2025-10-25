export { GlobalStats } from './global_stats.js';

/*
  Minimal entry worker that:
  - GET /members  -> fetches clan roster directly from Bungie and returns { fetchedAt, members, memberCount }
  - GET /stats    -> fetches roster directly and returns a minimal snapshot { memberCount, clears:0, updated, source }
  - POST /run-update -> admin-only forward to Durable Object stub to run its /update-members or /update-clears handlers
  - scheduled -> forwards to DO stub for clears & members (DO enforces any rate-limits)
  
  NOTE: KV usage is intentionally commented out. This implementation ALWAYS calls Bungie directly and returns any errors.
*/

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

function mapBungieResultToMember(result) {
  const destiny = result?.destinyUserInfo ?? {};
  const bungie = result?.bungieNetUserInfo ?? {};
  return {
    membershipId: String(destiny.membershipId ?? bungie.membershipId ?? ''),
    membershipType: Number(destiny.membershipType ?? bungie.membershipType ?? 0),
    displayName: destiny.displayName ?? bungie.displayName ?? '',
    supplementalDisplayName: bungie.supplementalDisplayName ?? '',
    isOnline: !!result.isOnline,
    joinDate: result.joinDate ?? null,
    memberType: result.memberType ?? null,
    // raw: result  // include if needed for debugging
  };
}

async function fetchClanRoster(env) {
  const clanId = env.BUNGIE_CLAN_ID;
  const template = env.BUNGIE_CLAN_ROSTER_ENDPOINT;
  if (!clanId || !template) {
    const err = new Error('missing BUNGIE_CLAN_ID or BUNGIE_CLAN_ROSTER_ENDPOINT');
    err.code = 'missing_config';
    throw err;
  }
  const url = template.replace('{clanId}', encodeURIComponent(clanId));
  const headers = { Accept: 'application/json' };
  if (env.BUNGIE_API_KEY) headers['X-API-Key'] = env.BUNGIE_API_KEY;

  const timeoutMs = Number(env.BUNGIE_PER_REQUEST_TIMEOUT_MS || DEFAULT_TIMEOUT);
  const res = await fetchWithTimeout(url, { method: 'GET', headers }, timeoutMs);
  if (!res || !res.ok) {
    const err = new Error(`bungie responded ${res ? res.status : 'no_response'}`);
    err.code = 'bungie_non_ok';
    err.status = res ? res.status : null;
    throw err;
  }
  const payload = await res.json().catch(() => null);
  if (!payload) {
    const err = new Error('invalid_json_from_bungie');
    err.code = 'invalid_json';
    throw err;
  }
  const arr = payload?.Response?.results ?? payload?.results ?? payload?.members ?? null;
  if (!Array.isArray(arr)) {
    const err = new Error('unexpected_payload_shape');
    err.code = 'unexpected_payload_shape';
    err.payload = payload;
    throw err;
  }
  return arr;
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const path = url.pathname;

    // GET /members -> direct Bungie fetch, no KV
    if (request.method === 'GET' && path === '/members') {
      try {
        const raw = await fetchClanRoster(env);
        const members = raw.map(mapBungieResultToMember);
        const fetchedAt = new Date().toISOString();
        return new Response(JSON.stringify({ fetchedAt, members, memberCount: members.length }), { headers: { 'Content-Type': 'application/json' }});
      } catch (e) {
        // Return error to client (no dummy data)
        const body = { ok: false, reason: e.code || 'fetch_failed', error: e.message || String(e) };
        if (e.status) body.status = e.status;
        return new Response(JSON.stringify(body), { status: 502, headers: { 'Content-Type': 'application/json' }});
      }
    }

    // GET /stats -> call Bungie roster to produce a minimal snapshot (clears left as 0)
    if (request.method === 'GET' && (path === '/stats' || path === '/')) {
      try {
        const raw = await fetchClanRoster(env);
        const members = raw.map(mapBungieResultToMember);
        const fetchedAt = new Date().toISOString();
        const snapshot = {
          clears: 0, // Clears computation removed per request (only Bungie fetch for now)
          prophecyClears: 0,
          updated: fetchedAt,
          source: 'bungie'
        };
        return new Response(JSON.stringify(snapshot), { headers: { 'Content-Type': 'application/json' }});
      } catch (e) {
        const body = { ok: false, reason: e.code || 'fetch_failed', error: e.message || String(e) };
        if (e.status) body.status = e.status;
        return new Response(JSON.stringify(body), { status: 502, headers: { 'Content-Type': 'application/json' }});
      }
    }

    // POST /run-update -> admin-only trigger forwarded to DO stub
    if ((request.method === 'POST' || request.method === 'GET') && (path === '/run-update' || path === '/run-update-members' || path === '/run-update-clears')) {
      const token = request.headers.get('x-admin-token') || '';
      if (!env.ADMIN_TOKEN || token !== env.ADMIN_TOKEN) {
        return new Response(JSON.stringify({ ok: false, reason: 'unauthorized' }), { status: 401, headers: { 'Content-Type': 'application/json' }});
      }

      // read optional JSON body
      let body = null;
      if (request.method === 'POST') {
        try { body = await request.json().catch(() => null); } catch (e) { body = null; }
      }

      try {
        const id = env.GLOBAL_STATS.idFromName('global');
        const stub = env.GLOBAL_STATS.get(id);

        // forward based on requested action
        if (body && body.action === 'members') {
          const res = await stub.fetch('https://globalstats.local/update-members', { method: 'POST', headers: { 'x-admin-token': token } });
          const text = await res.text();
          return new Response(text, { status: res.status, headers: { 'Content-Type': 'application/json' }});
        }
        if (body && body.action === 'clears') {
          const res = await stub.fetch('https://globalstats.local/update-clears', { method: 'POST', headers: { 'x-admin-token': token } });
          const text = await res.text();
          return new Response(text, { status: res.status, headers: { 'Content-Type': 'application/json' }});
        }

        // fallback to path-based forward
        if (path === '/run-update-members') {
          const res = await stub.fetch('https://globalstats.local/update-members', { method: 'POST', headers: { 'x-admin-token': token } });
          const text = await res.text();
          return new Response(text, { status: res.status, headers: { 'Content-Type': 'application/json' }});
        }
        if (path === '/run-update-clears') {
          const res = await stub.fetch('https://globalstats.local/update-clears', { method: 'POST', headers: { 'x-admin-token': token } });
          const text = await res.text();
          return new Response(text, { status: res.status, headers: { 'Content-Type': 'application/json' }});
        }

        // default -> update-clears
        const res = await stub.fetch('https://globalstats.local/update-clears', { method: 'POST', headers: { 'x-admin-token': token } });
        const text = await res.text();
        return new Response(text, { status: res.status, headers: { 'Content-Type': 'application/json' }});
      } catch (e) {
        return new Response(JSON.stringify({ ok: false, reason: 'do_forward_failed', error: String(e) }), { status: 500, headers: { 'Content-Type': 'application/json' }});
      }
    }

    return new Response('Not found', { status: 404 });
  },

  // scheduled: call DO to run clears and members (DO may enforce rate-limits)
  async scheduled(event, env, ctx) {
    try {
      const id = env.GLOBAL_STATS.idFromName('global');
      const stub = env.GLOBAL_STATS.get(id);
      await stub.fetch('https://globalstats.local/update-clears', { method: 'POST' }).catch(() => {});
      await stub.fetch('https://globalstats.local/update-members', { method: 'POST' }).catch(() => {});
    } catch (e) {
      console.error('scheduled handler error', e);
    }
  }
};