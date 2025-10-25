export { GlobalStats } from './global_stats.js';

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const path = url.pathname;

    // GET /stats -> synthesize from clears_snapshot + members_list (BUT do NOT include memberCount)
    if ((path === '/stats' || path === '/') && request.method === 'GET') {
      return handleGetStats(request, env);
    }

    // GET /members -> return KV.members_list shaped { fetchedAt, members, memberCount } (NO DO call)
    if (path === '/members' && request.method === 'GET') {
      return handleGetMembers(request, env);
    }

    // POST /run-update -> admin-protected trigger for DO (body.action = 'members'|'clears')
    if ((path === '/run-update' || path === '/run-update-members' || path === '/run-update-clears') && (request.method === 'POST' || request.method === 'GET')) {
      return handleRunUpdate(request, env);
    }

    return new Response('Not found', { status: 404 });
  },

  // scheduled: trigger both clears and members. DO will rate-limit actual members fetches using MIN_MEMBERS_UPDATE_INTERVAL_MS.
  async scheduled(event, env, ctx) {
    try {
      const id = env.GLOBAL_STATS.idFromName('global');
      const stub = env.GLOBAL_STATS.get(id);

      // Trigger clears (expected to run often)
      await stub.fetch('https://globalstats.local/update-clears', { method: 'POST' }).catch((e) => console.error('scheduled update-clears error', e));

      // Trigger members (infrequent; DO enforces MIN_MEMBERS_UPDATE_INTERVAL_MS)
      await stub.fetch('https://globalstats.local/update-members', { method: 'POST' }).catch((e) => console.error('scheduled update-members error', e));
    } catch (e) {
      console.error('scheduled handler error', e);
    }
  }
};

// GET /stats: read clears_snapshot + members_list and synthesize response
// NOTE: memberCount is intentionally NOT included here; clients should use GET /members for member count.
async function handleGetStats(request, env) {
  let clears = null;
  let membersList = null;

  try {
    if (env.BUNGIE_STATS) {
      const rawClears = await env.BUNGIE_STATS.get('clears_snapshot');
      if (rawClears) clears = JSON.parse(rawClears);
    }
  } catch (e) { clears = null; }

  try {
    if (env.BUNGIE_STATS) {
      const rawMembers = await env.BUNGIE_STATS.get('members_list');
      if (rawMembers) membersList = JSON.parse(rawMembers);
    }
  } catch (e) { membersList = null; }

  // Note: memberCount intentionally omitted from stats response.
  const snapshot = {
    clears: (clears && typeof clears.clears === 'number') ? clears.clears : 0,
    prophecyClears: (clears && typeof clears.prophecyClears === 'number') ? clears.prophecyClears : 0,
    // prefer clears_snapshot.fetchedAt, otherwise members_list.fetchedAt
    updated: (clears && clears.fetchedAt) ? clears.fetchedAt : (membersList && membersList.fetchedAt) ? membersList.fetchedAt : null,
    source: (clears ? 'clears_snapshot' : (membersList ? 'members_list_fallback' : 'empty'))
  };

  return new Response(JSON.stringify(snapshot), { headers: { 'Content-Type': 'application/json' }});
}

// GET /members: return KV.members_list only, shaped with memberCount (no DO call)
async function handleGetMembers(request, env) {
  try {
    if (env.BUNGIE_STATS) {
      const raw = await env.BUNGIE_STATS.get('members_list');
      if (raw) {
        const parsed = JSON.parse(raw);
        return new Response(JSON.stringify({
          fetchedAt: parsed?.fetchedAt ?? new Date().toISOString(),
          members: parsed?.members ?? [],
          memberCount: Array.isArray(parsed?.members) ? parsed.members.length : 0
        }), { headers: { 'Content-Type': 'application/json' }});
      }
    }
  } catch (e) { /* ignore */ }

  // fallback (only used if KV is empty)
  const dummy = { fetchedAt: new Date().toISOString(), members: [{ displayName: 'PlayerOne', membershipId: '1' }], memberCount: 1 };
  return new Response(JSON.stringify(dummy), { headers: { 'Content-Type': 'application/json' }});
}

// POST/GET /run-update : admin-protected manual triggers
async function handleRunUpdate(request, env) {
  const token = request.headers.get('x-admin-token') || '';
  if (!env.ADMIN_TOKEN || token !== env.ADMIN_TOKEN) {
    return new Response('Unauthorized', { status: 401 });
  }

  let body = null;
  if (request.method === 'POST') {
    try { body = await request.json().catch(() => null); } catch (e) { body = null; }
  }

  const id = env.GLOBAL_STATS.idFromName('global');
  const stub = env.GLOBAL_STATS.get(id);

  if (body && body.action === 'members') return stub.fetch('https://globalstats.local/update-members', { method: 'POST' });
  if (body && body.action === 'clears') return stub.fetch('https://globalstats.local/update-clears', { method: 'POST' });

  const path = new URL(request.url).pathname;
  if (path === '/run-update-members') return stub.fetch('https://globalstats.local/update-members', { method: 'POST' });
  if (path === '/run-update-clears') return stub.fetch('https://globalstats.local/update-clears', { method: 'POST' });

  // default -> clears
  return stub.fetch('https://globalstats.local/update-clears', { method: 'POST' });
}