export { GlobalStats } from './global_stats.js';

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const path = url.pathname;

    // GET /stats -> return KV.latest, fallback to derive from members_list, fallback zeros
    if ((path === '/stats' || path === '/') && request.method === 'GET') {
      return handleGetStats(request, env);
    }

    // GET /members -> return KV.members_list (shaped) or a small dummy if missing
    if (path === '/members' && request.method === 'GET') {
      return handleGetMembers(request, env);
    }

    // Admin endpoint to trigger updates via DO: protected by x-admin-token
    if ((path === '/run-update' || path === '/run-update-members' || path === '/run-update-clears') && (request.method === 'POST' || request.method === 'GET')) {
      return handleRunUpdate(request, env);
    }

    return new Response('Not found', { status: 404 });
  },

  // scheduled handler triggers DO update-clears and update-members (DO rate-limits itself)
  async scheduled(event, env, ctx) {
    try {
      const id = env.GLOBAL_STATS.idFromName('global');
      const stub = env.GLOBAL_STATS.get(id);
      // clear stats (frequent)
      await stub.fetch('https://globalstats.local/update-clears', { method: 'POST' }).catch(() => {});
      // members (infrequent) - DO will rate limit via MIN_MEMBERS_UPDATE_INTERVAL_MS
      await stub.fetch('https://globalstats.local/update-members', { method: 'POST' }).catch(() => {});
    } catch (e) {
      console.error('scheduled handler error', e);
    }
  }
};

async function handleGetStats(request, env) {
  // 1) try canonical latest
  try {
    if (env.BUNGIE_STATS) {
      const raw = await env.BUNGIE_STATS.get('latest');
      if (raw) return new Response(raw, { headers: { 'Content-Type': 'application/json' }});
    }
  } catch (e) { /* ignore */ }

  // 2) fallback: synthesize from members_list
  try {
    if (env.BUNGIE_STATS) {
      const raw = await env.BUNGIE_STATS.get('members_list');
      if (raw) {
        const parsed = JSON.parse(raw);
        const members = parsed?.members ?? [];
        const snapshot = { memberCount: members.length, clears: 0, prophecyClears: 0, updated: parsed?.fetchedAt ?? new Date().toISOString(), source: 'kv-fallback' };
        return new Response(JSON.stringify(snapshot), { headers: { 'Content-Type': 'application/json' }});
      }
    }
  } catch (e) { /* ignore */ }

  // 3) final fallback zeros
  return new Response(JSON.stringify({ memberCount: 0, clears: 0, prophecyClears: 0, updated: null }), { headers: { 'Content-Type': 'application/json' }});
}

async function handleGetMembers(request, env) {
  try {
    if (env.BUNGIE_STATS) {
      const raw = await env.BUNGIE_STATS.get('members_list');
      if (raw) {
        const p = JSON.parse(raw);
        return new Response(JSON.stringify({ fetchedAt: p.fetchedAt ?? new Date().toISOString(), members: p.members ?? [], memberCount: Array.isArray(p.members) ? p.members.length : 0 }), { headers: { 'Content-Type': 'application/json' }});
      }
    }
  } catch (e) { /* ignore */ }

  // dummy fallback
  const dummy = { fetchedAt: new Date().toISOString(), members: [{ displayName: 'PlayerOne', membershipId: '1' }], memberCount: 1 };
  return new Response(JSON.stringify(dummy), { headers: { 'Content-Type': 'application/json' }});
}

async function handleRunUpdate(request, env) {
  // admin token guard
  const token = request.headers.get('x-admin-token') || '';
  if (!env.ADMIN_TOKEN || token !== env.ADMIN_TOKEN) return new Response('Unauthorized', { status: 401 });

  // route action via body or path
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