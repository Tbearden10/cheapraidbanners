// src/index.js
export { GlobalStats } from './global_stats.js';

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const path = url.pathname;

    // GET /stats or GET /
    if ((path === '/stats' || path === '/') && request.method === 'GET') {
      return handleGetStats(request, env);
    }

    // GET /members -> return cached KV members_list or a small dummy list for dev/testing
    if (path === '/members' && request.method === 'GET') {
      return handleGetMembers(request, env);
    }

    // protected admin/manual endpoints to trigger specific updates
    if ((path === '/run-update' || path === '/run-update-clears' || path === '/run-update-members') && (request.method === 'GET' || request.method === 'POST')) {
      return handleRunUpdate(request, env);
    }

    return new Response('Not found', { status: 404 });
  },

  // scheduled() should trigger update-clears (and possibly update-members on a longer interval)
  async scheduled(event, env, ctx) {
    try {
      const id = env.GLOBAL_STATS.idFromName('global');
      const stub = env.GLOBAL_STATS.get(id);
      // default: trigger clears update
      await stub.fetch('https://globalstats.local/update-clears', { method: 'POST' });
    } catch (err) {
      console.error('scheduled trigger failed', err);
    }
  }
};

// Read the unioned snapshot from KV.latest (fast for reads).
// If latest is missing, try to read KV.members_list and synthesize a minimal snapshot.
async function handleGetStats(request, env) {
  try {
    // Try canonical latest snapshot
    if (env.BUNGIE_STATS) {
      const raw = await env.BUNGIE_STATS.get('latest');
      if (raw) {
        return new Response(raw, { headers: { 'Content-Type': 'application/json' }});
      }
    }

    // Fallback: try to derive memberCount from members_list
    try {
      if (env.BUNGIE_STATS) {
        const membersRaw = await env.BUNGIE_STATS.get('members_list');
        if (membersRaw) {
          const parsed = JSON.parse(membersRaw);
          const members = parsed?.members ?? [];
          const fetchedAt = parsed?.fetchedAt ?? null;
          const snapshot = {
            memberCount: Array.isArray(members) ? members.length : 0,
            clears: 0,
            prophecyClears: 0,
            updated: fetchedAt,
            source: 'kv-members-list-fallback'
          };
          return new Response(JSON.stringify(snapshot), { headers: { 'Content-Type': 'application/json' }});
        }
      }
    } catch (e) {
      console.warn('KV.members_list read failed', e);
    }

    // Final fallback: return zeros
    return new Response(JSON.stringify({ memberCount: 0, clears: 0, updated: null }), { headers: { 'Content-Type': 'application/json' }});
  } catch (err) {
    console.error('KV read error', err);
    return new Response(JSON.stringify({ memberCount: 0, clears: 0, updated: null }), { status: 500, headers: { 'Content-Type': 'application/json' }});
  }
}

// GET /members: return the members_list from KV if present; otherwise return a small dummy list for local/dev.
// Also trigger a background members refresh if the list is missing or stale (non-blocking).
// Replace or update the handleGetMembers function in src/index.js with this version.
// It returns { fetchedAt, members, memberCount } when members_list exists or for dummy data.

async function handleGetMembers(request, env) {
  const STALE_MS = Number(env.MEMBERS_STALE_MS || 60 * 60 * 1000); // default 1 hour
  const now = Date.now();

  try {
    if (env.BUNGIE_STATS) {
      const raw = await env.BUNGIE_STATS.get('members_list');
      if (raw) {
        const parsed = JSON.parse(raw);
        const fetchedAt = parsed?.fetchedAt ? parsed.fetchedAt : new Date().toISOString();
        const members = parsed?.members ?? [];

        // If stale (older than STALE_MS), trigger a background refresh but do not wait for it
        if (!parsed?.fetchedAt || (now - Date.parse(parsed.fetchedAt)) > STALE_MS) {
          triggerBackgroundMembersUpdate(env).catch(() => {});
        }

        // Return a shaped object with memberCount included
        const shaped = { fetchedAt, members, memberCount: Array.isArray(members) ? members.length : 0 };
        return new Response(JSON.stringify(shaped), { headers: { 'Content-Type': 'application/json' }});
      }
    }
  } catch (e) {
    console.warn('KV.members_list read failed', e);
    // fall through to return dummy and trigger background update
  }

  // If KV missing or read failed, trigger members update in background and return a dummy list
  triggerBackgroundMembersUpdate(env).catch(() => {});

  // Dummy fallback for dev / initial state, now including memberCount
  const dummy = {
    fetchedAt: new Date().toISOString(),
    members: [
      { displayName: 'PlayerOne', membershipId: '1', membershipType: 1 },
      { displayName: 'PlayerTwo', membershipId: '2', membershipType: 1 },
      { displayName: 'PlayerThree', membershipId: '3', membershipType: 1 }
    ],
    memberCount: 3
  };
  return new Response(JSON.stringify(dummy), { headers: { 'Content-Type': 'application/json' }});
}

function triggerBackgroundMembersUpdate(env) {
  try {
    const id = env.GLOBAL_STATS.idFromName('global');
    const doStub = env.GLOBAL_STATS.get(id);
    // Fire-and-forget: DO has its own rate-limits/internals to avoid over-calling the Bungie roster.
    return doStub.fetch('https://globalstats.local/update-members', { method: 'POST' });
  } catch (e) {
    return Promise.reject(e);
  }
}

async function handleRunUpdate(request, env) {
  // protect manual triggers with x-admin-token
  const token = request.headers.get('x-admin-token') || '';
  if (!env.ADMIN_TOKEN || token !== env.ADMIN_TOKEN) {
    return new Response('Unauthorized', { status: 401 });
  }

  // parse optional JSON body for control signals
  let body = null;
  if (request.method === 'POST') {
    try { body = await request.json().catch(() => null); } catch (e) { body = null; }
  }

  const id = env.GLOBAL_STATS.idFromName('global');
  const doStub = env.GLOBAL_STATS.get(id);

  // explicit member list update
  if (body && body.action === 'members') {
    return doStub.fetch('https://globalstats.local/update-members', { method: 'POST' });
  }

  // explicit clears update
  if (body && body.action === 'clears') {
    return doStub.fetch('https://globalstats.local/update-clears', { method: 'POST' });
  }

  // shorthand endpoints: /run-update-members or /run-update-clears via GET/POST path
  const pathname = new URL(request.url).pathname;
  if (pathname === '/run-update-members') {
    return doStub.fetch('https://globalstats.local/update-members', { method: 'POST' });
  }
  if (pathname === '/run-update-clears') {
    return doStub.fetch('https://globalstats.local/update-clears', { method: 'POST' });
  }

  // default: trigger clears update
  return doStub.fetch('https://globalstats.local/update-clears', { method: 'POST' });
}