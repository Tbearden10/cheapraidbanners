// src/index.js
// Export the Durable Object class so Wrangler knows it's provided by this script.
// Re-export from the file that implements it.
export { GlobalStats } from "./global_stats.js";

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const pathname = url.pathname;

    if (pathname === '/stats' && request.method === 'GET') {
      return handleGetStats(request, env);
    }

    if (pathname === '/run-update' && (request.method === 'GET' || request.method === 'POST')) {
      return handleRunUpdate(request, env);
    }

    return new Response('Not found', { status: 404 });
  },

  async scheduled(event, env, ctx) {
    try {
      const id = env.GLOBAL_STATS.idFromName('global');
      const doStub = env.GLOBAL_STATS.get(id);
      // Ask the singleton DO to update (DO enforces rate-limits)
      await doStub.fetch('https://globalstats.local/update', { method: 'POST' });
    } catch (err) {
      console.error('scheduled trigger failed', err);
    }
  }
};

async function handleGetStats(request, env) {
  try {
    const raw = await env.BUNGIE_STATS.get('latest');
    if (!raw) {
      return new Response(JSON.stringify({ clears: 0, updated: null }), { headers: { 'Content-Type': 'application/json' }});
    }
    return new Response(raw, { headers: { 'Content-Type': 'application/json' }});
  } catch (err) {
    console.error('KV read error', err);
    return new Response(JSON.stringify({ clears: 0, updated: null }), { status: 500, headers: { 'Content-Type': 'application/json' }});
  }
}

async function handleRunUpdate(request, env) {
  const token = request.headers.get('x-admin-token') || '';
  if (!env.ADMIN_TOKEN || token !== env.ADMIN_TOKEN) {
    return new Response('Unauthorized', { status: 401 });
  }

  // POST { set: n } -> forward to DO /set; otherwise trigger DO /update
  if (request.method === 'POST') {
    try {
      const body = await request.json().catch(() => null);
      if (body && typeof body.set === 'number') {
        const id = env.GLOBAL_STATS.idFromName('global');
        const doStub = env.GLOBAL_STATS.get(id);
        return await doStub.fetch('https://globalstats.local/set', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ set: body.set })
        });
      }
    } catch (e) {}
  }

  try {
    const id = env.GLOBAL_STATS.idFromName('global');
    const doStub = env.GLOBAL_STATS.get(id);
    const res = await doStub.fetch('https://globalstats.local/update', { method: 'POST' });
    const json = await res.json().catch(() => null);
    return new Response(JSON.stringify(json || { ok: false }), { headers: { 'Content-Type': 'application/json' }});
  } catch (err) {
    console.error('manual update failed', err);
    return new Response(JSON.stringify({ error: 'update_failed' }), { status: 500, headers: { 'Content-Type': 'application/json' }});
  }
}