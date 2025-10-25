// src/index.js
// Export the DO class so Wrangler detects Durable Objects
export { GlobalStats } from "./global_stats.js";

export default {
  async fetch(request, env) {
    const pathname = new URL(request.url).pathname;

    if (pathname === '/stats' && request.method === 'GET') {
      return handleGetStats(request, env);
    }

    if (pathname === '/run-update' && (request.method === 'GET' || request.method === 'POST')) {
      return handleRunUpdate(request, env);
    }

    return new Response('Not found', { status: 404 });
  },

  // scheduled() runs on the Cloudflare cron and triggers the singleton DO to update
  async scheduled(event, env, ctx) {
    try {
      const id = env.GLOBAL_STATS.idFromName('global');
      const doStub = env.GLOBAL_STATS.get(id);
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

  // Try to parse JSON body (if any)
  let body = null;
  if (request.method === 'POST') {
    try { body = await request.json().catch(() => null); } catch(e) { body = null; }
  }

  // If { set: n } -> absolute set on DO
  if (body && typeof body.set === 'number') {
    const id = env.GLOBAL_STATS.idFromName('global');
    const doStub = env.GLOBAL_STATS.get(id);
    return doStub.fetch('https://globalstats.local/set', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ set: body.set })
    });
  }

  // If { increment: n } or POST with no body -> increment by n (default 1)
  if (request.method === 'POST') {
    const incrementValue = (body && typeof body.increment === 'number') ? Math.floor(body.increment) : 1;
    const id = env.GLOBAL_STATS.idFromName('global');
    const doStub = env.GLOBAL_STATS.get(id);
    const res = await doStub.fetch('https://globalstats.local/incr', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ increment: incrementValue })
    });
    const text = await res.text().catch(() => null);
    return new Response(text || JSON.stringify({ ok: false }), { headers: { 'Content-Type': 'application/json' }});
  }

  // For GET or fallback, call /update on DO (same as the cron)
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