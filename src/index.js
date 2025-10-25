// src/index.js
// Scheduled Worker: cron runs performUpdate(), which writes the latest JSON to KV.
// Exposes GET /stats to return the latest KV value.
// Replace dummyBungieFetch() with your Bungie API calls (use env.BUNGIE_API_KEY secret).

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const pathname = url.pathname;

    if (pathname === '/stats' && request.method === 'GET') {
      return handleGetStats(request, env);
    }

    // Optional admin endpoint to trigger an immediate update (protected by ADMIN_TOKEN)
    if (pathname === '/run-update' && (request.method === 'GET' || request.method === 'POST')) {
      return handleRunUpdate(request, env);
    }

    return new Response('Not found', { status: 404 });
  },

  async scheduled(event, env, ctx) {
    try {
      await performUpdate(env);
    } catch (err) {
      console.error('Scheduled update failed', err);
    }
  }
};

async function handleGetStats(request, env) {
  try {
    const raw = await env.BUNGIE_STATS.get('latest');
    if (!raw) {
      return new Response(JSON.stringify({ clears: 0, updated: null }), {
        headers: { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' }
      });
    }
    return new Response(raw, {
      headers: { 'Content-Type': 'application/json', 'Cache-Control': 'public, max-age=10, s-maxage=10' }
    });
  } catch (err) {
    console.error('KV read error', err);
    return new Response(JSON.stringify({ error: 'kv_error' }), { status: 500, headers: { 'Content-Type': 'application/json' }});
  }
}

async function handleRunUpdate(request, env) {
  const token = request.headers.get('x-admin-token') || '';
  if (!env.ADMIN_TOKEN || token !== env.ADMIN_TOKEN) {
    return new Response('Unauthorized', { status: 401 });
  }
  try {
    const result = await performUpdate(env);
    return new Response(JSON.stringify(result), { headers: { 'Content-Type': 'application/json' }});
  } catch (err) {
    console.error('Manual update failed', err);
    return new Response(JSON.stringify({ error: 'update_failed' }), { status: 500, headers: { 'Content-Type': 'application/json' }});
  }
}

async function performUpdate(env) {
  // Replace with your Bungie API logic. For now we write a dummy payload:
  const data = await dummyBungieFetch();

  const payload = {
    ...data,
    updated: new Date().toISOString()
  };

  await env.BUNGIE_STATS.put('latest', JSON.stringify(payload));
  return payload;
}

async function dummyBungieFetch() {
  // deterministic test payload — you will replace this with real calls
  return { clears: 5 };
}