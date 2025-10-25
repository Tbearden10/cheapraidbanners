// src/index.js
// Scheduled Worker: background cron calls call performUpdate() which writes latest JSON into KV.
// Exposes GET /stats to return the latest KV value.
// Replace dummyBungieFetch() with your real Bungie code; put secret as BUNGIE_API_KEY.

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const pathname = url.pathname;

    if (pathname === '/stats' && request.method === 'GET') {
      return handleGetStats(request, env);
    }

    // optional admin trigger if you want on-demand runs (comment out if not needed)
    if (pathname === '/run-update') {
      return handleRunUpdate(request, env);
    }

    return new Response('Not found', { status: 404 });
  },

  // Cron handler invoked according to wrangler.toml [triggers].crons
  async scheduled(event, env, ctx) {
    try {
      await performUpdate(env);
    } catch (err) {
      // cloudflare logs will show scheduled errors
      console.error('Scheduled update failed', err);
    }
  }
};

async function handleGetStats(request, env) {
  try {
    const raw = await env.BUNGIE_STATS.get('latest');
    if (!raw) {
      const defaultPayload = { clears: 0, updated: null };
      return new Response(JSON.stringify(defaultPayload), {
        headers: { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' }
      });
    }
    return new Response(raw, {
      headers: { 'Content-Type': 'application/json', 'Cache-Control': 'public, max-age=10, s-maxage=10' }
    });
  } catch (err) {
    console.error('KV read error', err);
    return new Response(JSON.stringify({ error: 'kv_error' }), { status: 500, headers: { 'Content-Type': 'application/json' } });
  }
}

async function handleRunUpdate(request, env) {
  const token = request.headers.get('x-admin-token') || '';
  if (!env.ADMIN_TOKEN || token !== env.ADMIN_TOKEN) {
    return new Response('Unauthorized', { status: 401 });
  }
  try {
    const result = await performUpdate(env);
    return new Response(JSON.stringify(result), { headers: { 'Content-Type': 'application/json' } });
  } catch (err) {
    console.error('Manual update failed', err);
    return new Response(JSON.stringify({ error: 'update_failed' }), { status: 500, headers: { 'Content-Type': 'application/json' } });
  }
}

// performUpdate: call Bungie (replace dummy for your implementation) and put result to KV
async function performUpdate(env) {
  // Replace the next line with your real Bungie logic using env.BUNGIE_API_KEY
  const data = await dummyBungieFetch();

  const payload = {
    ...data,
    updated: new Date().toISOString()
  };

  await env.BUNGIE_STATS.put('latest', JSON.stringify(payload));
  return payload;
}

// Dummy function for testing — returns static clears = 5
async function dummyBungieFetch() {
  return { clears: 5 };
}