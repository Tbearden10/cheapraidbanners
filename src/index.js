export { GlobalStats } from './global_stats.js';

// Very small router:
// - GET /members -> read BUNGIE_STATS KV members_list and return it (or fallback empty array)
// - POST /run-update -> admin-only forward to DO stub to run update (returns DO response)

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const path = url.pathname;

    if (request.method === 'GET' && path === '/members') {
      // read KV directly and return JSON
      try {
        if (env.BUNGIE_STATS) {
          const raw = await env.BUNGIE_STATS.get('members_list');
          if (raw) {
            const parsed = JSON.parse(raw);
            const members = parsed?.members ?? [];
            return new Response(JSON.stringify({ fetchedAt: parsed?.fetchedAt ?? null, members, memberCount: members.length }), { headers: { 'Content-Type': 'application/json' }});
          }
        }
      } catch (e) {
        // ignore parse errors and fallthrough to empty
      }
      // fallback empty
      return new Response(JSON.stringify({ fetchedAt: null, members: [], memberCount: 0 }), { headers: { 'Content-Type': 'application/json' }});
    }

    // admin trigger: POST /run-update  (requires header x-admin-token)
    if ((request.method === 'POST' || request.method === 'GET') && path === '/run-update') {
      const token = request.headers.get('x-admin-token') || '';
      if (!env.ADMIN_TOKEN || token !== env.ADMIN_TOKEN) return new Response('Unauthorized', { status: 401 });

      // forward to DO
      try {
        const id = env.GLOBAL_STATS.idFromName('global');
        const stub = env.GLOBAL_STATS.get(id);
        // call DO update-members (force by including admin header)
        const res = await stub.fetch('https://globalstats.local/update-members', { method: 'POST', headers: { 'x-admin-token': token } });
        const text = await res.text();
        return new Response(text, { status: res.status, headers: { 'Content-Type': 'application/json' }});
      } catch (e) {
        return new Response(JSON.stringify({ ok: false, reason: 'do_error', error: String(e) }), { status: 500, headers: { 'Content-Type': 'application/json' }});
      }
    }

    return new Response('Not found', { status: 404 });
  }
};