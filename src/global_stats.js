// src/global_stats.js
// Durable Object: GlobalStats (single authoritative instance)
// Test stub: _fetchBungieCount returns 5 for now.

export class GlobalStats {
  constructor(state, env) {
    this.state = state;
    this.env = env;
  }

  async _ensure() {
    const d = await this.state.storage.get('data');
    if (typeof d === 'undefined') {
      await this.state.storage.put('data', { clears: 0, updated: null, source: 'init' });
    }
  }

  async _atomicWrite(newObj) {
    return this.state.storage.transaction(async (tx) => {
      await tx.put('data', newObj);
      return newObj;
    });
  }

  async fetch(request) {
    await this._ensure();
    const url = new URL(request.url);
    const pathname = url.pathname.replace(/^\//, '') || '';

    if (request.method === 'GET' && (pathname === '' || pathname === '/')) {
      const current = await this.state.storage.get('data');
      return new Response(JSON.stringify(current), { headers: { 'Content-Type': 'application/json' }});
    }

    if (request.method === 'POST' && pathname === 'update') {
      const last = (await this.state.storage.get('lastUpdateAt')) || 0;
      const now = Date.now();
      const minIntervalMs = 20 * 1000;
      if (now - last < minIntervalMs) {
        const current = await this.state.storage.get('data');
        return new Response(JSON.stringify({ ok: false, reason: 'rate_limited', current }), { headers: { 'Content-Type': 'application/json' }});
      }

      await this.state.storage.put('lastUpdateAt', now);

      // TEST STUB: return 5 for now
      const count = 5;

      if (typeof count === 'number') {
        const newObj = { clears: Math.max(0, Math.floor(count)), updated: new Date().toISOString(), source: 'bungie-stub' };
        await this._atomicWrite(newObj);

        // Best-effort KV backup
        try {
          if (this.env.BUNGIE_STATS) {
            await this.env.BUNGIE_STATS.put('latest', JSON.stringify(newObj));
          }
        } catch (e) {}

        return new Response(JSON.stringify({ ok: true, updated: newObj }), { headers: { 'Content-Type': 'application/json' }});
      }

      const current = await this.state.storage.get('data');
      return new Response(JSON.stringify({ ok: false, reason: 'fetch_failed', current }), { headers: { 'Content-Type': 'application/json' }});
    }

    if (request.method === 'POST' && pathname === 'set') {
      let body = null;
      try { body = await request.json().catch(() => null); } catch(e){ body = null; }
      if (body && typeof body.set === 'number') {
        const newObj = { clears: Math.max(0, Math.floor(body.set)), updated: new Date().toISOString(), source: 'manual' };
        await this._atomicWrite(newObj);
        try { if (this.env.BUNGIE_STATS) await this.env.BUNGIE_STATS.put('latest', JSON.stringify(newObj)); } catch(e){}
        return new Response(JSON.stringify({ ok: true, updated: newObj }), { headers: { 'Content-Type': 'application/json' }});
      }
      return new Response(JSON.stringify({ ok: false, reason: 'invalid_payload' }), { status: 400, headers: { 'Content-Type': 'application/json' }});
    }

    return new Response('Not found', { status: 404 });
  }
}