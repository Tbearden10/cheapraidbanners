// src/global_stats.js
// Durable Object: GlobalStats
// Single authoritative updater: idFromName('global') will give exactly one instance.

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

    // GET / -> return current stats
    if (request.method === 'GET' && (pathname === '' || pathname === '/')) {
      const current = await this.state.storage.get('data');
      return new Response(JSON.stringify(current), { headers: { 'Content-Type': 'application/json' }});
    }

    // POST /update -> authoritative fetch and store (this is what scheduled() triggers)
    if (request.method === 'POST' && pathname === 'update') {
      const last = (await this.state.storage.get('lastUpdateAt')) || 0;
      const now = Date.now();
      const minIntervalMs = 20 * 1000; // guard against too-frequent updates
      if (now - last < minIntervalMs) {
        const current = await this.state.storage.get('data');
        return new Response(JSON.stringify({ ok: false, reason: 'rate_limited', current }), { headers: { 'Content-Type': 'application/json' }});
      }

      await this.state.storage.put('lastUpdateAt', now);

      // TEST STUB: return 5 for now (replace this with real Bungie call later)
      const count = await this._fetchBungieCount();

      if (typeof count === 'number') {
        const newObj = { clears: Math.max(0, Math.floor(count)), updated: new Date().toISOString(), source: 'bungie-stub' };
        await this._atomicWrite(newObj);

        // Best-effort backup to KV
        try {
          if (this.env.BUNGIE_STATS) {
            await this.env.BUNGIE_STATS.put('latest', JSON.stringify(newObj));
          }
        } catch (e) {
          // non-fatal
          console.warn('KV backup failed', e);
        }

        return new Response(JSON.stringify({ ok: true, updated: newObj }), { headers: { 'Content-Type': 'application/json' }});
      }

      const current = await this.state.storage.get('data');
      return new Response(JSON.stringify({ ok: false, reason: 'fetch_failed', current }), { headers: { 'Content-Type': 'application/json' }});
    }

    // POST /set -> set absolute value (admin usage, only via admin endpoint in Worker)
    if (request.method === 'POST' && pathname === 'set') {
      let body = null;
      try { body = await request.json().catch(() => null); } catch(e) { body = null; }
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

  // TEST STUB: for now return 5. Replace with real fetch/parse later.
  async _fetchBungieCount() {
    return 5;
  }
}