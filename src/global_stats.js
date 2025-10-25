// src/global_stats.js
export class GlobalStats {
  constructor(state, env) {
    this.state = state;
    this.env = env;
  }

  async _ensure() {
    const d = await this.state.storage.get('data');
    if (typeof d === 'undefined') {
      const init = { clears: 0, updated: null, source: 'init' };
      await this.state.storage.put('data', init);
    }
  }

  async _atomicWrite(newObj) {
    return this.state.storage.transaction(async (tx) => {
      await tx.put('data', newObj);
      return newObj;
    });
  }

  async _kvBackup(newObj) {
    try {
      if (this.env.BUNGIE_STATS) {
        await this.env.BUNGIE_STATS.put('latest', JSON.stringify(newObj));
      }
    } catch (e) {
      console.warn('KV backup failed', e);
    }
  }

  // Small helper: perform Bungie fetch (with timeout). Replace URL/logic with real Bungie API.
  async _fetchBungieCount() {
    // Example placeholder: if BUNGIE_API_KEY is present, you can call the real API here.
    // Use AbortController for timeouts and short retry logic.
    // For now return a stub value to test the pipeline.
    return 5;
  }

  async fetch(request) {
    await this._ensure();
    const path = new URL(request.url).pathname.replace(/^\//, '') || '';

    // GET / -> current authoritative data from DO storage
    if (request.method === 'GET' && (path === '' || path === '/')) {
      const current = await this.state.storage.get('data');
      return new Response(JSON.stringify(current), { headers: { 'Content-Type': 'application/json' }});
    }

    // POST /update -> authoritative fetch + write (used by cron and manual GET fallback)
    if (request.method === 'POST' && path === 'update') {
      const last = (await this.state.storage.get('lastUpdateAt')) || 0;
      const now = Date.now();
      // Prevent overlapping/too-frequent external fetches (cron already every 5m)
      const minIntervalMs = 60 * 1000; // 1 minute guard for safety
      if (now - last < minIntervalMs) {
        const current = await this.state.storage.get('data');
        return new Response(JSON.stringify({ ok: false, reason: 'rate_limited', current }), { headers: { 'Content-Type': 'application/json' }});
      }

      await this.state.storage.put('lastUpdateAt', now);

      // Do the Bungie fetch with timeout & simple retry
      let count = null;
      try {
        // Insert real fetch logic here, using this.env.BUNGIE_API_KEY if available.
        // Example: count = await this._fetchBungieCountWithTimeout();
        count = count + await this._fetchBungieCount();
      } catch (err) {
        console.error('external fetch failed', err);
      }

      if (typeof count === 'number') {
        const newObj = { clears: Math.max(0, Math.floor(count)), updated: new Date().toISOString(), source: 'bungie' };
        await this._atomicWrite(newObj);
        await this.state.storage.put('lastSuccessfulAt', new Date().toISOString());
        await this._kvBackup(newObj);
        return new Response(JSON.stringify({ ok: true, updated: newObj }), { headers: { 'Content-Type': 'application/json' }});
      }

      // fetch failed: keep current data, write lastError
      const current = await this.state.storage.get('data');
      const errObj = { time: new Date().toISOString(), message: 'fetch_failed' };
      await this.state.storage.put('lastError', errObj);
      return new Response(JSON.stringify({ ok: false, reason: 'fetch_failed', current }), { headers: { 'Content-Type': 'application/json' }});
    }

    // POST /incr -> atomic increment (admin)
    if (request.method === 'POST' && path === 'incr') {
      let body = null;
      try { body = await request.json().catch(() => null); } catch(e) { body = null; }
      const delta = (body && typeof body.increment === 'number') ? Math.floor(body.increment) : 1;

      const newObj = await this.state.storage.transaction(async (tx) => {
        const current = (await tx.get('data')) || { clears: 0, updated: null, source: 'init' };
        const updatedClears = Math.max(0, Math.floor((current.clears || 0) + delta));
        const result = { ...current, clears: updatedClears, updated: new Date().toISOString(), source: 'manual-incr' };
        await tx.put('data', result);
        return result;
      });

      await this._kvBackup(newObj);
      return new Response(JSON.stringify({ ok: true, updated: newObj }), { headers: { 'Content-Type': 'application/json' }});
    }

    // POST /set -> set absolute value (admin)
    if (request.method === 'POST' && path === 'set') {
      let body = null;
      try { body = await request.json().catch(() => null); } catch(e) { body = null; }
      if (body && typeof body.set === 'number') {
        const newObj = { clears: Math.max(0, Math.floor(body.set)), updated: new Date().toISOString(), source: 'manual' };
        await this._atomicWrite(newObj);
        await this._kvBackup(newObj);
        return new Response(JSON.stringify({ ok: true, updated: newObj }), { headers: { 'Content-Type': 'application/json' }});
      }
      return new Response(JSON.stringify({ ok: false, reason: 'invalid_payload' }), { status: 400, headers: { 'Content-Type': 'application/json' }});
    }

    return new Response('Not found', { status: 404 });
  }
}