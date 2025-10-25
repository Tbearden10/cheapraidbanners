export class GlobalStats {
  constructor(state, env) {
    this.state = state;
    this.env = env;
  }

  // NOTE: KV calls are intentionally commented out. Re-enable if/when you want to persist.
  // Example (commented):
  // async _kvPut(key, obj) { if (!this.env.BUNGIE_STATS) return; await this.env.BUNGIE_STATS.put(key, JSON.stringify(obj)); }
  // async _kvGet(key) { if (!this.env.BUNGIE_STATS) return null; const raw = await this.env.BUNGIE_STATS.get(key); return raw ? JSON.parse(raw) : null; }

  async _fetchWithTimeout(url, opts = {}, timeoutMs = 8000) {
    const ctrl = new AbortController();
    const id = setTimeout(() => ctrl.abort(), timeoutMs);
    const method = opts.method ?? 'GET';
    const start = Date.now();
    try {
      console.log('[DO] fetch start', method, url);
      const res = await fetch(url, { ...opts, signal: ctrl.signal });
      console.log('[DO] fetch done', url, 'status', res.status, 'took', Date.now() - start, 'ms');
      return res;
    } catch (err) {
      console.warn('[DO] fetch error', url, err && (err.name || err.message));
      throw err;
    } finally {
      clearTimeout(id);
    }
  }

  _mapBungieResultToMember(result) {
    const destiny = result?.destinyUserInfo ?? {};
    const bungie = result?.bungieNetUserInfo ?? {};
    return {
      membershipId: String(destiny.membershipId ?? bungie.membershipId ?? ''),
      membershipType: Number(destiny.membershipType ?? bungie.membershipType ?? 0),
      displayName: destiny.displayName ?? bungie.displayName ?? '',
      supplementalDisplayName: bungie.supplementalDisplayName ?? '',
      isOnline: !!result.isOnline,
      joinDate: result.joinDate ?? null,
      memberType: result.memberType ?? null
    };
  }

  async _fetchClanRosterRaw() {
    const clanId = this.env.BUNGIE_CLAN_ID;
    const template = this.env.BUNGIE_CLAN_ROSTER_ENDPOINT;
    if (!clanId || !template) {
      const err = new Error('missing BUNGIE_CLAN_ID or BUNGIE_CLAN_ROSTER_ENDPOINT');
      err.code = 'missing_config';
      throw err;
    }
    const url = template.replace('{clanId}', encodeURIComponent(clanId));
    const headers = { Accept: 'application/json' };
    if (this.env.BUNGIE_API_KEY) headers['X-API-Key'] = this.env.BUNGIE_API_KEY;

    const timeoutMs = Number(this.env.BUNGIE_PER_REQUEST_TIMEOUT_MS || 8000);
    const res = await this._fetchWithTimeout(url, { method: 'GET', headers }, timeoutMs);
    if (!res || !res.ok) {
      const err = new Error(`bungie responded ${res ? res.status : 'no_response'}`);
      err.code = 'bungie_non_ok';
      err.status = res ? res.status : null;
      throw err;
    }
    const payload = await res.json().catch(() => null);
    if (!payload) {
      const err = new Error('invalid_json_from_bungie');
      err.code = 'invalid_json';
      throw err;
    }
    const arr = payload?.Response?.results ?? payload?.results ?? payload?.members ?? null;
    if (!Array.isArray(arr)) {
      const err = new Error('unexpected_payload_shape');
      err.code = 'unexpected_payload_shape';
      err.payload = payload;
      throw err;
    }
    return arr;
  }

  // POST /update-members -> fetch roster directly from Bungie and return members in response
  async _handleUpdateMembers({ force = false } = {}) {
    // rate-limit logic can remain (DO-level). We will still allow force via admin header.
    const last = (await this.state.storage.get('lastMembersUpdateAt')) || 0;
    const minMs = Number(this.env.MIN_MEMBERS_UPDATE_INTERVAL_MS || 0);
    const now = Date.now();
    if (!force && minMs > 0 && now - last < minMs) {
      return { ok: false, reason: 'rate_limited', nextAllowedAt: last + minMs };
    }
    await this.state.storage.put('lastMembersUpdateAt', now);

    // fetch roster
    const raw = await this._fetchClanRosterRaw();
    const members = raw.map(r => this._mapBungieResultToMember(r));
    const fetchedAt = new Date().toISOString();

    // KV write intentionally disabled. Uncomment to persist.
    // if (this.env.BUNGIE_STATS) {
    //   await this.env.BUNGIE_STATS.put('members_list', JSON.stringify({ fetchedAt, members }));
    // }

    await this.state.storage.put('lastMembersFetchedAt', fetchedAt);
    return { ok: true, members, fetchedAt, memberCount: members.length };
  }

  // POST /update-clears -> compute a minimal clears snapshot from the live roster (no KV)
  async _handleUpdateClears() {
    const raw = await this._fetchClanRosterRaw();
    const members = raw.map(r => this._mapBungieResultToMember(r));
    const computed = { clears: members.length, prophecyClears: 0, perMember: null };
    const snapshot = { fetchedAt: new Date().toISOString(), clears: computed.clears, prophecyClears: computed.prophecyClears, perMember: computed.perMember };

    // KV write intentionally disabled. Uncomment to persist.
    // if (this.env.BUNGIE_STATS) {
    //   await this.env.BUNGIE_STATS.put('clears_snapshot', JSON.stringify(snapshot));
    // }

    await this.state.storage.put('lastClearsFetchedAt', snapshot.fetchedAt);
    return { ok: true, snapshot };
  }

  async fetch(request) {
    const path = new URL(request.url).pathname.replace(/^\//, '') || '';

    // POST /update-members
    if (request.method === 'POST' && path === 'update-members') {
      // Allow force via admin header
      const adminHeader = request.headers.get('x-admin-token') || '';
      const force = !!(adminHeader && this.env.ADMIN_TOKEN && adminHeader === this.env.ADMIN_TOKEN);
      try {
        const out = await this._handleUpdateMembers({ force });
        return new Response(JSON.stringify(out), { headers: { 'Content-Type': 'application/json' }});
      } catch (e) {
        const body = { ok: false, reason: e.code || 'fetch_failed', error: e.message || String(e) };
        if (e.status) body.status = e.status;
        return new Response(JSON.stringify(body), { status: 502, headers: { 'Content-Type': 'application/json' }});
      }
    }

    // POST /update-clears
    if (request.method === 'POST' && path === 'update-clears') {
      try {
        const out = await this._handleUpdateClears();
        return new Response(JSON.stringify(out), { headers: { 'Content-Type': 'application/json' }});
      } catch (e) {
        const body = { ok: false, reason: e.code || 'fetch_failed', error: e.message || String(e) };
        if (e.status) body.status = e.status;
        return new Response(JSON.stringify(body), { status: 502, headers: { 'Content-Type': 'application/json' }});
      }
    }

    // GET /debug -> basic DO timestamps
    if (request.method === 'GET' && path === 'debug') {
      const lastMembers = await this.state.storage.get('lastMembersFetchedAt') || null;
      const lastClears = await this.state.storage.get('lastClearsFetchedAt') || null;
      return new Response(JSON.stringify({ lastMembers, lastClears }), { headers: { 'Content-Type': 'application/json' }});
    }

    return new Response('Not found', { status: 404 });
  }
}