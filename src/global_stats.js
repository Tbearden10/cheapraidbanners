export class GlobalStats {
  constructor(state, env) {
    this.state = state;
    this.env = env;
  }

  // simple helper: write debug to state storage
  async _writeDebug(obj) {
    try { await this.state.storage.put('lastDebug', obj); } catch (e) { /* ignore */ }
  }

  // fetch with timeout
  async _fetchWithTimeout(url, opts = {}, timeoutMs = 8000) {
    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort(), timeoutMs);
    try {
      const res = await fetch(url, { ...opts, signal: ctrl.signal });
      clearTimeout(timer);
      return res;
    } finally { clearTimeout(timer); }
  }

  // Map Bungie result -> minimal member
  _mapBungieResultToMember(result) {
    const destiny = result?.destinyUserInfo ?? {};
    const bungie = result?.bungieNetUserInfo ?? {};
    return {
      membershipId: String(destiny.membershipId ?? bungie.membershipId ?? ''),
      displayName: destiny.displayName ?? bungie.displayName ?? '',
      membershipType: Number(destiny.membershipType ?? bungie.membershipType ?? 0)
    };
  }

  // POST /update-members -> fetch roster from Bungie and write to KV
  async _handleUpdateMembers({ force = false } = {}) {
    // simple rate-limit: optional
    const last = (await this.state.storage.get('lastMembersUpdateAt')) || 0;
    const minMs = Number(this.env.MIN_MEMBERS_UPDATE_INTERVAL_MS || 0); // 0 means no rate-limit in this simple setup
    const now = Date.now();
    if (!force && minMs > 0 && now - last < minMs) {
      const next = last + minMs;
      await this._writeDebug({ event: 'rate_limited', next });
      return { ok: false, reason: 'rate_limited', nextAllowedAt: next };
    }
    await this.state.storage.put('lastMembersUpdateAt', now);

    // build roster URL
    const clanId = this.env.BUNGIE_CLAN_ID;
    const template = this.env.BUNGIE_CLAN_ROSTER_ENDPOINT;
    if (!clanId || !template) {
      await this._writeDebug({ event: 'missing_config' });
      return { ok: false, reason: 'missing_config' };
    }
    const url = template.replace('{clanId}', encodeURIComponent(clanId));
    const headers = { Accept: 'application/json' };
    if (this.env.BUNGIE_API_KEY) headers['X-API-Key'] = this.env.BUNGIE_API_KEY;

    let res;
    try {
      res = await this._fetchWithTimeout(url, { method: 'GET', headers }, Number(this.env.BUNGIE_PER_REQUEST_TIMEOUT_MS || 8000));
    } catch (e) {
      await this._writeDebug({ event: 'fetch_error', error: String(e) });
      return { ok: false, reason: 'fetch_failed' };
    }
    if (!res || !res.ok) {
      await this._writeDebug({ event: 'fetch_non_ok', status: res && res.status });
      return { ok: false, reason: 'fetch_failed', status: res && res.status };
    }

    let payload;
    try { payload = await res.json(); } catch (e) { payload = null; }
    const arr = payload?.Response?.results ?? payload?.results ?? payload?.members ?? [];
    const members = Array.isArray(arr) ? arr.map(r => this._mapBungieResultToMember(r)) : [];

    // write to KV (binding BUNGIE_STATS required)
    const fetchedAt = new Date().toISOString();
    if (this.env.BUNGIE_STATS) {
      try {
        await this.env.BUNGIE_STATS.put('members_list', JSON.stringify({ fetchedAt, members }));
      } catch (e) {
        await this._writeDebug({ event: 'kv_put_failed', error: String(e) });
        return { ok: false, reason: 'kv_put_failed' };
      }
    }

    await this.state.storage.put('lastMembersFetchedAt', fetchedAt);
    await this._writeDebug({ event: 'success', count: members.length, fetchedAt });
    return { ok: true, memberCount: members.length, fetchedAt };
  }

  // DO fetch handler
  async fetch(request) {
    const path = new URL(request.url).pathname.replace(/^\//, '') || '';

    // POST /update-members -> perform fetch & KV write
    if (request.method === 'POST' && path === 'update-members') {
      // allow force via x-admin-token header -> bypass rate-limit
      const adminHeader = request.headers.get('x-admin-token') || '';
      const force = !!(adminHeader && this.env.ADMIN_TOKEN && adminHeader === this.env.ADMIN_TOKEN);
      return new Response(JSON.stringify(await this._handleUpdateMembers({ force })), { headers: { 'Content-Type': 'application/json' }});
    }

    // GET /debug -> return last debug (small)
    if (request.method === 'GET' && path === 'debug') {
      const debug = await this.state.storage.get('lastDebug') || null;
      const lastMembersFetchedAt = await this.state.storage.get('lastMembersFetchedAt') || null;
      return new Response(JSON.stringify({ debug, lastMembersFetchedAt }), { headers: { 'Content-Type': 'application/json' }});
    }

    return new Response('Not found', { status: 404 });
  }
}