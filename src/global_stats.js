export class GlobalStats {
  constructor(state, env) {
    this.state = state;
    this.env = env;
  }

  // KV helpers
  async _kvPut(key, obj) {
    if (!this.env.BUNGIE_STATS) return;
    try { await this.env.BUNGIE_STATS.put(key, JSON.stringify(obj)); } catch (e) { /* ignore */ }
  }
  async _kvGet(key) {
    if (!this.env.BUNGIE_STATS) return null;
    try {
      const raw = await this.env.BUNGIE_STATS.get(key);
      return raw ? JSON.parse(raw) : null;
    } catch (e) { return null; }
  }

  // fetch with timeout
  async _fetchWithTimeout(url, opts = {}, timeoutMs = 7000) {
    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort(), timeoutMs);
    try {
      const res = await fetch(url, { ...opts, signal: ctrl.signal });
      clearTimeout(timer);
      return res;
    } finally { clearTimeout(timer); }
  }

  // Normalize roster payload into minimal array [{ membershipId, membershipType, displayName }]
  async _fetchClanRoster() {
    const clanId = this.env.BUNGIE_CLAN_ID;
    const template = this.env.BUNGIE_CLAN_ROSTER_ENDPOINT;
    if (!clanId || !template) return null;
    const url = template.replace('{clanId}', encodeURIComponent(clanId));
    const headers = { Accept: 'application/json' };
    if (this.env.BUNGIE_API_KEY) headers['X-API-Key'] = this.env.BUNGIE_API_KEY;

    let res;
    try { res = await this._fetchWithTimeout(url, { method: 'GET', headers }, Number(this.env.BUNGIE_PER_REQUEST_TIMEOUT_MS || 7000)); }
    catch (e) { return null; }
    if (!res || !res.ok) return null;

    let payload;
    try { payload = await res.json(); } catch (e) { return null; }

    // Try common shapes
    let arr = payload?.Response?.results ?? payload?.results ?? payload?.members ?? null;
    if (!Array.isArray(arr) && Array.isArray(payload?.Response?.results)) {
      arr = payload.Response.results.map(r => r?.member ?? r);
    }
    if (!Array.isArray(arr)) return null;

    const normalized = arr.map(m => {
      const member = m?.member ?? m;
      return {
        membershipId: String(member?.membershipId ?? member?.id ?? member?.destinyId ?? ''),
        membershipType: member?.membershipType ?? member?.membership_type ?? 0,
        displayName: member?.displayName ?? member?.display_name ?? member?.name ?? ''
      };
    }).filter(x => x.membershipId);

    return normalized;
  }

  // Minimal clears compute stub — reads members and produces a simple aggregation.
  // Replace perMember logic with real API calls if/when needed.
  async _computeClears(members) {
    const total = Array.isArray(members) ? members.length : 0;
    return { clears: total, prophecyClears: 0, perMember: null };
  }

  // POST /update-members : fetch roster and write KV.members_list
  async _handleUpdateMembers() {
    // rate-limit
    const last = (await this.state.storage.get('lastMembersUpdateAt')) || 0;
    const now = Date.now();
    const minMs = Number(this.env.MIN_MEMBERS_UPDATE_INTERVAL_MS || 3 * 60 * 60 * 1000); // default 3h
    if (now - last < minMs) {
      return { ok: false, reason: 'rate_limited' };
    }
    await this.state.storage.put('lastMembersUpdateAt', now);

    const members = await this._fetchClanRoster();
    if (!Array.isArray(members)) {
      await this.state.storage.put('lastMembersFetchErrorAt', new Date().toISOString());
      return { ok: false, reason: 'fetch_failed' };
    }

    const fetchedAt = new Date().toISOString();
    await this._kvPut('members_list', { fetchedAt, members });
    await this.state.storage.put('lastMembersFetchedAt', fetchedAt);

    // Do NOT touch clears_snapshot here — members update only writes members_list
    return { ok: true, memberCount: members.length, membersCount: members.length };
  }

  // POST /update-clears : read KV.members_list -> compute clears -> write KV.clears_snapshot
  async _handleUpdateClears() {
    const ml = await this._kvGet('members_list');
    const members = Array.isArray(ml?.members) ? ml.members : [];
    const computed = await this._computeClears(members);
    const snapshot = { fetchedAt: new Date().toISOString(), clears: computed.clears, prophecyClears: computed.prophecyClears, perMember: computed.perMember };
    await this._kvPut('clears_snapshot', snapshot);
    await this.state.storage.put('lastClearsFetchedAt', snapshot.fetchedAt);
    return { ok: true, snapshot };
  }

  // DO fetch handler
  async fetch(request) {
    const path = new URL(request.url).pathname.replace(/^\//, '') || '';

    if (request.method === 'POST' && path === 'update-members') {
      return new Response(JSON.stringify(await this._handleUpdateMembers()), { headers: { 'Content-Type': 'application/json' }});
    }

    if (request.method === 'POST' && path === 'update-clears') {
      return new Response(JSON.stringify(await this._handleUpdateClears()), { headers: { 'Content-Type': 'application/json' }});
    }

    if (request.method === 'GET' && (path === '' || path === '/')) {
      const lastMembers = await this.state.storage.get('lastMembersFetchedAt') || null;
      const lastClears = await this.state.storage.get('lastClearsFetchedAt') || null;
      return new Response(JSON.stringify({ lastMembers, lastClears }), { headers: { 'Content-Type': 'application/json' }});
    }

    return new Response('Not found', { status: 404 });
  }
}