export class GlobalStats {
  constructor(state, env) {
    this.state = state;
    this.env = env;
  }

  // tiny helper to fetch with timeout
  async _fetchWithTimeout(url, opts = {}, timeoutMs = 7000) {
    const c = new AbortController();
    const t = setTimeout(() => c.abort(), timeoutMs);
    try {
      const res = await fetch(url, { ...opts, signal: c.signal });
      clearTimeout(t);
      return res;
    } finally { clearTimeout(t); }
  }

  // conservative roster fetch + normalize
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
    if (!res.ok) return null;
    let payload;
    try { payload = await res.json(); } catch (e) { return null; }
    // try common shapes to extract members array
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

  // write to KV.latest and KV.members_list (if provided)
  async _kvPut(key, valueObj) {
    if (!this.env.BUNGIE_STATS) return;
    try { await this.env.BUNGIE_STATS.put(key, JSON.stringify(valueObj)); } catch (e) { /* ignore */ }
  }

  // compute clears - simple stub: counts members (replace with per-member logic later)
  async _computeClearsFromMembers(members) {
    // placeholder: treat clears as number of members
    return { clears: Array.isArray(members) ? members.length : 0, prophecyClears: 0 };
  }

  // POST /update-members : fetch roster -> persist members_list
  async _handleUpdateMembers() {
    // simple rate-limit guard using state storage flag (prevent too-frequent manual calls)
    const last = (await this.state.storage.get('lastMembersUpdateAt')) || 0;
    const now = Date.now();
    const minMs = Number(this.env.MIN_MEMBERS_UPDATE_INTERVAL_MS || 60 * 60 * 1000);
    if (now - last < minMs) {
      return { ok: false, reason: 'rate_limited' };
    }
    await this.state.storage.put('lastMembersUpdateAt', now);

    const members = await this._fetchClanRoster();
    if (!Array.isArray(members)) return { ok: false, reason: 'fetch_failed' };

    const fetchedAt = new Date().toISOString();
    await this._kvPut('members_list', { fetchedAt, members });
    // update a minimal latest snapshot's memberCount so /stats has a starting point
    const snapshot = { memberCount: members.length, clears: 0, prophecyClears: 0, updated: fetchedAt, source: 'members-update' };
    await this._kvPut('latest', snapshot);
    // persist basic DO storage for debug
    await this.state.storage.put('lastMembersFetchedAt', fetchedAt);
    return { ok: true, memberCount: members.length, members };
  }

  // POST /update-clears : read members_list from KV -> compute clears -> persist latest snapshot
  async _handleUpdateClears() {
    let members = [];
    try {
      if (this.env.BUNGIE_STATS) {
        const raw = await this.env.BUNGIE_STATS.get('members_list');
        if (raw) {
          const parsed = JSON.parse(raw);
          members = parsed?.members ?? [];
        }
      }
    } catch (e) {
      members = [];
    }
    const { clears, prophecyClears } = await this._computeClearsFromMembers(members);
    const snapshot = { memberCount: members.length, clears, prophecyClears, updated: new Date().toISOString(), source: 'clears-update' };
    await this._kvPut('latest', snapshot);
    await this.state.storage.put('lastClearsFetchedAt', snapshot.updated);
    return { ok: true, snapshot };
  }

  // public fetch handler for the DO
  async fetch(request) {
    const url = new URL(request.url);
    const path = url.pathname.replace(/^\//, '') || '';
    if (request.method === 'POST' && path === 'update-members') {
      const res = await this._handleUpdateMembers();
      return new Response(JSON.stringify(res), { headers: { 'Content-Type': 'application/json' }});
    }
    if (request.method === 'POST' && path === 'update-clears') {
      const res = await this._handleUpdateClears();
      return new Response(JSON.stringify(res), { headers: { 'Content-Type': 'application/json' }});
    }
    // GET / returns simple DO storage snapshot for debugging
    if (request.method === 'GET' && (path === '' || path === '/')) {
      const lastMembers = await this.state.storage.get('lastMembersFetchedAt') || null;
      const lastClears = await this.state.storage.get('lastClearsFetchedAt') || null;
      const data = { lastMembers, lastClears };
      return new Response(JSON.stringify(data), { headers: { 'Content-Type': 'application/json' }});
    }
    return new Response('Not found', { status: 404 });
  }
}