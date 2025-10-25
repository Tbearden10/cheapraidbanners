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

  // Fetch raw clan roster (returns array of result objects from Bungie)
  async _fetchClanRosterRaw() {
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

    // Expect payload.Response.results (Bungie GroupsV2)
    const arr = payload?.Response?.results ?? payload?.results ?? payload?.members ?? null;
    if (!Array.isArray(arr)) return null;
    return arr;
  }

  // Map a raw Bungie "result" entry to a minimal member object
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
      // raw fields useful to front-end/debugging
      memberType: result.memberType ?? null,   // numeric memberType from GroupsV2 (preserve; frontend can map to role strings)
      raw: result
    };
  }

  // Fetch profile data to extract emblem/background (Profile endpoint). Returns emblemUrl or null.
  async _fetchEmblemForProfile(membershipType, membershipId) {
    if (!membershipId) return null;
    if (!this.env.BUNGIE_API_KEY) return null;
    const base = 'https://www.bungie.net';
    const url = `${base}/Platform/Destiny2/${membershipType}/Profile/${encodeURIComponent(membershipId)}/?components=100,200`;
    const headers = { Accept: 'application/json', 'X-API-Key': this.env.BUNGIE_API_KEY };

    try {
      const res = await this._fetchWithTimeout(url, { method: 'GET', headers }, Number(this.env.BUNGIE_PER_REQUEST_TIMEOUT_MS || 7000));
      if (!res || !res.ok) return null;
      const j = await res.json().catch(() => null);
      if (!j) return null;

      // Try common places for an emblem/background path
      const resp = j.Response ?? j;
      const profileData = resp?.profile ?? resp?.profileData ?? resp;
      // 1) userInfo.profilePicturePath (profile component)
      let emblemPath = profileData?.data?.userInfo?.profilePicturePath
        // 2) profile data's emblemBackgroundPath
        || profileData?.data?.emblemBackgroundPath
        // 3) fallback to first character's emblem path (component 200)
        || (function () {
          const chars = resp?.characters?.data;
          if (chars && typeof chars === 'object') {
            const first = Object.values(chars)[0];
            return first?.emblemBackgroundPath ?? first?.emblemPath ?? null;
          }
          return null;
        })();

      if (emblemPath) return `${base}${emblemPath}`;
      return null;
    } catch (e) {
      // don't fail hard on individual profile fetches
      return null;
    }
  }

  // Enrich members with emblemUrl (profile fetch). Uses batching to avoid too many concurrent profile calls.
  async _enrichMembersWithEmblems(membersRaw) {
    const out = [];
    if (!Array.isArray(membersRaw)) return out;

    const batchSize = Number(this.env.BUNGIE_PROFILE_BATCH_SIZE || 6); // default concurrency 6
    for (let i = 0; i < membersRaw.length; i += batchSize) {
      const chunk = membersRaw.slice(i, i + batchSize);
      const promises = chunk.map(async (r) => {
        const mapped = this._mapBungieResultToMember(r);
        // attempt profile fetch for emblem
        try {
          mapped.emblemUrl = await this._fetchEmblemForProfile(mapped.membershipType || 3, mapped.membershipId);
        } catch (e) {
          mapped.emblemUrl = null;
        }
        return mapped;
      });
      // wait for chunk
      const results = await Promise.all(promises);
      out.push(...results);
      // optional small pause between batches to be polite to Bungie (can be configured)
      const pauseMs = Number(this.env.BUNGIE_PROFILE_BATCH_PAUSE_MS || 250);
      if (pauseMs > 0 && i + batchSize < membersRaw.length) {
        await new Promise(res => setTimeout(res, pauseMs));
      }
    }

    return out;
  }

  // Minimal clears compute stub — reads members and produces a simple aggregation.
  async _computeClears(members) {
    const total = Array.isArray(members) ? members.length : 0;
    return { clears: total, prophecyClears: 0, perMember: null };
  }

  // POST /update-members : fetch roster, enrich with emblemUrl (profile), write KV.members_list
  async _handleUpdateMembers() {
    // rate-limit
    const last = (await this.state.storage.get('lastMembersUpdateAt')) || 0;
    const now = Date.now();
    const minMs = Number(this.env.MIN_MEMBERS_UPDATE_INTERVAL_MS || 3 * 60 * 60 * 1000); // default 3h
    if (now - last < minMs) {
      return { ok: false, reason: 'rate_limited' };
    }
    await this.state.storage.put('lastMembersUpdateAt', now);

    const raw = await this._fetchClanRosterRaw();
    if (!Array.isArray(raw)) {
      await this.state.storage.put('lastMembersFetchErrorAt', new Date().toISOString());
      return { ok: false, reason: 'fetch_failed' };
    }

    // Enrich (profile emblem + mapping) — be mindful of Bungie API rate limits.
    const members = await this._enrichMembersWithEmblems(raw);

    const fetchedAt = new Date().toISOString();
    await this._kvPut('members_list', { fetchedAt, members });
    await this.state.storage.put('lastMembersFetchedAt', fetchedAt);

    return { ok: true, memberCount: members.length };
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