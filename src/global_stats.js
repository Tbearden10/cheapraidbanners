export class GlobalStats {
  constructor(state, env) {
    this.state = state;
    this.env = env;
  }

  async _ensure() {
    const d = await this.state.storage.get('data');
    if (typeof d === 'undefined') {
      const init = { memberCount: 0, clears: 0, updated: null, source: 'init' };
      await this.state.storage.put('data', init);
    }
  }

  async _atomicWrite(newObj) {
    return this.state.storage.transaction(async (tx) => {
      await tx.put('data', newObj);
      return newObj;
    });
  }

  // write a canonical "latest" snapshot and optional per-key backups
  async _kvBackup(snapshot, { membersList = null, clearsSnapshot = null } = {}) {
    try {
      if (!this.env.BUNGIE_STATS) {
        console.warn('No BUNGIE_STATS KV binding available; skipping KV backup');
        return;
      }
      // write unified latest snapshot
      await this.env.BUNGIE_STATS.put('latest', JSON.stringify(snapshot));

      // optional separate writes for members_list and clears_snapshot
      if (membersList !== null) {
        await this.env.BUNGIE_STATS.put('members_list', JSON.stringify({ fetchedAt: new Date().toISOString(), members: membersList }));
      }
      if (clearsSnapshot !== null) {
        await this.env.BUNGIE_STATS.put('clears_snapshot', JSON.stringify(clearsSnapshot));
      }
    } catch (e) {
      console.warn('KV backup failed', e);
      // non-fatal; continue
    }
  }

  async _fetchWithTimeout(url, opts = {}, timeoutMs = 8000) {
    const controller = new AbortController();
    const to = setTimeout(() => controller.abort(), timeoutMs);
    try {
      const res = await fetch(url, { ...opts, signal: controller.signal });
      clearTimeout(to);
      return res;
    } finally {
      clearTimeout(to);
    }
  }

  // fetch raw clan roster JSON (unchanged)
  async _fetchClanRoster() {
    const clanId = this.env.BUNGIE_CLAN_ID;
    const template = this.env.BUNGIE_CLAN_ROSTER_ENDPOINT;
    if (!clanId || !template) {
      await this.state.storage.put('lastError', { time: new Date().toISOString(), message: 'missing_roster_config' });
      console.warn('Missing BUNGIE_CLAN_ID or BUNGIE_CLAN_ROSTER_ENDPOINT');
      return null;
    }

    const url = template.replace('{clanId}', encodeURIComponent(clanId));
    const headers = { Accept: 'application/json' };
    if (this.env.BUNGIE_API_KEY) headers['X-API-Key'] = this.env.BUNGIE_API_KEY;

    const timeoutMs = Number(this.env.BUNGIE_PER_REQUEST_TIMEOUT_MS || 7000);

    let res;
    try {
      res = await this._fetchWithTimeout(url, { method: 'GET', headers }, timeoutMs);
    } catch (err) {
      console.error('Roster network error', err);
      await this.state.storage.put('lastError', { time: new Date().toISOString(), message: 'roster_network', detail: String(err) });
      return null;
    }

    if (!res || !res.ok) {
      const txt = await (res ? res.text().catch(() => '') : Promise.resolve('no-response'));
      console.warn('Roster HTTP error', res && res.status, txt.slice(0, 300));
      await this.state.storage.put('lastError', { time: new Date().toISOString(), message: 'roster_http', status: res && res.status, detail: txt.slice(0, 300) });
      return null;
    }

    let payload;
    try {
      payload = await res.json();
    } catch (err) {
      const txt = await res.text().catch(() => '');
      console.warn('Roster JSON parse failed', err, txt.slice(0, 300));
      await this.state.storage.put('lastError', { time: new Date().toISOString(), message: 'roster_json_parse', detail: String(err) });
      return null;
    }

    // small DO storage marker
    try { await this.state.storage.put('lastRoster', { fetchedAt: new Date().toISOString(), size: (payload && JSON.stringify(payload).length) || 0 }); } catch(e){}

    return payload;
  }

  // -- NEW: create and persist a canonical members list in KV --
  // returns normalized members array or null on failure
  async _updateMembersList() {
    const roster = await this._fetchClanRoster();
    if (!roster) return null;

    // Extract member objects in a conservative way.
    // Adjust this mapping to suit the exact roster/shape you get back.
    let membersArray =
      roster?.Response?.detail ? null : // GroupV2 detail contains only memberCount; use roster endpoint instead when members array exists
      roster?.Response?.results ??
      roster?.Response?.data?.results ??
      roster?.results ??
      roster?.members ??
      null;

    // Some shapes: Response.results => entries that include member info; try to map
    if (!Array.isArray(membersArray) && Array.isArray(roster?.Response?.results)) {
      const alt = roster.Response.results.map(r => r?.member ?? r);
      if (Array.isArray(alt)) membersArray = alt;
    }

    // If still no members array but GroupV2 returned detail with memberCount only, fallback to empty array
    if (!Array.isArray(membersArray)) {
      // nothing to store as member list; but we can return null to indicate we didn't get members
      const mc = roster?.Response?.detail?.memberCount ?? roster?.memberCount ?? null;
      if (mc != null) {
        // store only the count in DO storage and KV latest (no members list)
        const snapshot = { memberCount: Number(mc), clears: Number(mc), updated: new Date().toISOString(), source: 'bungie' };
        await this._atomicWrite(snapshot);
        await this._kvBackup(snapshot, { membersList: [], clearsSnapshot: { clears: snapshot.clears, updated: snapshot.updated } });
        return [];
      }
      await this.state.storage.put('lastError', { time: new Date().toISOString(), message: 'no_members_array_in_roster' });
      return null;
    }

    // Normalize minimal member fields (membershipId, membershipType, displayName) for future clears fetching.
    const normalized = membersArray.map(m => {
      const member = m?.member ?? m;
      return {
        membershipId: String(member?.membershipId ?? member?.membership_id ?? member?.id ?? member?.destinyId ?? ''),
        membershipType: member?.membershipType ?? member?.membership_type ?? member?.type ?? 0,
        displayName: member?.displayName ?? member?.display_name ?? member?.name ?? ''
      };
    }).filter(x => x.membershipId);

    // Persist members_list to KV and also update DO storage.memberCount and latest snapshot
    try {
      const snapshot = {
        memberCount: normalized.length,
        clears: normalized.length, // keep clears in sync for now (you will compute real clears in updateClears)
        updated: new Date().toISOString(),
        source: 'bungie-members'
      };
      // DO storage
      await this._atomicWrite(snapshot);
      // KV backup: latest + members_list
      await this._kvBackup(snapshot, { membersList: normalized, clearsSnapshot: { clears: snapshot.clears, updated: snapshot.updated } });
      // also record lastSuccessfulAt
      await this.state.storage.put('lastSuccessfulAt', new Date().toISOString());
    } catch (e) {
      console.warn('Failed to persist members list or snapshot', e);
    }

    return normalized;
  }

  // -- NEW: compute clears using members list stored in KV (or fallback to DO storage) --
  // This function should fetch per-member clears (or other metric) and aggregate.
  // For now it contains a small stub that returns members.length (so it's usable immediately).
  // Replace perMemberFetch() with real Bungie API calls to compute actual clears.
  async _updateClearsFromMembersList() {
    // 1) Try to read members_list from KV
    let members = null;
    try {
      if (this.env.BUNGIE_STATS) {
        const raw = await this.env.BUNGIE_STATS.get('members_list');
        if (raw) {
          const parsed = JSON.parse(raw);
          members = parsed?.members ?? null;
        }
      }
    } catch (e) {
      console.warn('Failed to read members_list from KV', e);
      members = null;
    }

    // 2) If KV not present, try DO storage lastRoster / fetch again
    if (!Array.isArray(members)) {
      // try DO method to fetch roster (and store members_list)
      const normalized = await this._updateMembersList();
      if (Array.isArray(normalized)) members = normalized;
    }

    if (!Array.isArray(members)) {
      await this.state.storage.put('lastError', { time: new Date().toISOString(), message: 'no_members_for_clears' });
      return null;
    }

    // 3) Aggreate clears from members:
    // TODO: implement real per-member API calls here (use _fetchWithTimeout + concurrency).
    // For now: fallback -> treat clears as simply the members.length (so pages work immediately).
    const totalClears = members.length;

    // Persist clears snapshot and update DO storage/data.latest
    const snapshot = { memberCount: members.length, clears: totalClears, updated: new Date().toISOString(), source: 'bungie-clears' };
    try {
      await this._atomicWrite(snapshot);
      await this._kvBackup(snapshot, { membersList: members, clearsSnapshot: { clears: totalClears, updated: snapshot.updated } });
      await this.state.storage.put('lastSuccessfulAt', new Date().toISOString());
    } catch (e) {
      console.warn('Failed to persist clears snapshot', e);
    }

    return totalClears;
  }

  // Main fetch handler now supports separate update endpoints
  async fetch(request) {
    await this._ensure();
    const path = new URL(request.url).pathname.replace(/^\//, '') || '';

    // GET / -> return current authoritative data from DO storage
    if (request.method === 'GET' && (path === '' || path === '/')) {
      const current = await this.state.storage.get('data');
      return new Response(JSON.stringify(current), { headers: { 'Content-Type': 'application/json' }});
    }

    // POST /update-members -> fetch roster and store member list (infrequent)
    if (request.method === 'POST' && path === 'update-members') {
      // rate-limit guard
      const last = (await this.state.storage.get('lastMembersUpdateAt')) || 0;
      const now = Date.now();
      const minMs = Number(this.env.MIN_MEMBERS_UPDATE_INTERVAL_MS || 60 * 60 * 1000); // default 1h
      if (now - last < minMs) {
        const current = await this.state.storage.get('data');
        return new Response(JSON.stringify({ ok: false, reason: 'rate_limited', current }), { headers: { 'Content-Type': 'application/json' }});
      }
      await this.state.storage.put('lastMembersUpdateAt', now);

      const members = await this._updateMembersList();
      if (Array.isArray(members)) {
        return new Response(JSON.stringify({ ok: true, memberCount: members.length }), { headers: { 'Content-Type': 'application/json' }});
      } else {
        const current = await this.state.storage.get('data');
        return new Response(JSON.stringify({ ok: false, reason: 'fetch_failed', current }), { headers: { 'Content-Type': 'application/json' }});
      }
    }

    // POST /update-clears -> compute clears using cached members_list (more frequent)
    if (request.method === 'POST' && path === 'update-clears') {
      const last = (await this.state.storage.get('lastClearsUpdateAt')) || 0;
      const now = Date.now();
      const minMs = Number(this.env.MIN_CLEARS_UPDATE_INTERVAL_MS || 5 * 60 * 1000); // default 5m
      if (now - last < minMs) {
        const current = await this.state.storage.get('data');
        return new Response(JSON.stringify({ ok: false, reason: 'rate_limited', current }), { headers: { 'Content-Type': 'application/json' }});
      }
      await this.state.storage.put('lastClearsUpdateAt', now);

      const total = await this._updateClearsFromMembersList();
      if (typeof total === 'number') {
        return new Response(JSON.stringify({ ok: true, clears: total }), { headers: { 'Content-Type': 'application/json' }});
      } else {
        const current = await this.state.storage.get('data');
        return new Response(JSON.stringify({ ok: false, reason: 'fetch_failed', current }), { headers: { 'Content-Type': 'application/json' }});
      }
    }

    // Backwards-compatible single update route (keeps previous behavior)
    if (request.method === 'POST' && path === 'update') {
      // call clears update by default (preserves prior behavior)
      const res = await this.fetch(new Request(new URL(request.url).toString().replace(/\/update$/, '/update-clears'), { method: 'POST' }));
      return res;
    }

    // POST /incr and /set operate on memberCount (kept for admin)
    if (request.method === 'POST' && path === 'incr') {
      let body = null;
      try { body = await request.json().catch(() => null); } catch(e) { body = null; }
      const delta = (body && typeof body.increment === 'number') ? Math.floor(body.increment) : 1;

      const newObj = await this.state.storage.transaction(async (tx) => {
        const current = (await tx.get('data')) || { memberCount: 0, updated: null, source: 'init' };
        const updatedCount = Math.max(0, Math.floor((current.memberCount || 0) + delta));
        const result = { ...current, memberCount: updatedCount, clears: updatedCount, updated: new Date().toISOString(), source: 'manual-incr' };
        await tx.put('data', result);
        return result;
      });

      await this._kvBackup(newObj, { membersList: null, clearsSnapshot: { clears: newObj.clears, updated: newObj.updated } });
      return new Response(JSON.stringify({ ok: true, updated: newObj }), { headers: { 'Content-Type': 'application/json' }});
    }

    if (request.method === 'POST' && path === 'set') {
      let body = null;
      try { body = await request.json().catch(() => null); } catch(e) { body = null; }
      if (body && typeof body.set === 'number') {
        const newObj = { memberCount: Math.max(0, Math.floor(body.set)), clears: Math.max(0, Math.floor(body.set)), updated: new Date().toISOString(), source: 'manual' };
        await this._atomicWrite(newObj);
        await this._kvBackup(newObj, { membersList: null, clearsSnapshot: { clears: newObj.clears, updated: newObj.updated } });
        return new Response(JSON.stringify({ ok: true, updated: newObj }), { headers: { 'Content-Type': 'application/json' }});
      }
      return new Response(JSON.stringify({ ok: false, reason: 'invalid_payload' }), { status: 400, headers: { 'Content-Type': 'application/json' }});
    }

    return new Response('Not found', { status: 404 });
  }
}