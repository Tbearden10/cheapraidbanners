// Durable Object: GlobalStats
// - Fetches clan roster, enriches members with emblem data, persists per-member KV keys `member:<id>`
// - Dispatches per-member jobs to MEMBER_WORKER for clears computation
// - update-clears can dispatch per-member jobs or compute synchronously (fallback)
// - Aggregates per-member KV entries and now selects a single mostRecentClanActivity (prefers multi-member instance)

import { computeClearsForMembers } from './lib/bungieApi.js';

export class GlobalStats {
  constructor(state, env) {
    this.state = state;
    this.env = env;
  }

  async _fetchWithTimeout(url, opts = {}, timeoutMs = 8000) {
    const ctrl = new AbortController();
    const id = setTimeout(() => ctrl.abort(), timeoutMs);
    try {
      const res = await fetch(url, { ...opts, signal: ctrl.signal });
      return res;
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
      supplementalDisplayName: bungie.supplementalDisplayName ?? '',
      isOnline: !!result.isOnline,
      joinDate: result.joinDate ?? null,
      memberType: result.memberType ?? null,
      emblemPath: null,
      emblemBackgroundPath: null,
      emblemHash: null,
      emblemFetchError: null
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

  // ----- emblem fetching helpers -----
  _makeProfileUrl(membershipType, membershipId) {
    return `https://www.bungie.net/Platform/Destiny2/${membershipType}/Profile/${encodeURIComponent(membershipId)}?components=100,200`;
  }

  async _fetchEmblemForMember(member) {
    const membershipType = Number(member.membershipType || 0);
    const membershipId = String(member.membershipId || '');
    if (!membershipType || !membershipId) {
      return { emblemPath: null, emblemBackgroundPath: null, emblemHash: null, error: 'missing_ids' };
    }
    const url = this._makeProfileUrl(membershipType, membershipId);
    const headers = { Accept: 'application/json' };
    if (this.env.BUNGIE_API_KEY) headers['X-API-Key'] = this.env.BUNGIE_API_KEY;
    const timeoutMs = Number(this.env.BUNGIE_PER_REQUEST_TIMEOUT_MS || 8000);
    try {
      const res = await this._fetchWithTimeout(url, { method: 'GET', headers }, timeoutMs);
      if (!res || !res.ok) {
        return { emblemPath: null, emblemBackgroundPath: null, emblemHash: null, error: `profile_non_ok:${res ? res.status : 'no'}` };
      }
      const payload = await res.json().catch(() => null);
      if (!payload) return { emblemPath: null, emblemBackgroundPath: null, emblemHash: null, error: 'invalid_json' };
      const response = payload.Response ?? {};
      const profileData = response?.profile?.data ?? {};
      const charactersData = response?.characters?.data ?? {};

      // choose most recent character
      let chosenChar = null;
      let latestTs = 0;
      for (const cid of Object.keys(charactersData)) {
        const c = charactersData[cid];
        if (!c) continue;
        const ts = Date.parse(c.dateLastPlayed ?? '') || 0;
        if (ts > latestTs) { latestTs = ts; chosenChar = c; }
      }
      if (!chosenChar && profileData && Array.isArray(profileData.characterIds) && profileData.characterIds.length) {
        const firstId = profileData.characterIds[0];
        chosenChar = charactersData[firstId] ?? null;
      }
      if (!chosenChar) {
        const anyKey = Object.keys(charactersData)[0];
        chosenChar = anyKey ? charactersData[anyKey] : null;
      }

      const rawEmblemPath = chosenChar?.emblemPath ?? null;
      const rawEmblemBackgroundPath = chosenChar?.emblemBackgroundPath ?? null;
      const emblemHash = chosenChar?.emblemHash ?? null;
      const toFull = (p) => (p ? (p.startsWith('http') ? p : `https://www.bungie.net${p}`) : null);
      return { emblemPath: toFull(rawEmblemPath), emblemBackgroundPath: toFull(rawEmblemBackgroundPath), emblemHash: emblemHash ?? null, error: null };
    } catch (err) {
      return { emblemPath: null, emblemBackgroundPath: null, emblemHash: null, error: err && (err.message || String(err)) ? (err.message || String(err)) : String(err) };
    }
  }

  async _concurrentMap(items, worker, limit = 6) {
    const results = new Array(items.length);
    let i = 0;
    const next = async () => {
      while (true) {
        const idx = i++;
        if (idx >= items.length) return;
        try {
          results[idx] = await worker(items[idx], idx);
        } catch (err) {
          results[idx] = { error: err && err.message ? err.message : String(err) };
        }
      }
    };
    const pool = [];
    for (let p = 0; p < Math.min(limit, items.length); p++) pool.push(next());
    await Promise.all(pool);
    return results;
  }

  async _populateMemberEmblems(members) {
    if (!Array.isArray(members) || members.length === 0) return members;
    const maxConcurrent = Number(this.env.BUNGIE_EMBLEM_CONCURRENCY || 6);
    await this._concurrentMap(
      members,
      async (member) => {
        try {
          const info = await this._fetchEmblemForMember(member);
          member.emblemPath = info.emblemPath;
          member.emblemBackgroundPath = info.emblemBackgroundPath;
          member.emblemHash = info.emblemHash;
          member.emblemFetchError = info.error;
        } catch (e) {
          member.emblemPath = null;
          member.emblemBackgroundPath = null;
          member.emblemHash = null;
          member.emblemFetchError = e && e.message ? e.message : String(e);
        }
        return member;
      },
      maxConcurrent
    );
    return members;
  }

  _membersEqual(a = [], b = []) {
    if (!Array.isArray(a) || !Array.isArray(b)) return false;
    if (a.length !== b.length) return false;
    const mapA = new Map(a.map(m => [String(m.membershipId), Number(m.membershipType)]));
    for (const m of b) {
      const id = String(m.membershipId);
      if (!mapA.has(id) || mapA.get(id) !== Number(m.membershipType)) return false;
    }
    return true;
  }

  async _dispatchMemberJob(member, opts = {}) {
    try {
      const idStr = String(member.membershipId);
      const memberDoId = this.env.MEMBER_WORKER.idFromName(`member-${idStr}`);
      const memberStub = this.env.MEMBER_WORKER.get(memberDoId);
      const body = JSON.stringify({
        membershipId: idStr,
        membershipType: Number(member.membershipType || 0),
        opts,
      });
      memberStub.fetch('https://member.local/process', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body,
      }).then(async (res) => {
        try {
          const text = await res.text().catch(() => null);
          console.log('[GlobalStats] member job ack', idStr, res.status, text && String(text).slice(0,200));
        } catch (e) {}
      }).catch((err) => {
        console.warn('[GlobalStats] member job dispatch failed for', idStr, err && err.message ? err.message : err);
      });
      return true;
    } catch (err) {
      console.warn('[GlobalStats] dispatch error', err && (err.message || err));
      return false;
    }
  }

  async _readMemberKV(membershipId) {
    if (!this.env.BUNGIE_STATS) return null;
    try {
      const raw = await this.env.BUNGIE_STATS.get(`member_clears:${membershipId}`);
      if (!raw) return null;
      return JSON.parse(raw);
    } catch (e) {
      console.warn('[GlobalStats] KV read member failed', membershipId, e && e.message ? e.message : e);
      return null;
    }
  }

  async _aggregateFromMemberKV(members) {
    const perMember = [];
    let total = 0;
    let totalProphecy = 0;

    // map instanceId -> { periodTs, activity, membershipIds: Set }
    const instanceMap = new Map();

    let globalLatestSolo = null; // fallback most recent across all members (if no multi-member activity found)

    for (const m of members) {
      try {
        const idStr = String(m.membershipId);
        const kv = await this._readMemberKV(idStr);
        if (!kv) continue;
        const c = Number(kv.clears || 0);
        const p = Number(kv.prophecyClears || 0);
        total += Number(c || 0);
        totalProphecy += Number(p || 0);

        const memberEntry = {
          membershipId: idStr,
          membershipType: Number(m.membershipType || 0),
          clears: Number(c || 0),
          prophecyClears: Number(p || 0),
          lastActivityAt: kv.lastActivityAt || null,
          mostRecentActivity: kv.mostRecentActivity || null
        };

        // If the member has mostRecentActivity with instanceId, record it in instanceMap
        const mra = memberEntry.mostRecentActivity;
        if (mra && mra.instanceId && mra.period) {
          const inst = String(mra.instanceId);
          const ts = Date.parse(mra.period) || 0;
          const existing = instanceMap.get(inst);
          if (!existing) {
            instanceMap.set(inst, {
              periodTs: ts,
              activity: mra,
              membershipIds: new Set([idStr])
            });
          } else {
            // update latest periodTs if this occurrence is newer
            if (ts > (existing.periodTs || 0)) {
              existing.periodTs = ts;
              existing.activity = mra;
            }
            existing.membershipIds.add(idStr);
          }
        }

        // Track global latest solo as fallback
        if (!globalLatestSolo && mra && mra.period) {
          globalLatestSolo = { membershipId: idStr, activity: mra, periodTs: Date.parse(mra.period) || 0 };
        } else if (mra && mra.period) {
          const ts = Date.parse(mra.period) || 0;
          if (!globalLatestSolo || ts > (globalLatestSolo.periodTs || 0)) {
            globalLatestSolo = { membershipId: idStr, activity: mra, periodTs: ts };
          }
        }

        perMember.push(memberEntry);
      } catch (e) {
        console.warn('[GlobalStats] aggregation error for', m && m.membershipId, e && e.message ? e.message : e);
        continue;
      }
    }

    // Determine a mostRecentClanActivity:
    // 1) find all instanceIds with membershipIds.size >= 2, pick the one with largest periodTs
    // 2) if none, pick the latest overall (globalLatestSolo)
    // 3) if still none, pick a random per-member activity if available
    let chosen = null;
    let chosenTs = 0;
    for (const [inst, info] of instanceMap.entries()) {
      const count = (info.membershipIds && info.membershipIds.size) ? info.membershipIds.size : 0;
      if (count >= 2) {
        if (!chosen || (info.periodTs || 0) > chosenTs) {
          chosen = {
            instanceId: inst,
            activity: info.activity,
            membershipIds: Array.from(info.membershipIds),
            periodTs: info.periodTs || 0
          };
          chosenTs = info.periodTs || 0;
        }
      }
    }

    if (!chosen) {
      // fallback to latest solo (mostRecentActivity across members)
      if (globalLatestSolo && globalLatestSolo.activity) {
        chosen = {
          instanceId: globalLatestSolo.activity.instanceId || null,
          activity: globalLatestSolo.activity,
          membershipIds: globalLatestSolo.membershipId ? [globalLatestSolo.membershipId] : [],
          periodTs: globalLatestSolo.periodTs || 0,
          fallback: 'solo'
        };
      } else if (perMember.length) {
        // ultimate fallback: random pick
        for (const pm of perMember) {
          if (pm.mostRecentActivity && pm.mostRecentActivity.instanceId) {
            chosen = {
              instanceId: pm.mostRecentActivity.instanceId,
              activity: pm.mostRecentActivity,
              membershipIds: [pm.membershipId],
              periodTs: Date.parse(pm.mostRecentActivity.period || '') || 0,
              fallback: 'random'
            };
            break;
          }
        }
      }
    }

    const snapshot = {
      clears: total,
      prophecyClears: totalProphecy,
      perMember,
      memberCount: members.length,
      processedCount: perMember.length,
      fetchedAt: new Date().toISOString()
    };

    if (chosen && chosen.activity) {
      snapshot.mostRecentClanActivity = {
        instanceId: chosen.instanceId,
        activity: chosen.activity,
        membershipIds: Array.isArray(chosen.membershipIds) ? chosen.membershipIds : [],
        source: chosen.fallback ? chosen.fallback : 'multi-member',
        periodTs: chosen.periodTs || null
      };
    } else {
      snapshot.mostRecentClanActivity = null;
    }

    return snapshot;
  }

  async _writeClearsSnapshot(snapshot) {
    if (!this.env.BUNGIE_STATS) return;
    try {
      await this.env.BUNGIE_STATS.put('clears_snapshot', JSON.stringify(snapshot));
    } catch (e) {
      console.warn('[GlobalStats] KV write failed for clears_snapshot', e && e.message ? e.message : e);
    }
  }

  _sleep(ms) { return new Promise(resolve => setTimeout(resolve, ms)); }

  // update-members: write per-member KV entries (member:<id>), remove stale keys, enqueue new
  async _handleUpdateMembers({ force = false } = {}) {
    const last = (await this.state.storage.get('lastMembersUpdateAt')) || 0;
    const minMs = Number(this.env.MIN_MEMBERS_UPDATE_INTERVAL_MS || 0);
    const nowTs = Date.now();
    if (!force && minMs > 0 && nowTs - last < minMs) {
      return { ok: false, reason: 'rate_limited', nextAllowedAt: last + minMs };
    }
    await this.state.storage.put('lastMembersUpdateAt', nowTs);

    const raw = await this._fetchClanRosterRaw();
    let members = raw.map(r => this._mapBungieResultToMember(r));
    const fetchedAt = new Date().toISOString();

    try {
      members = await this._populateMemberEmblems(members);
    } catch (e) {
      console.warn('[GlobalStats] populateMemberEmblems failed', e && e.message ? e.message : e);
    }

    // Build a small member payload and persist per-member KV keys
    const memberMap = new Map();
    for (const m of members) {
      const id = String(m.membershipId);
      const small = {
        membershipId: id,
        membershipType: Number(m.membershipType || 0),
        supplementalDisplayName: m.supplementalDisplayName ?? '',
        isOnline: !!m.isOnline,
        joinDate: m.joinDate ?? null,
        memberType: m.memberType ?? null,
        emblemPath: m.emblemPath ?? null,
        emblemBackgroundPath: m.emblemBackgroundPath ?? null,
        emblemHash: m.emblemHash ?? null
      };
      memberMap.set(id, small);
    }

    if (this.env.BUNGIE_STATS) {
      try {
        // list existing member: keys
        const listed = await this.env.BUNGIE_STATS.list({ prefix: 'member:' });
        const existingKeys = (listed && listed.keys) ? listed.keys.map(k => k.name) : [];
        const existingIds = new Set(existingKeys.map(k => k.replace(/^member:/, '')));

        const newIds = new Set(Array.from(memberMap.keys()));

        // write or update member:<id> where necessary
        for (const [id, small] of memberMap.entries()) {
          const keyName = `member:${id}`;
          if (!existingIds.has(id)) {
            // new member - write key
            await this.env.BUNGIE_STATS.put(keyName, JSON.stringify(small));
            console.log('[GlobalStats] wrote member KV', keyName);
          } else {
            // existing - check if content differs (fast path: read and compare)
            try {
              const raw = await this.env.BUNGIE_STATS.get(keyName);
              const prev = raw ? JSON.parse(raw) : null;
              // shallow compare essential fields
              if (!prev || prev.supplementalDisplayName !== small.supplementalDisplayName || prev.emblemPath !== small.emblemPath || prev.joinDate !== small.joinDate) {
                await this.env.BUNGIE_STATS.put(keyName, JSON.stringify(small));
                console.log('[GlobalStats] updated member KV', keyName);
              }
            } catch (e) {
              // If read fails, attempt to write to ensure presence
              await this.env.BUNGIE_STATS.put(keyName, JSON.stringify(small));
            }
          }
        }

        // delete keys for members no longer present
        for (const existingKey of existingKeys) {
          const id = existingKey.replace(/^member:/, '');
          if (!newIds.has(id)) {
            try {
              await this.env.BUNGIE_STATS.delete(existingKey);
              console.log('[GlobalStats] deleted stale member KV', existingKey);
            } catch (e) {
              console.warn('[GlobalStats] failed deleting stale member key', existingKey, e && e.message ? e.message : e);
            }
          }
        }

        // Enqueue per-member job for newly added members (prevIds computed from existingKeys)
        const prevIds = new Set(existingKeys.map(k => k.replace(/^member:/, '')));
        for (const m of members) {
          const idStr = String(m.membershipId);
          if (!prevIds.has(idStr)) {
            this._dispatchMemberJob(m, { includeDeletedCharacters: true }).catch(() => {});
          }
        }
      } catch (e) {
        console.warn('[GlobalStats] members_list KV operations failed', e && e.message ? e.message : e);
      }
    }

    await this.state.storage.put('lastMembersFetchedAt', fetchedAt);
    return { ok: true, members, fetchedAt, memberCount: members.length };
  }

  // Full implementation of update-clears: dispatch per-member jobs or compute synchronously and write clears_snapshot
  async _handleUpdateClearsImpl({ userLimit = null, waitForCompletion = false, waitTimeoutMs = 120000, opts = {} } = {}) {
    // fetch roster to know which members to dispatch
    const raw = await this._fetchClanRosterRaw();
    const members = raw.map(r => this._mapBungieResultToMember(r));
    const fetchedAt = new Date().toISOString();

    const limit = userLimit ? Math.min(Number(userLimit), members.length) : members.length;
    let dispatched = 0;

    // Default opts: include deleted characters by default, forward fetchAll flag if provided
    const memberOpts = {
      includeDeletedCharacters: opts.includeDeletedCharacters ?? true,
      fetchAllActivitiesForCharacter: opts.fetchAllActivitiesForCharacter ?? false,
      pageSize: opts.pageSize,
      maxPages: opts.maxPages,
      retries: opts.retries,
      backoffBaseMs: opts.backoffBaseMs,
    };

    for (let i = 0; i < limit; i++) {
      const m = members[i];
      const ok = await this._dispatchMemberJob(m, memberOpts);
      if (ok) dispatched++;
      // optionally small delay between dispatches to avoid bursts
      if (this.env.MEMBER_DISPATCH_THROTTLE_MS) {
        await this._sleep(Number(this.env.MEMBER_DISPATCH_THROTTLE_MS));
      }
    }

    await this.state.storage.put('lastClearsDispatchedAt', fetchedAt);

    // If not waiting, return quickly with dispatched info
    if (!waitForCompletion) {
      return { ok: true, dispatched, totalMembers: members.length, fetchedAt };
    }

    // If waiting, two strategies:
    // 1) Poll per-member KV for results until timeout (best-effort)
    // 2) If polling does not find all results in time, fallback to computing directly
    const start = Date.now();
    const deadline = start + Number(waitTimeoutMs || 120000);
    const remainingMemberIds = members.slice(0, limit).map(m => String(m.membershipId));
    const found = new Set();

    // Poll loop
    while (Date.now() < deadline) {
      for (const id of remainingMemberIds) {
        if (found.has(id)) continue;
        const kv = await this._readMemberKV(id);
        if (kv && (typeof kv.clears !== 'undefined' || typeof kv.prophecyClears !== 'undefined')) {
          found.add(id);
        }
      }
      if (found.size >= remainingMemberIds.length) break;
      // small backoff between polls
      await this._sleep(1500);
    }

    // If we found all per-member KV entries, aggregate them.
    if (found.size >= remainingMemberIds.length) {
      const { clears, prophecyClears, perMember } = await this._aggregateFromMemberKV(members.slice(0, limit));
      const snapshot = {
        fetchedAt: new Date().toISOString(),
        source: 'per-member-kv',
        clears,
        prophecyClears,
        perMember,
        memberCount: members.length,
        processedCount: perMember.length
      };

      // Persist snapshot to KV
      await this._writeClearsSnapshot(snapshot);
      await this.state.storage.put('lastClearsFetchedAt', snapshot.fetchedAt);

      return { ok: true, dispatched, totalMembers: members.length, fetchedAt: snapshot.fetchedAt, snapshot };
    }

    // If we didn't find all results before timeout, compute synchronously directly (fallback)
    try {
      // Run server-side compute across the members slice using computeClearsForMembers (streaming, server-side)
      const computeOpts = {
        userLimit: limit,
        concurrency: Number(this.env.BUNGIE_FETCH_CONCURRENCY || 2),
        pageSize: memberOpts.pageSize,
        maxPages: memberOpts.maxPages,
        timeoutMs: Number(this.env.BUNGIE_PER_REQUEST_TIMEOUT_MS || 8000),
        retries: memberOpts.retries,
        backoffBaseMs: memberOpts.backoffBaseMs,
        includeDeletedCharacters: memberOpts.includeDeletedCharacters,
        fetchAllActivitiesForCharacter: memberOpts.fetchAllActivitiesForCharacter
      };

      const result = await computeClearsForMembers(members, this.env, computeOpts);

      // Normalize snapshot
      const snapshot = {
        fetchedAt: result.fetchedAt || new Date().toISOString(),
        source: 'server-compute-fallback',
        clears: Number(result.clears || 0),
        prophecyClears: Number(result.prophecyClears || 0),
        perMember: Array.isArray(result.perMember) ? result.perMember : [],
        memberCount: members.length,
        processedCount: Array.isArray(result.perMember) ? result.perMember.length : 0
      };

      // Persist to KV
      await this._writeClearsSnapshot(snapshot);
      await this.state.storage.put('lastClearsFetchedAt', snapshot.fetchedAt);

      return { ok: true, dispatched, totalMembers: members.length, fetchedAt: snapshot.fetchedAt, snapshot };
    } catch (err) {
      console.error('[GlobalStats] synchronous compute failed', err && (err.message || err));
      // If sync compute fails, still return what we have (best-effort)
      const { clears, prophecyClears, perMember } = await this._aggregateFromMemberKV(members.slice(0, limit));
      const snapshot = {
        fetchedAt: new Date().toISOString(),
        source: 'partial-aggregate',
        clears,
        prophecyClears,
        perMember,
        memberCount: members.length,
        processedCount: perMember.length
      };
      await this._writeClearsSnapshot(snapshot);
      await this.state.storage.put('lastClearsFetchedAt', snapshot.fetchedAt);
      return { ok: true, dispatched, totalMembers: members.length, fetchedAt: snapshot.fetchedAt, snapshot, warning: 'sync_compute_failed' };
    }
  }

  async _handleDebug() {
    const lastMembers = await this.state.storage.get('lastMembersFetchedAt') || null;
    const lastClearsDispatched = await this.state.storage.get('lastClearsDispatchedAt') || null;
    const lastClearsFetchedAt = await this.state.storage.get('lastClearsFetchedAt') || null;
    return { lastMembers, lastClearsDispatched, lastClearsFetchedAt };
  }

  // ---- fetch entrypoint ----
  async fetch(request) {
    const url = new URL(request.url);
    const path = url.pathname.replace(/^\//, '') || '';
    const method = request.method;

    if (method === 'POST' && path === 'update-members') {
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

    if (method === 'POST' && path === 'update-clears') {
      try {
        let body = null;
        try { body = await request.json().catch(() => null); } catch (e) { body = null; }
        const userLimit = body && body.userLimit ? Number(body.userLimit) : null;
        const waitForCompletion = Boolean(body && body.waitForCompletion);
        const waitTimeoutMs = body && body.waitTimeoutMs ? Number(body.waitTimeoutMs) : 120000;
        const opts = body && body.opts ? body.opts : {};
        const out = await this._handleUpdateClearsImpl({ userLimit, waitForCompletion, waitTimeoutMs, opts });
        return new Response(JSON.stringify(out), { headers: { 'Content-Type': 'application/json' }});
      } catch (e) {
        const body = { ok: false, reason: e.code || 'fetch_failed', error: e.message || String(e) };
        if (e.status) body.status = e.status;
        return new Response(JSON.stringify(body), { status: 502, headers: { 'Content-Type': 'application/json' }});
      }
    }

    // GET /clears-snapshot -> return cached snapshot from KV if present
    if (method === 'GET' && path === 'clears-snapshot') {
      if (!this.env.BUNGIE_STATS) return new Response(JSON.stringify({ ok: false, reason: 'no_kv' }), { status: 404, headers: { 'Content-Type': 'application/json' }});
      try {
        const raw = await this.env.BUNGIE_STATS.get('clears_snapshot');
        if (!raw) return new Response(JSON.stringify({ ok: false, reason: 'no_snapshot' }), { status: 404, headers: { 'Content-Type': 'application/json' }});
        const parsed = JSON.parse(raw);
        return new Response(JSON.stringify(parsed), { headers: { 'Content-Type': 'application/json' }});
      } catch (e) {
        return new Response(JSON.stringify({ ok: false, reason: 'kv_read_failed', error: e && e.message ? e.message : String(e) }), { status: 500, headers: { 'Content-Type': 'application/json' }});
      }
    }

    // GET /debug
    if (method === 'GET' && path === 'debug') {
      const out = await this._handleDebug();
      return new Response(JSON.stringify(out), { headers: { 'Content-Type': 'application/json' }});
    }

    return new Response('Not found', { status: 404 });
  }
}