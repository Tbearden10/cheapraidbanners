// Durable Object: GlobalStats
// - Updated to dispatch per-character jobs in update-clears instead of per-member.
// - Aggregation reads character_clears:{membershipId}:{characterId} keys and folds them into per-member results.

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

  // ----- emblem fetching helpers (unchanged) -----
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

  async _dispatchCharacterJob(membershipId, membershipType, characterId, opts = {}) {
    try {
      const idStr = String(membershipId);
      const memberDoId = this.env.MEMBER_WORKER.idFromName(`member-${idStr}`);
      const memberStub = this.env.MEMBER_WORKER.get(memberDoId);
      const body = JSON.stringify({
        membershipId: idStr,
        membershipType: Number(membershipType || 0),
        characterId: String(characterId),
        opts,
      });
      memberStub.fetch('https://member.local/process', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body,
      }).then(async (res) => {
        try {
          const text = await res.text().catch(() => null);
          console.log('[GlobalStats] character job ack', `${idStr}:${characterId}`, res.status, text && String(text).slice(0,200));
        } catch (e) {}
      }).catch((err) => {
        console.warn('[GlobalStats] character job dispatch failed for', `${idStr}:${characterId}`, err && err.message ? err.message : err);
      });
      return true;
    } catch (err) {
      console.warn('[GlobalStats] dispatch error', err && (err.message || err));
      return false;
    }
  }

  async _readCharacterKV(membershipId, characterId) {
    if (!this.env.BUNGIE_STATS) return null;
    try {
      const raw = await this.env.BUNGIE_STATS.get(`character_clears:${membershipId}:${characterId}`);
      if (!raw) return null;
      return JSON.parse(raw);
    } catch (e) {
      console.warn('[GlobalStats] KV read character failed', membershipId, characterId, e && e.message ? e.message : e);
      return null;
    }
  }

  async _aggregateFromCharacterKV(members) {
    // members: array of member objects { membershipId, membershipType, ... }
    // We'll read all character_clears keys and aggregate per-member.
    const perMember = [];
    let total = 0;
    let totalProphecy = 0;

    // We'll also build instanceMap to find mostRecentClanActivity (same logic as before)
    const instanceMap = new Map();
    let globalLatestSolo = null;

    for (const m of members) {
      try {
        const idStr = String(m.membershipId);
        // list character keys for this membershipId
        // NOTE: Workers KV doesn't support efficient hierarchical listing; we'll attempt to list with prefix
        if (!this.env.BUNGIE_STATS) continue;
        const listed = await this.env.BUNGIE_STATS.list({ prefix: `character_clears:${idStr}:` });
        const keys = (listed && listed.keys) ? listed.keys.map(k => k.name) : [];
        // If no character-level keys exist, fall back to member_clears:<id> if present
        if (!keys.length) {
          try {
            const rawMember = await this.env.BUNGIE_STATS.get(`member_clears:${idStr}`);
            if (rawMember) {
              const kv = JSON.parse(rawMember);
              const c = Number(kv.clears || 0);
              const p = Number(kv.prophecyClears || 0);
              total += c;
              totalProphecy += p;
              perMember.push({
                membershipId: idStr,
                membershipType: Number(m.membershipType || 0),
                clears: c,
                prophecyClears: p,
                lastActivityAt: kv.lastActivityAt || null,
                mostRecentActivity: kv.mostRecentActivity || null
              });
              // record instance if present
              const mra = kv.mostRecentActivity;
              if (mra && mra.instanceId && mra.period) {
                const inst = String(mra.instanceId);
                const ts = Date.parse(mra.period) || 0;
                const existing = instanceMap.get(inst);
                if (!existing) {
                  instanceMap.set(inst, { periodTs: ts, activity: mra, membershipIds: new Set([idStr]) });
                } else {
                  if (ts > (existing.periodTs || 0)) { existing.periodTs = ts; existing.activity = mra; }
                  existing.membershipIds.add(idStr);
                }
              }
              continue;
            }
          } catch (e) {
            // swallow and continue
          }
        }

        // accumulate across character keys
        let memberClears = 0;
        let memberProphecyClears = 0;
        let memberLastActivity = null;
        let memberMostRecentActivity = null;

        for (const key of keys) {
          try {
            const raw = await this.env.BUNGIE_STATS.get(key);
            if (!raw) continue;
            const kv = JSON.parse(raw);
            const c = Number(kv.clears || 0);
            const p = Number(kv.prophecyClears || 0);
            memberClears += c;
            memberProphecyClears += p;
            // pick latest character lastActivity
            if (kv.lastActivityAt) {
              if (!memberLastActivity || Date.parse(kv.lastActivityAt) > Date.parse(memberLastActivity)) {
                memberLastActivity = kv.lastActivityAt;
              }
            }
            // pick most recentActivity across characters
            if (kv.mostRecentActivity && kv.mostRecentActivity.period) {
              if (!memberMostRecentActivity || Date.parse(kv.mostRecentActivity.period) > Date.parse(memberMostRecentActivity.period)) {
                memberMostRecentActivity = kv.mostRecentActivity;
              }
            }
            // add to instanceMap if character-level activity contains instanceId
            const mra = kv.mostRecentActivity;
            if (mra && mra.instanceId && mra.period) {
              const inst = String(mra.instanceId);
              const ts = Date.parse(mra.period) || 0;
              const existing = instanceMap.get(inst);
              if (!existing) {
                instanceMap.set(inst, { periodTs: ts, activity: mra, membershipIds: new Set([idStr]) });
              } else {
                if (ts > (existing.periodTs || 0)) { existing.periodTs = ts; existing.activity = mra; }
                existing.membershipIds.add(idStr);
              }
            }
          } catch (e) {
            // skip malformed
          }
        }

        total += memberClears;
        totalProphecy += memberProphecyClears;
        perMember.push({
          membershipId: idStr,
          membershipType: Number(m.membershipType || 0),
          clears: memberClears,
          prophecyClears: memberProphecyClears,
          lastActivityAt: memberLastActivity,
          mostRecentActivity: memberMostRecentActivity
        });

        // update globalLatestSolo
        if (memberMostRecentActivity && memberMostRecentActivity.period) {
          const ts = Date.parse(memberMostRecentActivity.period) || 0;
          if (!globalLatestSolo || ts > (globalLatestSolo.periodTs || 0)) {
            globalLatestSolo = { membershipId: idStr, activity: memberMostRecentActivity, periodTs: ts };
          }
        }
      } catch (e) {
        console.warn('[GlobalStats] aggregation error for', m && m.membershipId, e && e.message ? e.message : e);
        continue;
      }
    }

    // Decide mostRecentClanActivity using instanceMap (prefer multi-member instances)
    let chosen = null;
    let chosenTs = 0;
    for (const [inst, info] of instanceMap.entries()) {
      const count = (info.membershipIds && info.membershipIds.size) ? info.membershipIds.size : 0;
      if (count >= 2) {
        if (!chosen || (info.periodTs || 0) > chosenTs) {
          chosen = { instanceId: inst, activity: info.activity, membershipIds: Array.from(info.membershipIds), periodTs: info.periodTs || 0 };
          chosenTs = info.periodTs || 0;
        }
      }
    }

    if (!chosen) {
      if (globalLatestSolo && globalLatestSolo.activity) {
        chosen = {
          instanceId: globalLatestSolo.activity.instanceId || null,
          activity: globalLatestSolo.activity,
          membershipIds: globalLatestSolo.membershipId ? [globalLatestSolo.membershipId] : [],
          periodTs: globalLatestSolo.periodTs || 0,
          fallback: 'solo'
        };
      } else if (perMember.length) {
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

  // update-members: same as before (writes member:<id> keys)
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

    // Build small member payload and persist keys
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
        const listed = await this.env.BUNGIE_STATS.list({ prefix: 'member:' });
        const existingKeys = (listed && listed.keys) ? listed.keys.map(k => k.name) : [];
        const existingIds = new Set(existingKeys.map(k => k.replace(/^member:/, '')));
        const newIds = new Set(Array.from(memberMap.keys()));

        for (const [id, small] of memberMap.entries()) {
          const keyName = `member:${id}`;
          if (!existingIds.has(id)) {
            await this.env.BUNGIE_STATS.put(keyName, JSON.stringify(small));
            console.log('[GlobalStats] wrote member KV', keyName);
          } else {
            try {
              const raw = await this.env.BUNGIE_STATS.get(keyName);
              const prev = raw ? JSON.parse(raw) : null;
              if (!prev || prev.supplementalDisplayName !== small.supplementalDisplayName || prev.emblemPath !== small.emblemPath || prev.joinDate !== small.joinDate) {
                await this.env.BUNGIE_STATS.put(keyName, JSON.stringify(small));
                console.log('[GlobalStats] updated member KV', keyName);
              }
            } catch (e) {
              await this.env.BUNGIE_STATS.put(keyName, JSON.stringify(small));
            }
          }
        }

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
      } catch (e) {
        console.warn('[GlobalStats] members_list KV operations failed', e && e.message ? e.message : e);
      }
    }

    await this.state.storage.put('lastMembersFetchedAt', fetchedAt);
    return { ok: true, members, fetchedAt, memberCount: members.length };
  }

  // update-clears: dispatch one job per character (faster per-job)
  async _handleUpdateClearsImpl({ userLimit = null, waitForCompletion = false, waitTimeoutMs = 120000, opts = {} } = {}) {
    // fetch roster to know which members to dispatch
    const raw = await this._fetchClanRosterRaw();
    const members = raw.map(r => this._mapBungieResultToMember(r));
    const fetchedAt = new Date().toISOString();

    const limit = userLimit ? Math.min(Number(userLimit), members.length) : members.length;
    let dispatched = 0;

    // Default opts forwarded to character-level jobs
    const jobOpts = {
      includeDeletedCharacters: opts.includeDeletedCharacters ?? true,
      fetchAllActivitiesForCharacter: opts.fetchAllActivitiesForCharacter ?? false,
      pageSize: opts.pageSize,
      maxPages: opts.maxPages,
      retries: opts.retries,
      backoffBaseMs: opts.backoffBaseMs,
    };

    // For each member, enumerate characters (prefer Stats endpoint), then dispatch one job per character
    for (let i = 0; i < limit; i++) {
      const m = members[i];
      const membershipId = String(m.membershipId);
      const membershipType = Number(m.membershipType || 0);

      // enumerate character IDs for this member
      let characterIds = [];
      try {
        // prefer the account Stats endpoint
        const stats = await fetch(`https://www.bungie.net/Platform/Destiny2/${membershipType}/Account/${encodeURIComponent(membershipId)}/Stats/`, { headers: this.env.BUNGIE_API_KEY ? { 'X-API-Key': this.env.BUNGIE_API_KEY } : undefined });
        if (stats && stats.ok) {
          const payload = await stats.json().catch(() => null);
          const rawChars = payload?.Response?.characters ?? null;
          if (Array.isArray(rawChars)) characterIds = rawChars.map(c => String(c.characterId));
          else if (rawChars && typeof rawChars === 'object') characterIds = Object.keys(rawChars).map(k => String(k));
        }
      } catch (e) {
        // fallback to profile endpoint (simpler)
        try {
          const profRes = await fetch(`https://www.bungie.net/Platform/Destiny2/${membershipType}/Profile/${encodeURIComponent(membershipId)}?components=200`, { headers: this.env.BUNGIE_API_KEY ? { 'X-API-Key': this.env.BUNGIE_API_KEY } : undefined });
          if (profRes && profRes.ok) {
            const payload = await profRes.json().catch(() => null);
            const chars = payload?.Response?.characters?.data ?? {};
            characterIds = Object.keys(chars || {});
          }
        } catch (e2) {
          // give up for this member
          characterIds = [];
        }
      }

      if (!Array.isArray(characterIds) || characterIds.length === 0) {
        // If no character ids, still dispatch a member-level job for compatibility
        const ok = await this._dispatchCharacterJob(membershipId, membershipType, '', jobOpts);
        if (ok) dispatched++;
        continue;
      }

      for (const cid of characterIds) {
        const ok = await this._dispatchCharacterJob(membershipId, membershipType, cid, jobOpts);
        if (ok) dispatched++;
        // throttle between dispatches
        if (this.env.MEMBER_DISPATCH_THROTTLE_MS) await this._sleep(Number(this.env.MEMBER_DISPATCH_THROTTLE_MS));
      }
    }

    await this.state.storage.put('lastClearsDispatchedAt', fetchedAt);

    if (!waitForCompletion) {
      return { ok: true, dispatched, totalMembers: members.length, fetchedAt };
    }

    // Wait for character-level KV results
    const start = Date.now();
    const deadline = start + Number(waitTimeoutMs || 120000);

    // Build list of expected character keys
    const expectedKeys = [];
    for (let i = 0; i < limit; i++) {
      const m = members[i];
      const membershipId = String(m.membershipId);
      // try to list characters again quickly (best-effort)
      try {
        const listed = await this.env.BUNGIE_STATS.list({ prefix: `character_clears:${membershipId}:` });
        const keys = (listed && listed.keys) ? listed.keys.map(k => k.name) : [];
        if (keys.length) {
          for (const k of keys) expectedKeys.push(k);
        } else {
          // fall back to member-level key expectation if no character keys found
          expectedKeys.push(`member_clears:${membershipId}`);
        }
      } catch (e) {
        expectedKeys.push(`member_clears:${membershipId}`);
      }
    }

    const found = new Set();
    // Poll loop
    while (Date.now() < deadline) {
      for (const key of expectedKeys) {
        if (found.has(key)) continue;
        try {
          const raw = await this.env.BUNGIE_STATS.get(key);
          if (raw) found.add(key);
        } catch (e) {
          // ignore KV read errors
        }
      }
      if (found.size >= expectedKeys.length) break;
      await this._sleep(1500);
    }

    // Aggregate from character-level KV where available (fallback to member-level)
    const snapshot = await this._aggregateFromCharacterKV(members.slice(0, limit));
    snapshot.source = 'character-per-job';
    // Persist
    await this._writeClearsSnapshot(snapshot);
    await this.state.storage.put('lastClearsFetchedAt', snapshot.fetchedAt);

    return { ok: true, dispatched, totalMembers: members.length, fetchedAt: snapshot.fetchedAt, snapshot };
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
        console.warn('[GlobalStats] KV read clears_snapshot failed', e && e.message ? e.message : e);
        return new Response(JSON.stringify({ ok: false, reason: 'kv_read_failed', error: String(e) }), { status: 500, headers: { 'Content-Type': 'application/json' }});
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