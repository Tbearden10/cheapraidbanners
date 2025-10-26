// Durable Object: MemberWorker
// - POST /process starts/resumes a job
// - GET  /status  returns stored job state (for observability)
// - jobRun persists progress and logs key steps to console so wrangler dev output shows activity

import { computePerMemberHistory } from './lib/activityHistory.js';
import { fetchAggregateActivityStats, computeClearsForMember } from './lib/bungieApi.js';

export class MemberWorker {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.jobKeyPrefix = 'job:';
  }

  async fetch(request) {
    const url = new URL(request.url);
    const path = url.pathname || '';
    // POST /process -> start/resume job
    if (request.method === 'POST' && path.endsWith('/process')) {
      try {
        const body = await request.json().catch(() => null);
        if (!body || !body.membershipId) {
          return new Response(JSON.stringify({ ok: false, error: 'missing_membershipId' }), { status: 400, headers: { 'Content-Type': 'application/json' }});
        }
        const membershipId = String(body.membershipId);
        const membershipType = Number(body.membershipType || 0);
        const opts = body.opts || {};

        const jobKey = `${this.jobKeyPrefix}${membershipId}`;
        const existing = (await this.state.storage.get(jobKey)) || {};
        const job = {
          membershipId,
          membershipType,
          opts,
          state: existing.state || 'pending',
          progress: existing.progress || { characters: body.characters || [], nextCharacterIndex: 0 },
          createdAt: existing.createdAt || new Date().toISOString(),
          lastUpdatedAt: new Date().toISOString()
        };
        await this.state.storage.put(jobKey, job);

        // Start background run (do not await so HTTP returns quickly)
        this.jobRun(jobKey).catch(err => console.error('[MemberWorker] jobRun unexpected error', err && err.message ? err.message : err));

        return new Response(JSON.stringify({ ok: true, queued: true }), { headers: { 'Content-Type': 'application/json' }});
      } catch (e) {
        return new Response(JSON.stringify({ ok: false, error: e && e.message ? e.message : String(e) }), { status: 500, headers: { 'Content-Type': 'application/json' }});
      }
    }

    // GET /status -> return job state for this DO
    if (request.method === 'GET' && path.endsWith('/status')) {
      try {
        const list = await this.state.storage.list();
        const jobs = [];
        for await (const entry of list) {
          try {
            const key = entry && (entry.key ?? entry.name ?? entry.id ?? (typeof entry === 'string' ? entry : undefined));
            if (!key || typeof key !== 'string') continue;
            if (!key.startsWith(this.jobKeyPrefix)) continue;
            const job = await this.state.storage.get(key);
            jobs.push(job);
          } catch (err) {
            console.error('[MemberWorker] status: failed processing storage entry', err && err.message ? err.message : err, entry);
          }
        }
        return new Response(JSON.stringify({ ok: true, jobs }), { headers: { 'Content-Type': 'application/json' }});
      } catch (e) {
        return new Response(JSON.stringify({ ok: false, error: e && e.message ? e.message : String(e) }), { status: 500, headers: { 'Content-Type': 'application/json' }});
      }
    }

    return new Response('Not Found', { status: 404 });
  }

  async alarm() {
    try {
      const list = await this.state.storage.list();
      for await (const entry of list) {
        try {
          const key = entry && (entry.key ?? entry.name ?? entry.id ?? (typeof entry === 'string' ? entry : undefined));
          if (!key || typeof key !== 'string') continue;
          if (!key.startsWith(this.jobKeyPrefix)) continue;
          const job = await this.state.storage.get(key);
          if (job && job.state !== 'done' && job.state !== 'running') {
            this.jobRun(key).catch(err => console.error('[MemberWorker] alarm jobRun err', err && err.message ? err.message : err));
          }
        } catch (entryErr) {
          console.error('[MemberWorker] alarm: failed processing storage entry', entryErr && entryErr.message ? entryErr.message : entryErr, entry);
        }
      }
    } catch (e) {
      console.error('[MemberWorker] alarm scan failed', e && e.message ? e.message : e);
    }
  }

  // Core job runner — persists progress and logs important milestones
  async jobRun(jobKey) {
    const job = (await this.state.storage.get(jobKey)) || null;
    if (!job) {
      console.log('[MemberWorker] jobRun: no job found for', jobKey);
      return;
    }

    // simple lock/guard against concurrent runners
    if (job.lockedAt && (Date.now() - new Date(job.lockedAt).getTime()) < 1000 * 60 * 10) {
      console.log('[MemberWorker] jobRun: already locked recently for', job.membershipId);
      return;
    }
    job.lockedAt = new Date().toISOString();
    job.state = 'running';
    job.lastUpdatedAt = new Date().toISOString();
    await this.state.storage.put(jobKey, job);

    console.log('[MemberWorker] START job', job.membershipId, 'opts=', job.opts || {});

    try {
      const membershipId = job.membershipId;
      const membershipType = Number(job.membershipType || 0);
      const opts = job.opts || {};

      // Resolve characters: use profile as fallback (we do minimal here; computeClearsForMember also enumerates)
      let characters = Array.isArray(job.progress?.characters) && job.progress.characters.length ? job.progress.characters : [];
      if (!characters.length) {
        try {
          const profileUrl = `https://www.bungie.net/Platform/Destiny2/${membershipType}/Profile/${membershipId}/?components=200`;
          const headers = { Accept: 'application/json' };
          if (this.env.BUNGIE_API_KEY) headers['X-API-Key'] = this.env.BUNGIE_API_KEY;
          const res = await fetch(profileUrl, { method: 'GET', headers });
          if (res && res.ok) {
            const json = await res.json().catch(() => null);
            const chars = json?.Response?.characters?.data || {};
            characters = Object.keys(chars);
            console.log('[MemberWorker] fetched characters for', membershipId, 'count=', characters.length);
          }
        } catch (e) {
          console.warn('[MemberWorker] profile fetch failed for', membershipId, e && e.message ? e.message : e);
        }
      }

      if (characters.length && (!job.progress || !Array.isArray(job.progress.characters) || job.progress.characters.length === 0)) {
        job.progress = job.progress || {};
        job.progress.characters = characters;
        job.progress.nextCharacterIndex = 0;
        await this.state.storage.put(jobKey, job);
      }

      // Run computeClearsForMember (heavy operation)
      console.log('[MemberWorker] computing clears for', membershipId);
      const result = await computeClearsForMember({ membershipId, membershipType }, this.env, opts);
      console.log('[MemberWorker] computeClearsForMember DONE for', membershipId, 'clears=', result.clears, 'prophecy=', result.prophecyClears);

      // write per-member KV (member_clears:<membershipId>)
      if (this.env.BUNGIE_STATS) {
        const kvKey = `member_clears:${membershipId}`;
        // include mostRecentActivity if present (computeClearsForMember now returns it)
        const out = { ...result, fetchedAt: new Date().toISOString() };
        try {
          await this.env.BUNGIE_STATS.put(kvKey, JSON.stringify(out));
          console.log('[MemberWorker] wrote KV', kvKey, 'mostRecentActivity=', !!out.mostRecentActivity);
        } catch (e) {
          console.error('[MemberWorker] KV write failed for', membershipId, e && e.message ? e.message : e);
          // schedule retry
          job.state = 'pending';
          job.error = e && e.message ? e.message : String(e);
          job.lastUpdatedAt = new Date().toISOString();
          await this.state.storage.put(jobKey, job);
          await this.state.storage.setAlarm(Date.now() + Number(this.env.MEMBER_RETRY_ALARM_MS || 15000));
          return;
        }
      } else {
        console.warn('[MemberWorker] no KV namespace bound; skipping member_clears write for', membershipId);
      }

      // mark done
      job.state = 'done';
      job.result = result;
      job.completedAt = new Date().toISOString();
      job.lastUpdatedAt = new Date().toISOString();
      await this.state.storage.put(jobKey, job);
      console.log('[MemberWorker] COMPLETED job', membershipId);
      return;
    } catch (err) {
      console.error('[MemberWorker] jobRun fatal for', job.membershipId, err && err.message ? err.message : err);
      // mark pending and retry later
      try {
        const j = await this.state.storage.get(jobKey);
        if (j) {
          j.state = 'pending';
          j.error = err && err.message ? err.message : String(err);
          j.lastUpdatedAt = new Date().toISOString();
          await this.state.storage.put(jobKey, j);
          await this.state.storage.setAlarm(Date.now() + Number(this.env.MEMBER_RETRY_ALARM_MS || 15000));
        }
      } catch (err2) {
        console.error('[MemberWorker] failed storing error state', err2 && err2.message ? err2.message : err2);
      }
      return;
    } finally {
      // remove transient lock
      try {
        const j = await this.state.storage.get(jobKey);
        if (j) { delete j.lockedAt; await this.state.storage.put(jobKey, j); }
      } catch (e) {}
    }
  }
}