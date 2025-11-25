// stats.js - robust loader + normalizer for backend /stats response
// Produces a stable shape used by the UI:
// {
//   dungeonClears: number,
//   totalPlaytimeSeconds: number,
//   perMember: [{ membershipId: string, totalFullClears: number, totalPlaytimeSeconds: number, stats: [...], lastProcessedDate }],
//   memberCount: number,
//   fetchedAt: string
// }

let previousStatsData = null;
const API_BASE = window.__utils?.API_BASE || 'https://api.cheapraidbanners.com';

/**
 * Normalize a backend /stats response into the UI-friendly shape.
 * The backend may include:
 * - aggregateStats: [{ dungeon_hash, total_full_clears, total_playtime_seconds }, ...]
 * - members: [{ membershipId, stats: [{ dungeonHash, totalFullClears, totalPlaytimeSeconds, instanceId }, ...] }, ...]
 * - memberCount, fetchedAt
 */
function normalizeStatsResponse(resp) {
  if (!resp) return null;

  console.log('[Stats] Normalizing response:', resp);

  // 1) overall aggregate: prefer a row where dungeon_hash === 'all', otherwise sum
  let totalFull = 0;
  let totalPlay = 0;
  if (Array.isArray(resp.aggregateStats) && resp.aggregateStats.length > 0) {
    const allRow = resp.aggregateStats.find(r => String(r.dungeon_hash) === 'all' || r.dungeon_hash === 'all');
    if (allRow) {
      totalFull = Number(allRow.total_full_clears ?? allRow.totalFullClears ?? 0);
      totalPlay = Number(allRow.total_playtime_seconds ?? allRow.totalPlaytimeSeconds ?? 0);
    } else {
      for (const r of resp.aggregateStats) {
        totalFull += Number(r.total_full_clears ?? r.totalFullClears ?? 0);
        totalPlay += Number(r.total_playtime_seconds ?? r.totalPlaytimeSeconds ?? 0);
      }
    }
  }

  // 2) per-member aggregates: if backend provides members with stats arrays, preserve them
  const perMember = [];
  if (Array.isArray(resp.members) && resp.members.length > 0) {
    for (const m of resp.members) {
      // membership id may be membershipId or membership_id
      const membershipId = String(m.destinyUserInfo.membershipId ?? m.membership_id ?? '');
      let memberFull = 0;
      let memberPlay = 0;

      // prefer nested stats array on member (member.stats) if present
      const statsArr = Array.isArray(m.stats) ? m.stats : Array.isArray(m.memberStats) ? m.memberStats : [];
      
      // Normalize each stat entry to handle both camelCase and snake_case
      const normalizedStats = statsArr.map(s => ({
        dungeonHash: s.dungeonHash ?? s.dungeon_hash,
        totalFullClears: Number(s.totalFullClears ?? s.total_full_clears ?? 0),
        totalPlaytimeSeconds: Number(s.totalPlaytimeSeconds ?? s.total_playtime_seconds ?? 0),
        lastProcessedDate: s.lastProcessedDate ?? s.last_processed_date,
        instanceId: s.instanceId ?? s.instance_id // CRITICAL for activities!
      }));

      if (normalizedStats.length > 0) {
        for (const s of normalizedStats) {
          memberFull += s.totalFullClears;
          memberPlay += s.totalPlaytimeSeconds;
        }
      }

      perMember.push({
        membershipId,
        totalFullClears: memberFull,
        totalPlaytimeSeconds: memberPlay,
        stats: normalizedStats, // Keep the full normalized stats array
        lastProcessedDate: m.lastProcessedDate ?? m.last_processed_date ?? null
      });
    }
  }

  // 3) If backend provided a separate perMember list (flat), merge/add values
  if (Array.isArray(resp.perMember) && resp.perMember.length > 0) {
    const map = new Map(perMember.map(p => [p.membershipId, p]));
    for (const pm of resp.perMember) {
      const id = String(pm.membershipId ?? pm.membership_id ?? '');
      const existing = map.get(id);
      const full = Number(pm.totalFullClears ?? pm.total_full_clears ?? 0);
      const play = Number(pm.totalPlaytimeSeconds ?? pm.total_playtime_seconds ?? 0);
      const stats = pm.stats ?? pm.memberStats ?? [];
      if (existing) {
        existing.totalFullClears = existing.totalFullClears + full;
        existing.totalPlaytimeSeconds = existing.totalPlaytimeSeconds + play;
        // Merge stats arrays if present
        if (Array.isArray(stats) && stats.length > 0) {
          existing.stats = [...existing.stats, ...stats];
        }
      } else {
        map.set(id, { 
          membershipId: id, 
          totalFullClears: full, 
          totalPlaytimeSeconds: play,
          stats: stats || [],
          lastProcessedDate: pm.lastProcessedDate ?? pm.last_processed_date ?? null
        });
      }
    }
    // use mapped list
    perMember.length = 0;
    for (const v of map.values()) perMember.push(v);
  }

  console.log('[Stats] Normalized perMember count:', perMember.length);
  if (perMember.length > 0) {
    console.log('[Stats] First member sample:', perMember[0]);
  }

  // 4) If perMember is empty but aggregateStats exists and members list has memberCount,
  //    we still want to expose memberCount and fetchedAt
  const memberCount = Number(resp.memberCount ?? (Array.isArray(resp.members) ? resp.members.length : 0));

  const fetchedAt = resp.fetchedAt ?? new Date().toISOString();

  return {
    dungeonClears: Number(totalFull || 0),
    totalPlaytimeSeconds: Number(totalPlay || 0),
    perMember,
    memberCount,
    fetchedAt
  };
}

function renderStatsLocal(data, forceRender = false) {
  const dungeonEl = window.__utils?.$ ? window.__utils.$('dungeon-count') : document.getElementById('dungeon-count');
  const playtimeEl = window.__utils?.$ ? window.__utils.$('playtime') : document.getElementById('playtime');
  const updatedEl = window.__utils?.$ ? window.__utils.$('last-updated') : document.getElementById('last-updated');

  if (!forceRender && !window.__utils?.dataHasChanged(data, previousStatsData)) {
    console.log('[Stats] No changes detected, skipping render');
    return;
  }

  previousStatsData = data ? JSON.parse(JSON.stringify(data)) : null;

  if (!data) {
    if (dungeonEl) dungeonEl.textContent = '—';
    if (playtimeEl) playtimeEl.textContent = '—';
    if (updatedEl) updatedEl.textContent = 'Loading...';
    return;
  }

  if (dungeonEl && typeof data.dungeonClears !== 'undefined') {
    // animateCounter will format numbers
    if (window.__utils?.animateCounter) window.__utils.animateCounter(dungeonEl, data.dungeonClears);
    else dungeonEl.textContent = String(data.dungeonClears);
  }

  if (playtimeEl && typeof data.totalPlaytimeSeconds !== 'undefined') {
    const hours = Math.floor(data.totalPlaytimeSeconds / 3600);
    if (window.__utils?.animateCounter) window.__utils.animateCounter(playtimeEl, hours);
    else playtimeEl.textContent = String(hours) + 'h';

    // Add 'h' suffix after animation completes (defensive)
    setTimeout(() => {
      if (playtimeEl && playtimeEl.textContent && !playtimeEl.textContent.includes('h')) {
        playtimeEl.textContent += 'h';
      }
    }, 1250);
  }

  if (updatedEl && data.fetchedAt) {
    const newText = new Date(data.fetchedAt).toLocaleString();
    if (updatedEl.textContent !== newText) {
      updatedEl.textContent = newText;
    }
  }
}

/**
 * loadStats - fetch /stats from server, normalize into UI shape, render
 */
async function loadStats(forceRender = false) {
  // Attempt server API first
  const statsUrl = new URL('/stats', API_BASE).toString();
  let raw = await (window.__utils?.fetchJson ? window.__utils.fetchJson(statsUrl) : fetch(statsUrl).then(r => r.ok ? r.json().catch(()=>null) : null).catch(()=>null));
  if (!raw) {
    console.warn('[Stats] /stats returned no data, using client fallback');
    raw = await fetchStatsFallback();
  }

  // Normalize regardless of source (server or fallback)
  const normalized = normalizeStatsResponse(raw);

  renderStatsLocal(normalized, forceRender);
  return normalized;
}

/**
 * Minimal fallback if /stats is completely unavailable.
 * Returns a shape compatible with normalizeStatsResponse
 */
async function fetchStatsFallback() {
  // Minimal fallback: show member count and zeros for clears/playtime
  const membersUrl = new URL('/members', (window.__utils?.API_BASE || window.API_BASE || API_BASE || 'https://api.cheapraidbanners.com')).toString();
  const membersResp = await (window.__utils?.fetchJson ? window.__utils.fetchJson(membersUrl) : fetch(membersUrl).then(r => r.ok ? r.json().catch(()=>null) : null).catch(()=>null));
  const members = (membersResp && (membersResp.members || membersResp)) || null;
  if (!members || members.length === 0) {
    // try Bungie roster fallback
    const rosterResp = await fetch(`https://www.bungie.net/Platform/GroupV2/${encodeURIComponent(window.__utils?.CLAN_ID || window.CLAN_ID)}/Members/`, {
      headers: { 'X-API-Key': env.BUNGIE_API_KEY || window.__utils?.BUNGIE_API_KEY || window.BUNGIE_API_KEY }
    }).then(r => r.ok ? r.json().catch(() => null) : null).catch(() => null);
    members = rosterResp?.Response?.results?.map((r) => r.member?.destinyUserInfo) || [];
  }

  const memberCount = members ? members.length : 0;
  return {
    aggregateStats: [],
    members: members || [],
    perMember: [],
    memberCount,
    fetchedAt: new Date().toISOString()
  };
}

// expose for app usage
window.loadStats = loadStats;
window.renderStats = renderStatsLocal;