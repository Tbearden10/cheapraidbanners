// stats.js - FIXED: Properly handles backend response format
// Backend returns: { members: [...], aggregateStats: [...], memberCount, fetchedAt }

let previousStatsData = null;

/**
 * Get API base URL - evaluated at runtime, not module load time
 */
function getApiBase() {
  return window.__utils?.API_BASE || window.API_BASE || 'https://api.cheapraidbanners.com';
}

/**
 * Normalize backend /stats response
 * Backend format:
 * {
 *   members: [{ membershipId, displayName, stats: [{ dungeonHash, totalFullClears, totalPlaytimeSeconds }] }],
 *   aggregateStats: [{ dungeon_hash, total_full_clears, total_playtime_seconds }],
 *   memberCount,
 *   fetchedAt
 * }
 */
function normalizeStatsResponse(resp) {
  if (!resp) return null;

  console.log('[Stats] Normalizing response:', resp);

  // 1) Get totals from aggregateStats where dungeon_hash === 'all'
  let totalFull = 0;
  let totalPlay = 0;
  
  if (Array.isArray(resp.aggregateStats) && resp.aggregateStats.length > 0) {
    const allRow = resp.aggregateStats.find(r => String(r.dungeon_hash) === 'all');
    if (allRow) {
      totalFull = Number(allRow.total_full_clears || 0);
      totalPlay = Number(allRow.total_playtime_seconds || 0);
      console.log('[Stats] Found "all" aggregate:', { totalFull, totalPlay });
    } else {
      // Sum all dungeons if no "all" row
      for (const r of resp.aggregateStats) {
        if (r.dungeon_hash !== 'all') {
          totalFull += Number(r.total_full_clears || 0);
          totalPlay += Number(r.total_playtime_seconds || 0);
        }
      }
      console.log('[Stats] Summed from individual dungeons:', { totalFull, totalPlay });
    }
  }

  // 2) Build per-member stats from members array
  const perMember = [];
  
  if (Array.isArray(resp.members) && resp.members.length > 0) {
    for (const m of resp.members) {
      const membershipId = String(m.membershipId || '');
      if (!membershipId) continue;

      let memberFull = 0;
      let memberPlay = 0;
      const statsArr = Array.isArray(m.stats) ? m.stats : [];

      // Sum up member's stats across all dungeons
      for (const s of statsArr) {
        memberFull += Number(s.totalFullClears || 0);
        memberPlay += Number(s.totalPlaytimeSeconds || 0);
      }

      perMember.push({
        membershipId,
        totalFullClears: memberFull,
        totalPlaytimeSeconds: memberPlay,
        stats: statsArr, // Keep full stats array
        lastProcessedDate: m.lastProcessedDate || null
      });
    }
  }

  console.log('[Stats] Normalized:', {
    dungeonClears: totalFull,
    totalPlaytimeSeconds: totalPlay,
    perMemberCount: perMember.length
  });

  if (perMember.length > 0) {
    console.log('[Stats] First member sample:', perMember[0]);
  }

  const memberCount = Number(resp.memberCount || perMember.length);
  const fetchedAt = resp.fetchedAt || new Date().toISOString();

  return {
    dungeonClears: totalFull,
    totalPlaytimeSeconds: totalPlay,
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

  console.log('[Stats] Rendering:', {
    clears: data.dungeonClears,
    playtime: data.totalPlaytimeSeconds
  });

  if (dungeonEl && typeof data.dungeonClears !== 'undefined') {
    if (window.__utils?.animateCounter) {
      window.__utils.animateCounter(dungeonEl, data.dungeonClears);
    } else {
      dungeonEl.textContent = String(data.dungeonClears);
    }
  }

  if (playtimeEl && typeof data.totalPlaytimeSeconds !== 'undefined') {
    const hours = Math.floor(data.totalPlaytimeSeconds / 3600);
    if (window.__utils?.animateCounter) {
      window.__utils.animateCounter(playtimeEl, hours);
    } else {
      playtimeEl.textContent = String(hours) + 'h';
    }

    // Add 'h' suffix after animation completes
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
 * loadStats - fetch /stats from server, normalize, render
 */
async function loadStats(forceRender = false) {
  const API_BASE = getApiBase();
  const statsUrl = new URL('/stats', API_BASE).toString();
  
  console.log('[Stats] Fetching from:', statsUrl);
  
  const raw = await (window.__utils?.fetchJson 
    ? window.__utils.fetchJson(statsUrl) 
    : fetch(statsUrl).then(r => r.ok ? r.json().catch(()=>null) : null).catch(()=>null)
  );

  if (!raw) {
    console.error('[Stats] Failed to fetch stats');
    return null;
  }

  console.log('[Stats] Raw response:', raw);

  const normalized = normalizeStatsResponse(raw);
  renderStatsLocal(normalized, forceRender);
  return normalized;
}

// Expose for app usage
window.loadStats = loadStats;
window.renderStats = renderStatsLocal;