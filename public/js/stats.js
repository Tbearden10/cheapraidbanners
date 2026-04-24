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

  // 1) Get totals from clanStats
  let totalFull = 0;
  let totalPlay = 0;
  if (resp.clanStats) {
    totalFull = Number(resp.clanStats.totalFullClears || 0);
    totalPlay = Number(resp.clanStats.totalPlaytimeSeconds || 0);
  }

  // 2) Build per-member stats from memberStats array
  const perMember = [];
  if (Array.isArray(resp.memberStats) && resp.memberStats.length > 0) {
    for (const m of resp.memberStats) {
      const membershipId = String(m.membershipId || '');
      if (!membershipId) continue;
      perMember.push({
        membershipId,
        displayName: m.displayName || '',
        totalFullClears: Number(m.totalFullClears || 0),
        totalPlaytimeSeconds: Number(m.totalPlaytimeSeconds || 0),
      });
    }
  }

  const memberCount = perMember.length;
  const lastUpdated = resp.lastUpdated || new Date().toISOString();

  return {
    dungeonClears: totalFull,
    totalPlaytimeSeconds: totalPlay,
    perMember,
    memberCount,
    lastUpdated,
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

  if (updatedEl && data.lastUpdated) {
    const newText = new Date(data.lastUpdated).toLocaleString();
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