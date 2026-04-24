// stats.js - Minimal, only for new backend format

let previousStatsData = null;

function getApiBase() {
  return window.__utils?.API_BASE || window.API_BASE || 'https://api.cheapraidbanners.com';
}

function normalizeStatsResponse(resp) {
  if (!resp) return null;
  // Directly extract the fields you need
  return {
    dungeonClears: Number(resp.clanStats?.totalFullClears ?? 0),
    totalPlaytimeSeconds: Number(resp.clanStats?.totalPlaytimeSeconds ?? 0),
    perMember: Array.isArray(resp.memberStats) ? resp.memberStats : [],
    memberCount: Array.isArray(resp.memberStats) ? resp.memberStats.length : 0,
    lastUpdated: resp.lastUpdated || new Date().toISOString(),
  };
}

function renderStatsLocal(data, forceRender = false) {
  const dungeonEl = window.__utils?.$ ? window.__utils.$('dungeon-count') : document.getElementById('dungeon-count');
  const playtimeEl = window.__utils?.$ ? window.__utils.$('playtime') : document.getElementById('playtime');
  const updatedEl = window.__utils?.$ ? window.__utils.$('last-updated') : document.getElementById('last-updated');

  if (!forceRender && !window.__utils?.dataHasChanged(data, previousStatsData)) return;
  previousStatsData = data ? JSON.parse(JSON.stringify(data)) : null;

  if (!data) {
    if (dungeonEl) dungeonEl.textContent = '—';
    if (playtimeEl) playtimeEl.textContent = '—';
    if (updatedEl) updatedEl.textContent = 'Loading...';
    return;
  }

  if (dungeonEl && typeof data.dungeonClears !== 'undefined') {
    window.__utils?.animateCounter
      ? window.__utils.animateCounter(dungeonEl, data.dungeonClears)
      : (dungeonEl.textContent = String(data.dungeonClears));
  }

  if (playtimeEl && typeof data.totalPlaytimeSeconds !== 'undefined') {
    const hours = Math.floor(data.totalPlaytimeSeconds / 3600);
    if (window.__utils?.animateCounter) {
      window.__utils.animateCounter(playtimeEl, hours);
      setTimeout(() => {
        if (playtimeEl && playtimeEl.textContent && !playtimeEl.textContent.includes('h')) {
          playtimeEl.textContent += 'h';
        }
      }, 1250);
    } else {
      playtimeEl.textContent = String(hours) + 'h';
    }
  }

  if (updatedEl && data.lastUpdated) {
    const newText = new Date(data.lastUpdated).toLocaleString();
    if (updatedEl.textContent !== newText) {
      updatedEl.textContent = newText;
    }
  }
}

async function loadStats(forceRender = false) {
  const API_BASE = getApiBase();
  const statsUrl = new URL('/stats', API_BASE).toString();

  try {
    const raw = await (window.__utils?.fetchJson
      ? window.__utils.fetchJson(statsUrl)
      : fetch(statsUrl).then(r => r.ok ? r.json().catch(() => null) : null).catch(() => null)
    );
    if (!raw) throw new Error('Failed to fetch stats');
    const normalized = normalizeStatsResponse(raw);
    renderStatsLocal(normalized, forceRender);
    return normalized;
  } catch (err) {
    console.error('[Stats] Error loading stats:', err);
    return null;
  }
}

// Expose for app usage
window.loadStats = loadStats;
window.renderStats = renderStatsLocal;