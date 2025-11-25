// Activity leaderboard management
let previousLeaderboardData = null;

/**
 * Get API base URL - evaluated at runtime
 */
function getApiBase() {
  return window.__utils?.API_BASE || window.API_BASE || 'https://api.cheapraidbanners.com';
}

function renderActivityLeaderboard(data, forceRender = false) {
  const el = window.__utils?.$ ? window.__utils.$('activity-leaderboard-content') : document.getElementById('activity-leaderboard-content');
  if (!el) return;

  const dataHasChanged = window.__utils?.dataHasChanged || window.dataHasChanged;
  if (!forceRender && dataHasChanged && !dataHasChanged(data, previousLeaderboardData)) {
    console.log('[Leaderboard] No changes detected, skipping render');
    return;
  }

  previousLeaderboardData = data ? JSON.parse(JSON.stringify(data)) : null;

  if (!data || !data.activities || data.activities.length === 0) {
    el.innerHTML = '<div style="color: var(--chocolate); font-style: italic; text-align: center; padding: 40px;">No activity data available.</div>';
    return;
  }

  const escape = window.__utils?.escapeHtml || window.escapeHtml || (t => String(t));
  const nf = window.__utils?.nf || window.nf || new Intl.NumberFormat();

  el.className = 'activity-leaderboard-grid';
  el.innerHTML = data.activities.map((activity, idx) => {
    const rankClass = idx === 0 ? 'rank-1' : idx === 1 ? 'rank-2' : idx === 2 ? 'rank-3' : '';
    
    return `
      <div class="activity-leaderboard-item ${rankClass}">
        <div class="activity-leaderboard-rank ${rankClass}">#${idx + 1}</div>
        <div class="activity-leaderboard-name">${escape(activity.name)}</div>
        <div class="activity-leaderboard-count">${nf.format(activity.count)}</div>
        <div class="activity-leaderboard-label">Clears</div>
      </div>
    `;
  }).join('');

  console.log('[Leaderboard] Rendered', data.activities.length, 'activities');
}

async function loadActivityLeaderboard(forceRender = false) {
  try {
    const API_BASE = getApiBase();
    const url = new URL('/activity-leaderboard', API_BASE).toString();
    
    console.log('[Leaderboard] Fetching from:', url);
    
    const fetchJsonFn = window.__utils?.fetchJson || window.fetchJson;
    const data = await fetchJsonFn(url);
    
    if (data) {
      renderActivityLeaderboard(data, forceRender);
    } else {
      console.warn('[Leaderboard] No data received');
    }
  } catch (err) {
    console.error('[Leaderboard] Error loading:', err);
  }
}

// Expose functions globally
window.loadActivityLeaderboard = loadActivityLeaderboard;
window.renderActivityLeaderboard = renderActivityLeaderboard;