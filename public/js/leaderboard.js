// Activity leaderboard management
let previousLeaderboardData = null;

function renderActivityLeaderboard(data, forceRender = false) {
  const el = $('activity-leaderboard-content');
  if (!el) return;

  if (!forceRender && !dataHasChanged(data, previousLeaderboardData)) {
    console.log('[Leaderboard] No changes detected, skipping render');
    return;
  }

  previousLeaderboardData = data ? JSON.parse(JSON.stringify(data)) : null;

  if (!data || !data.activities || data.activities.length === 0) {
    el.innerHTML = '<div style="color: var(--chocolate); font-style: italic; text-align: center; padding: 40px;">No activity data available.</div>';
    return;
  }

  el.className = 'activity-leaderboard-grid';
  el.innerHTML = data.activities.map((activity, idx) => {
    const rankClass = idx === 0 ? 'rank-1' : idx === 1 ? 'rank-2' : idx === 2 ? 'rank-3' : '';
    
    return `
      <div class="activity-leaderboard-item ${rankClass}">
        <div class="activity-leaderboard-rank ${rankClass}">#${idx + 1}</div>
        <div class="activity-leaderboard-name">${escapeHtml(activity.name)}</div>
        <div class="activity-leaderboard-count">${nf.format(activity.count)}</div>
        <div class="activity-leaderboard-label">Clears</div>
      </div>
    `;
  }).join('');

  console.log('[Leaderboard] Rendered', data.activities.length, 'activities');
}

async function loadActivityLeaderboard(forceRender = false) {
  const data = await fetchJson('/activity-leaderboard');
  if (data) {
    renderActivityLeaderboard(data, forceRender);
  }
}