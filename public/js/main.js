// utils
window.animateCounter = function(el, target, duration = 1200) {
  if (!el) return;
  target = Number(target) || 0;
  const start = Number((el.textContent || '').replace(/[^\d]/g, '')) || 0;
  if (start === target) return;
  const startTime = performance.now();
  function step(now) {
    const progress = Math.min(1, (now - startTime) / duration);
    const value = Math.round(start + (target - start) * progress);
    el.textContent = value;
    if (progress < 1) requestAnimationFrame(step);
  }
  requestAnimationFrame(step);
};
window.__utils = { API_BASE: "https://api.cheapraidbanners.com" };

function formatDuration(seconds) {
  if (!seconds || seconds <= 0) return '0s';
  const mins = Math.floor(seconds / 60);
  const secs = Math.floor(seconds % 60);
  return mins > 0 ? `${mins}m ${secs}s` : `${secs}s`;
}

// stats
async function loadStats() {
  try {
    const res = await fetch(window.__utils.API_BASE + '/stats');
    const data = await res.json();
    const dungeonEl = document.getElementById('dungeon-count');
    const playtimeEl = document.getElementById('playtime');
    if (dungeonEl && typeof data.clanStats?.totalFullClears !== 'undefined') {
      window.animateCounter(dungeonEl, data.clanStats.totalFullClears);
    }
    if (playtimeEl && typeof data.clanStats?.totalPlaytimeSeconds !== 'undefined') {
      const hours = Math.floor(data.clanStats.totalPlaytimeSeconds / 3600);
      window.animateCounter(playtimeEl, hours);
      setTimeout(() => {
        if (playtimeEl.textContent && !playtimeEl.textContent.includes('h')) {
          playtimeEl.textContent += 'h';
        }
      }, 1250);
    }
    document.getElementById('last-updated').textContent = new Date(data.lastUpdated).toLocaleString();
    return data;
  } catch {
    document.getElementById('dungeon-count').textContent = '—';
    document.getElementById('playtime').textContent = '—';
    document.getElementById('last-updated').textContent = '—';
    return null;
  }
}

// members
function renderMembers(members) {
  const container = document.getElementById('member-stats-container');
  if (!container) return;
  if (!members.length) {
    container.innerHTML = '<div style="text-align:center;padding:40px;color:var(--chocolate);">No members to show.</div>';
    return;
  }
  container.innerHTML = members.map((m, idx) => {
    const emblem = m.emblemPath || '';
    const name = m.displayName || 'Unknown';
    const clears = m.totalFullClears ?? 0;
    const rankClass = idx === 0 ? 'rank-1' : idx === 1 ? 'rank-2' : idx === 2 ? 'rank-3' : '';
    const rankBadge = idx < 3 ? `<div class="member-stat-rank ${rankClass}">#${idx + 1}</div>` : '';
    return `
      <div class="member-stat-card ${rankClass}">
        ${rankBadge}
        <div class="member-stat-emblem">
          ${emblem ? `<img src="${emblem}" alt="${name} emblem" />` : `<div style="width:100%;height:100%;background:linear-gradient(135deg,#eee,#ddd);"></div>`}
        </div>
        <div class="member-stat-name" title="${name}">${name}</div>
        <div class="member-stat-clears">${clears}</div>
        <div class="member-stat-label">Dungeon Clears</div>
      </div>
    `;
  }).join('');
}

// activities
async function loadRecentActivities() {
  const container = document.getElementById('recent-activities-content');
  if (!container) return;
  let loadingTimeout = setTimeout(() => {
    container.innerHTML = '<div style="color: var(--chocolate); text-align:center; padding:40px;">Loading recent activities...</div>';
  }, 200);
  try {
    const res = await fetch(window.__utils.API_BASE + '/recent-activities');
    const activities = await res.json();
    clearTimeout(loadingTimeout);
    if (!Array.isArray(activities) || activities.length === 0) {
      container.innerHTML = '<div style="color: var(--chocolate); text-align:center; padding:40px;">No recent activities found.</div>';
      return;
    }
    container.innerHTML = activities.map(act => {
      const duration = formatDuration(act.duration);
      const badgeClass = act.completed ? 'clear-success' : 'clear-fail';
      const badgeText = act.completed ? '✓' : '✗';
      const imageUrl = act.image || '';
      const styleAttr = imageUrl
        ? `background-image: url('${imageUrl}');`
        : 'background: linear-gradient(135deg, var(--chocolate) 0%, var(--chocolate-dark) 100%);';
      return `
        <div class="gallery-item" style="${styleAttr}">
          <div class="gallery-item-overlay always-visible">
            <div class="gallery-item-time">${duration}</div>
            <div class="gallery-item-badge ${badgeClass}">${badgeText}</div>
          </div>
        </div>
      `;
    }).join('');
  } catch {
    clearTimeout(loadingTimeout);
    container.innerHTML = '<div style="color: var(--chocolate); text-align:center; padding:40px;">Failed to load activities.</div>';
  }
}

// app
document.addEventListener('DOMContentLoaded', async () => {
  // Fetch stats and members in parallel
  const [statsData, membersData] = await Promise.all([
    loadStats(),
    (async () => {
      try {
        const res = await fetch(window.__utils.API_BASE + '/members');
        const data = await res.json();
        document.getElementById('members-count').textContent = data.members?.length ?? '—';
        return data.members || [];
      } catch {
        document.getElementById('members-count').textContent = '—';
        return [];
      }
    })()
  ]);

  // Merge member stats from statsData into membersData
  let memberStatsMap = {};
  if (statsData && Array.isArray(statsData.memberStats)) {
    for (const stat of statsData.memberStats) {
      if (stat.membershipId) memberStatsMap[stat.membershipId] = stat;
    }
  }
  const mergedMembers = membersData.map(m => {
    const stats = memberStatsMap[m.membershipId] || {};
    return {
      ...m,
      totalFullClears: stats.totalFullClears ?? 0,
      totalPlaytimeSeconds: stats.totalPlaytimeSeconds ?? 0
    };
  });

  renderMembers(mergedMembers);
  loadRecentActivities();
  document.querySelectorAll('.scroll-reveal').forEach(el => {
    const d = Number(el.getAttribute('data-delay')) || 0;
    if (d > 0) setTimeout(() => el.classList.add('visible'), d);
    else el.classList.add('visible');
  });
});