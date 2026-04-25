// utils
window.nf = new Intl.NumberFormat();

window.animateCounter = function(el, target, duration = 1200) {
  if (!el) return;
  target = Number(target) || 0;
  const start = Number((el.textContent || '').replace(/[^\d]/g, '')) || 0;
  if (start === target) {
    el.textContent = window.nf.format(target);
    return;
  }
  const startTime = performance.now();
  function step(now) {
    const progress = Math.min(1, (now - startTime) / duration);
    const value = Math.round(start + (target - start) * progress);
    el.textContent = window.nf.format(value);
    if (progress < 1) requestAnimationFrame(step);
  }
  requestAnimationFrame(step);
};
window.__utils = { API_BASE: "https://api.cheapraidbanners.com" };

function formatPlaytime(seconds) {
  seconds = Math.floor(seconds || 0);
  const d = Math.floor(seconds / 86400);
  const h = Math.floor((seconds % 86400) / 3600);
  const m = Math.floor((seconds % 3600) / 60);
  const s = seconds % 60;
  if (d > 0) return `${d.toLocaleString()}d ${h}h`;
  if (h > 0) return `${h}h ${m}m`;
  if (m > 0) return `${m}m ${s}s`;
  return `${s}s`;
}

// stats
async function loadStats() {
  try {
    const res = await fetch(window.__utils.API_BASE + '/stats');
    const data = await res.json();
    const dungeonEl = document.getElementById('dungeon-count');
    if (dungeonEl && typeof data.clanStats?.totalFullClears !== 'undefined') {
      window.animateCounter(dungeonEl, data.clanStats.totalFullClears);
    }
    const playtimeEl = document.getElementById('playtime');
    if (playtimeEl && typeof data.clanStats?.totalPlaytimeSeconds !== 'undefined') {
      playtimeEl.textContent = formatPlaytime(data.clanStats.totalPlaytimeSeconds);
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
      <div class="member-stat-card ${rankClass} fade-in" data-clears="${clears}" data-playtime="${m.totalPlaytimeSeconds ?? 0}">
        ${idx < 3 ? `<div class="member-stat-rank ${rankClass}">#${idx + 1}</div>` : ''}
        <div class="member-stat-inner">
          <div class="member-stat-emblem">
            ${emblem ? `<img src="${emblem}" alt="${name} emblem" />` : `<div class="member-stat-emblem-fallback"></div>`}
          </div>
          <div class="member-stat-right">
            <div class="member-stat-header">
              <div class="member-stat-name" title="${name}">${name}</div>
            </div>
            <div class="member-stat-stats">
              <div class="member-stat-stat">
                <div class="member-stat-clears">—</div>
                <div class="member-stat-stat-label">CLEARS</div>
              </div>
              <div class="member-stat-stat-divider"></div>
              <div class="member-stat-stat">
                <div class="member-stat-playtime">—</div>
                <div class="member-stat-stat-label">PLAYTIME</div>
              </div>
            </div>
          </div>
        </div>
      </div>
    `;
  }).join('');

  // Lazy fade-in and animate clears
  const cards = container.querySelectorAll('.member-stat-card');
  const observer = new IntersectionObserver((entries, obs) => {
    const intersecting = entries
      .filter(e => e.isIntersecting)
      .sort((a, b) => a.boundingClientRect.left - b.boundingClientRect.left);

    intersecting.forEach((entry, i) => {
      setTimeout(() => {
        const card = entry.target;
        card.classList.add('visible');
        const clears = Number(card.getAttribute('data-clears')) || 0;
        const clearsEl = card.querySelector('.member-stat-clears');
        if (clearsEl) window.animateCounter(clearsEl, clears, 1600);
        const playtime = Number(card.getAttribute('data-playtime')) || 0;
        const playtimeEl = card.querySelector('.member-stat-playtime');
        if (playtimeEl) playtimeEl.textContent = formatPlaytime(playtime);
        obs.unobserve(card);
      }, i * 80);
    });
  }, { threshold: 0.2 });
  cards.forEach(card => observer.observe(card));
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
      const duration = formatPlaytime(act.duration);
      const badgeClass = act.completed ? 'clear-success' : 'clear-fail';
      const imageUrl = act.image || '';
      const styleAttr = imageUrl
        ? `background-image: url('${imageUrl}');`
        : 'background: linear-gradient(135deg, var(--chocolate) 0%, var(--chocolate-dark) 100%);';
      return `
        <div class="gallery-item" style="${styleAttr}">
          <div class="gallery-item-overlay always-visible">
            <div class="gallery-item-badge ${badgeClass}">${duration}</div>
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
  const [statsData, membersData] = await Promise.all([
    loadStats(),
    (async () => {
        try {
        const res = await fetch(window.__utils.API_BASE + '/members');
        const data = await res.json();
        const countEl = document.getElementById('members-count');
        if (countEl) window.animateCounter(countEl, data.members?.length ?? 0, 1200);
        return data.members || [];
        } catch {
        document.getElementById('members-count').textContent = '—';
        return [];
        }
    })()
    ]);

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

  // Sort by clears descending
  mergedMembers.sort((a, b) => (b.totalFullClears ?? 0) - (a.totalFullClears ?? 0));

  renderMembers(mergedMembers);
  loadRecentActivities();
  document.querySelectorAll('.scroll-reveal').forEach(el => {
    const d = Number(el.getAttribute('data-delay')) || 0;
    if (d > 0) setTimeout(() => el.classList.add('visible'), d);
    else el.classList.add('visible');
  });
});