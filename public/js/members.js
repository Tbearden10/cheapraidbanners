async function loadMembers() {
  try {
    const res = await fetch(window.__utils.API_BASE + '/members');
    const data = await res.json();
    document.getElementById('members-count').textContent = data.members?.length ?? '—';
    renderMembers(data.members || []);
  } catch {
    document.getElementById('members-count').textContent = '—';
    renderMembers([]);
  }
}

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
window.loadMembers = loadMembers;