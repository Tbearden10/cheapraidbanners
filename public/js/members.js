// Members management
let previousMembersData = null;
window.__membersById = new Map();

function renderMemberStats(statsData, membersData) {
  const container = $('member-stats-container');
  if (!container) return;

  if (!membersData || !membersData.members || !statsData || !statsData.perMember) {
    container.innerHTML = '<div style="text-align: center; padding: 40px; color: var(--chocolate);">Loading stats...</div>';
    return;
  }

  // Merge member info with stats
  const memberStats = statsData.perMember.map(stat => {
    const member = membersData.members.find(m => String(m.membershipId) === String(stat.membershipId));
    return {
      ...stat,
      dungeonClears: stat.dungeonClears,
      raidClears: stat.raidClears,
      playtime: stat.totalPlaytimeSeconds,
      displayName: member?.supplementalDisplayName || member?.displayName || 'Guardian',
      memberType: member?.memberType || 2,
      isOnline: member?.isOnline || false,
      emblemPath: member?.emblemPath || member?.emblemBackgroundPath || ''
    };
  });


  // Sort by clears (descending)
  memberStats.sort((a, b) => (b.raidClears+b.dungeonClears) - (a.dungeonClears+a.raidClears));

  container.innerHTML = memberStats.map((stat, idx) => {
    const rankClass = idx === 0 ? 'rank-1' : idx === 1 ? 'rank-2' : idx === 2 ? 'rank-3' : '';
    const statusClass = stat.isOnline ? 'online' : 'offline';
    const statusText = stat.isOnline ? 'Online' : 'Offline';
    
    return `
      <div class="member-stat-card ${rankClass}">
        <div class="member-stat-rank">#${idx + 1}</div>
        
        ${stat.emblemPath ? `
          <div class="member-stat-emblem">
            <img src="${stat.emblemPath}" alt="${escapeHtml(stat.displayName)}" />
          </div>
        ` : ''}
        
        <div class="member-stat-status">
          <span class="status-dot ${statusClass}"></span>
          <span class="status-text-small">${statusText}</span>
        </div>
        
        <div class="member-stat-name">${escapeHtml(stat.displayName)}</div>
        <div class="member-stat-clears">${stat.dungeonClears} | ${stat.raidClears}</div>
        <div class="member-stat-label">Dungeons | Raids</div>
      </div>
    `;
  }).join('');

  // Update member count
  const countEl = $('members-count');
  if (countEl && membersData.members) {
    countEl.textContent = nf.format(membersData.members.length);
  }

  console.log('[Members] Rendered', memberStats.length, 'member stats');
}

function renderMemberStatsBars(statsData, membersData) {
  const container = $('member-stats-container');
  if (!container) return;

  if (!membersData || !membersData.members || !statsData || !statsData.perMember) {
    container.innerHTML = '<div style="text-align: center; padding: 40px; color: var(--chocolate);">Loading stats...</div>';
    return;
  }

  // Merge member info with stats
  const memberStats = statsData.perMember.map(stat => {
    const member = membersData.members.find(m => String(m.membershipId) === String(stat.membershipId));
    return {
      ...stat,
      displayName: member?.supplementalDisplayName || member?.displayName || 'Guardian',
      emblemPath: member?.emblemPath || member?.emblemBackgroundPath || ''
    };
  });

  // Sort by clears (descending)
  memberStats.sort((a, b) => (b.dungeonClears+b.raidClears) - (a.dungeonClears+a.raidClears));
  
  const maxClears = (memberStats[0]?.dungeonClears + memberStats[0]?.raidClears) || 1;

  container.className = 'member-stats-container bars-view';
  container.innerHTML = memberStats.map((stat, idx) => {
    const rankClass = idx === 0 ? 'rank-1' : idx === 1 ? 'rank-2' : idx === 2 ? 'rank-3' : '';
    const percentage = ((stat.dungeonClears + stat.raidClears) / maxClears) * 100;
    
    return `
      <div class="member-stat-bar">
        <div class="member-stat-bar-rank ${rankClass}">#${idx + 1}</div>
        <div class="member-stat-bar-info">
          ${stat.emblemPath ? `
            <div class="member-stat-bar-emblem">
              <img src="${stat.emblemPath}" alt="${escapeHtml(stat.displayName)}" />
            </div>
          ` : ''}
          <div class="member-stat-bar-name">${escapeHtml(stat.displayName)}</div>
        </div>
        <div class="member-stat-bar-visual">
          <div class="member-stat-bar-fill-container">
            <div class="member-stat-bar-fill" style="width: ${percentage}%"></div>
          </div>
          <div class="member-stat-bar-value">${nf.format(stat.dungeonClears)} | ${nf.format(stat.raidClears)}</div>
        </div>
      </div>
    `;
  }).join('');

  console.log('[Members] Rendered bars view');
}

async function loadMembers(forceRender = false) {
  const data = await fetchJson('/members');
  
  if (!forceRender && !dataHasChanged(data, previousMembersData)) {
    console.log('[Members] No changes detected, skipping render');
    return previousMembersData;
  }
  
  previousMembersData = data ? JSON.parse(JSON.stringify(data)) : null;
  
  if (data && data.members) {
    window.__membersById.clear();
    data.members.forEach(m => {
      window.__membersById.set(String(m.membershipId), m);
    });
  }
  
  return data;
}