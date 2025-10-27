let previousClearsData = null;

function renderRecentClears(data, forceRender = false) {
  const el = $('recent-activity-content');
  if (!el) return;

  if (!forceRender && !dataHasChanged(data, previousClearsData)) {
    console.log('[RecentClears] No changes detected, skipping render');
    return;
  }

  previousClearsData = data ? JSON.parse(JSON.stringify(data)) : null;

  if (!data || !data.clears || data.clears.length === 0) {
    el.innerHTML = '<div style="color: var(--chocolate); font-style: italic; text-align: center; padding: 40px; grid-column: 1/-1;">No recent clan activities found.</div>';
    return;
  }

  el.innerHTML = data.clears.map((clear, index) => {
    const date = new Date(clear.period);
    const timeAgo = formatTimeAgo(date);
    const pgcrImage = clear.activity?.pgcrImage || '';
    const isClear = clear.isClear !== false; // Assume true if not explicitly false
    const badgeClass = isClear ? 'clear-success' : 'clear-fail';
    const badgeText = isClear ? '✓' : '✗';

    // Get player names (up to 6 displayed)
    const playerNamesArray = clear.clanPlayers
      .slice(0, 6)
      .map(player => {
        const displayName = player.bungieGlobalDisplayName || player.displayName || 'Guardian';
        const nameCode = player.bungieGlobalDisplayNameCode;
        return nameCode ? `${displayName}#${nameCode}` : displayName;
      });

    const remainingCount = Math.max(0, clear.clanPlayers.length - 6);
    const playersDisplay = playerNamesArray.join(', ') + (remainingCount > 0 ? `, +${remainingCount} more` : '');

    // Adjust grid span for collage effect
    const gridSpanClass = index % 2 === 0 ? 'gallery-item-large' : 'gallery-item-small';

    return `
      <div 
        class="gallery-item ${gridSpanClass}" 
        style="background-image: url('${pgcrImage}');"
        data-instance="${clear.instanceId || ''}">
        <div class="gallery-item-overlay">
          <div class="gallery-item-time">${timeAgo}</div>
          <div class="gallery-item-badge ${badgeClass}">${badgeText}</div>
          <div class="gallery-item-players">${escapeHtml(playersDisplay)}</div>
        </div>
      </div>
    `;
  }).join('');

  el.querySelectorAll('.gallery-item').forEach(item => {
    const instanceId = item.getAttribute('data-instance');
    if (instanceId) {
      item.addEventListener('click', () => window.showPGCRModal(instanceId));
    }
  });

  console.log('[RecentClears] Rendered', data.clears.length, 'activities in gallery');
}


async function loadRecentClears(forceRender = false) {
  const data = await fetchJson('/recent-clears');
  if (data) {
    renderRecentClears(data, forceRender);
  }
}