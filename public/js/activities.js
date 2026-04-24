async function loadRecentActivities() {
  const container = document.getElementById('recent-activities-content');
  if (!container) return;
  container.innerHTML = '<div style="color: var(--chocolate); text-align:center; padding:40px;">Loading recent activities...</div>';
  try {
    const res = await fetch(window.__utils.API_BASE + '/recent-activities');
    const activities = await res.json();
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
    container.innerHTML = '<div style="color: var(--chocolate); text-align:center; padding:40px;">Failed to load activities.</div>';
  }
}

function formatDuration(seconds) {
  if (!seconds || seconds <= 0) return '0s';
  const mins = Math.floor(seconds / 60);
  const secs = Math.floor(seconds % 60);
  return mins > 0 ? `${mins}m ${secs}s` : `${secs}s`;
}
window.loadRecentActivities = loadRecentActivities;