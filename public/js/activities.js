// activities.js - simplified for new /recent-activities

const ACTIVITIES_TO_DISPLAY = 3;

/**
 * Format seconds into a readable duration string (e.g., "12m 34s")
 */
function formatDuration(seconds) {
  if (!seconds || seconds <= 0) return '0s';
  const mins = Math.floor(seconds / 60);
  const secs = Math.floor(seconds % 60);
  return mins > 0 ? `${mins}m ${secs}s` : `${secs}s`;
}

/**
 * Load recent activities from backend
 */
async function loadRecentActivities() {
  const container = document.getElementById('recent-activities-content');
  if (!container) return;

  container.innerHTML = '<div style="color: var(--chocolate); text-align:center; padding:40px;">Loading recent activities...</div>';

  try {
    const apiBase = window.API_BASE || window.__utils?.API_BASE || 'https://api.cheapraidbanners.com';
    const url = `${apiBase}/recent-activities`;

    const fetchJsonFn = window.__utils?.fetchJson || window.fetchJson;
    const activities = await fetchJsonFn(url);

    if (!activities || !Array.isArray(activities) || activities.length === 0) {
      container.innerHTML = '<div style="color: var(--chocolate); text-align:center; padding:40px;">No recent activities found.</div>';
      return;
    }

    renderRecentActivities(activities);
  } catch (err) {
    console.error('[RecentActivities] Failed to load', err);
    container.innerHTML = '<div style="color: var(--chocolate); text-align:center; padding:40px;">Failed to load activities.</div>';
  }
}

/**
 * Render activities into gallery
 */
function renderRecentActivities(activities) {
  const container = document.getElementById('recent-activities-content');
  if (!container) return;

  const escape = window.escapeHtml || (t => String(t));

  const itemsHtml = activities.map(act => {
    const duration = formatDuration(act.duration);
    const completed = !!act.completed;
    const badgeClass = completed ? 'clear-success' : 'clear-fail';
    const badgeText = completed ? '✓' : '✗';
    const imageUrl = act.image || '';
    const instanceId = act.instanceId || '';
    const instanceAttr = instanceId ? `data-instance="${escape(instanceId)}"` : '';

    const styleAttr = imageUrl
      ? `background-image: url('${escape(imageUrl)}');`
      : 'background: linear-gradient(135deg, var(--chocolate) 0%, var(--chocolate-dark) 100%);';

    return `
      <div class="gallery-item" ${instanceAttr} title="" style="${styleAttr}">
        <div class="gallery-item-overlay always-visible">
          <div class="gallery-item-time">${duration}</div>
          <div class="gallery-item-badge ${badgeClass}">${badgeText}</div>
        </div>
      </div>
    `;
  }).join('');

  container.innerHTML = `<div class="gallery-grid">${itemsHtml}</div>`;

  const grid = container.querySelector('.gallery-grid');
  if (grid) {
    grid.style.display = 'grid';
    grid.style.gridGap = '12px';
    const cols = Math.min(ACTIVITIES_TO_DISPLAY, activities.length);
    grid.style.gridTemplateColumns = `repeat(${cols}, 1fr)`;
  }

  container.querySelectorAll('.gallery-item').forEach(item => {
    const instanceId = item.getAttribute('data-instance');
    if (instanceId) {
      item.style.cursor = 'pointer';
      item.addEventListener('click', () => {
        if (typeof window.showPGCRModal === 'function') {
          window.showPGCRModal(instanceId);
        }
      });
    }
  });
}

// Expose globally
window.loadRecentActivities = loadRecentActivities;
