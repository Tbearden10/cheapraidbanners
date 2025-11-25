// activities.js - Uses /recent-activities backend endpoint
// - Backend fetches activity history directly from Bungie API (no DB)
// - Displays: duration, completion status, and activity image

const ACTIVITIES_TO_DISPLAY = 4;

/**
 * Format seconds into a readable duration string (e.g., "12m 34s")
 */
function formatDuration(seconds) {
  if (!seconds || seconds <= 0) return '0s';
  const mins = Math.floor(seconds / 60);
  const secs = Math.floor(seconds % 60);
  if (mins > 0) {
    return `${mins}m ${secs}s`;
  }
  return `${secs}s`;
}

/**
 * Main function to load and display recent activities from backend
 */
async function loadRecentActivities() {
  const container = document.getElementById('recent-activities-content');
  if (!container) return;

  try {
    container.innerHTML = '<div style="color: var(--chocolate); text-align:center; padding:40px;">Loading recent activities...</div>';

    // Use the global API_BASE from utils.js
    const apiBase = window.API_BASE || window.__utils?.API_BASE || 'https://api.cheapraidbanners.com';
    const url = `${apiBase}/recent-activities`;
    
    console.log('[RecentActivities] Fetching from:', url);

    // Use fetchJson helper from utils.js
    const fetchJsonFn = window.__utils?.fetchJson || window.fetchJson;
    const activities = await fetchJsonFn(url);

    console.log('[RecentActivities] Received:', activities?.length || 0, 'activities');

    if (!activities || !Array.isArray(activities) || activities.length === 0) {
      container.innerHTML = '<div style="color: var(--chocolate); text-align:center; padding:40px;">No recent activities found.</div>';
      return;
    }

    // Render activities
    renderRecentActivities(activities);

  } catch (err) {
    console.error('[RecentActivities] Error:', err);
    container.innerHTML = '<div style="color: var(--chocolate); text-align:center; padding:40px;">Failed to load activities.</div>';
  }
}

function renderRecentActivities(activities) {
  const container = document.getElementById('recent-activities-content');
  if (!container) return;

  if (!activities || activities.length === 0) {
    container.innerHTML = '<div style="color: var(--chocolate); font-style: italic; text-align: center; padding: 40px;">No recent activities found.</div>';
    return;
  }

  const escape = window.escapeHtml || (t => String(t));
  
  const itemsHtml = activities.map(act => {
    // Duration from activity values
    const durationSeconds = act.values?.timePlayedSeconds?.basic?.value || 0;
    const duration = formatDuration(durationSeconds);
    
    // Completion status
    const completed = act.values?.completed?.basic?.value === 1;
    const badgeClass = completed ? 'clear-success' : 'clear-fail';
    const badgeText = completed ? '✓' : '✗';
    
    // Activity image from enriched activityDetails
    let imageUrl = '';
    if (act.activityDetails?.pgcrImage) {
      imageUrl = act.activityDetails.pgcrImage;
    }
    
    // Fallback: try to construct image URL if we have the hash
    if (!imageUrl && act.activityDetails?.directorActivityHash) {
      console.log('[RecentActivities] No pgcrImage in response for activity', act.activityDetails.instanceId);
    }
    
    const imageStyle = imageUrl 
      ? `style="background-image:url('${escape(imageUrl)}'); background-size:cover; background-position:center;"` 
      : 'style="background: linear-gradient(135deg, var(--chocolate) 0%, var(--chocolate-dark) 100%);"';
    
    // Instance ID for PGCR modal
    const instanceId = act.activityDetails?.instanceId || '';
    const instanceAttr = instanceId ? `data-instance="${escape(instanceId)}"` : '';
    
    // Activity name if available
    const activityName = act.activityDetails?.activityName || '';

    return `
      <div class="gallery-item" ${imageStyle} ${instanceAttr} title="${escape(activityName)}">
        <div class="gallery-item-overlay always-visible">
          <div class="gallery-item-time">${escape(duration)}</div>
          <div class="gallery-item-badge ${badgeClass}">${badgeText}</div>
        </div>
      </div>
    `;
  }).join('');

  container.innerHTML = `<div class="gallery-grid">${itemsHtml}</div>`;

  // Adjust grid layout based on number of items - optimized for 5 items
  const grid = container.querySelector('.gallery-grid');
  if (grid) {
    const count = Math.min(ACTIVITIES_TO_DISPLAY, activities.length);
    // For 5 items, display 5 columns on desktop
    grid.style.gridTemplateColumns = `repeat(${count}, minmax(160px, 1fr))`;
  }

  // Attach click handlers for PGCR modal
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

  console.log('[RecentActivities] Rendered', activities.length, 'activities');
}

// Expose function globally
window.loadRecentActivities = loadRecentActivities;