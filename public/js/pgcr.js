const pgcrCache = {};

window.showPGCRModal = async function(instanceId) {
  if (!instanceId) return;

  const modal = createPGCRModal();
  document.body.appendChild(modal);

  const contentEl = modal.querySelector('.pgcr-content');
  contentEl.innerHTML = '<div style="text-align: center; padding: 40px; color: var(--chocolate);">Loading activity details...</div>';

  // Check if data is already cached
  if (pgcrCache[instanceId]) {
    console.log(`[PGCR] Using cached data for instanceId: ${instanceId}`);
    renderPGCRContent(contentEl, pgcrCache[instanceId]);
    return;
  }

  // Fetch data if not in cache
  try {
    console.log(`[PGCR] Fetching data for instanceId: ${instanceId}`);
    const data = await fetchJson(`/pgcr?instanceId=${encodeURIComponent(instanceId)}`);
    
    if (!data || data.error) {
      contentEl.innerHTML = `<div style="text-align: center; padding: 40px; color: #dc143c;">Failed to load activity: ${data?.error || 'Unknown error'}</div>`;
      return;
    }

    // Cache the data
    pgcrCache[instanceId] = data;
    console.log(`[PGCR] Data cached for instanceId: ${instanceId}`);

    renderPGCRContent(contentEl, data);
  } catch (error) {
    console.error('[PGCR] Error:', error);
    contentEl.innerHTML = '<div style="text-align: center; padding: 40px; color: #dc143c;">Failed to load activity details</div>';
  }
};

function createPGCRModal() {
  const modal = document.createElement('div');
  modal.className = 'pgcr-modal';
  modal.innerHTML = `
    <div class="pgcr-backdrop" onclick="this.parentElement.remove()"></div>
    <div class="pgcr-dialog">
      <button class="pgcr-close" onclick="this.closest('.pgcr-modal').remove()">×</button>
      <div class="pgcr-content"></div>
    </div>
  `;
  return modal;
}

function renderPGCRContent(container, data) {
  const activity = data.activity || {};
  const players = data.players || [];
  const duration = data.activityDurationSeconds || 0;
  
  // Determine if activity was completed based on PGCR data
  const startedFromBeginning = data.activityWasStartedFromBeginning || false;
  const startingPhase = data.startingPhaseIndex || 0;
  
  // Check if any player completed
  const anyCompleted = players.some(p => p.completed);

  const durationStr = formatDuration(duration);
  const dateStr = data.period ? new Date(data.period).toLocaleString() : 'Unknown date';

  container.innerHTML = `
    <div class="pgcr-header" style="${activity.pgcrImage ? `background-image: url('${activity.pgcrImage}');` : ''}">
      <div class="pgcr-header-overlay">
        <h2 class="pgcr-title">${activity.name || 'Activity'}</h2>
        <div class="pgcr-meta">
          <span>${dateStr}</span>
          <span>•</span>
          <span>${durationStr}</span>
        </div>
      </div>
    </div>
    
    <div class="pgcr-players">
      ${players.map(player => renderPlayerCard(player)).join('')}
    </div>
  `;
}

function renderPlayerCard(player) {
  // Check the completed value from PGCR
  const completed = player.completed || false;
  const completedClass = completed ? 'completed' : 'incomplete';
  const completedText = completed ? 'Completed' : 'Did Not Complete';
  const timePlayedStr = formatDuration(player.timePlayedSeconds || 0);
  const kd = player.killsDeathsRatio || 0;

  // Use global display name with code
  const displayName = player.bungieGlobalDisplayName || player.displayName || 'Guardian';
  const nameCode = player.bungieGlobalDisplayNameCode;
  const fullName = nameCode ? `${displayName}#${nameCode}` : displayName;

  return `
    <div class="pgcr-player ${completedClass}">
      <div class="pgcr-player-header">
        <div class="pgcr-player-left">
          ${player.iconPath ? `
            <img src="${player.iconPath}" alt="" class="pgcr-player-icon" />
          ` : ''}
          <div class="pgcr-player-info">
            <div class="pgcr-player-name">${escapeHtml(fullName)}</div>
            <div class="pgcr-player-class">${player.class?.name || 'Unknown'} • ${player.lightLevel || '?'} Light</div>
          </div>
        </div>
        <div class="pgcr-player-right">
          <div class="pgcr-player-time">${timePlayedStr}</div>
          <div class="pgcr-player-status ${completedClass}">${completedText}</div>
        </div>
      </div>
      
      <div class="pgcr-player-stats">
        <div class="pgcr-stat-group">
          <div class="pgcr-stat">
            <div class="pgcr-stat-value">${player.kills || 0}</div>
            <div class="pgcr-stat-label">Kills</div>
          </div>
          <div class="pgcr-stat">
            <div class="pgcr-stat-value">${player.deaths || 0}</div>
            <div class="pgcr-stat-label">Deaths</div>
          </div>
          <div class="pgcr-stat">
            <div class="pgcr-stat-value">${player.assists || 0}</div>
            <div class="pgcr-stat-label">Assists</div>
          </div>
          <div class="pgcr-stat">
            <div class="pgcr-stat-value">${kd.toFixed(2)}</div>
            <div class="pgcr-stat-label">K/D</div>
          </div>
        </div>
        
        ${(player.precisionKills || player.grenadeKills || player.meleeKills || player.superKills) ? `
          <div class="pgcr-stat-group">
            <div class="pgcr-stat">
              <div class="pgcr-stat-value">${player.precisionKills || 0}</div>
              <div class="pgcr-stat-label">Precision</div>
            </div>
            <div class="pgcr-stat">
              <div class="pgcr-stat-value">${player.grenadeKills || 0}</div>
              <div class="pgcr-stat-label">Grenade</div>
            </div>
            <div class="pgcr-stat">
              <div class="pgcr-stat-value">${player.meleeKills || 0}</div>
              <div class="pgcr-stat-label">Melee</div>
            </div>
            <div class="pgcr-stat">
              <div class="pgcr-stat-value">${player.superKills || 0}</div>
              <div class="pgcr-stat-label">Super</div>
            </div>
          </div>
        ` : ''}
      </div>
    </div>
  `;
}