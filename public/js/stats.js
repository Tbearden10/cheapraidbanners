let previousStatsData = null;

function renderStats(data, forceRender = false) {
  const dungeonEl = $('dungeon-count');
  const raidEl = $('raid-count');
  const playtimeEl = $('playtime');
  const updatedEl = $('last-updated');

  if (!forceRender && !dataHasChanged(data, previousStatsData)) {
    console.log('[Stats] No changes detected, skipping render');
    return;
  }
  
  previousStatsData = data ? JSON.parse(JSON.stringify(data)) : null;

  if (!data) {
    if (dungeonEl) dungeonEl.textContent = '—';
    if (raidEl) raidEl.textContent = '—';
    if (playtimeEl) playtimeEl.textContent = '—';
    if (updatedEl) updatedEl.textContent = 'Loading...';
    return;
  }

  if (dungeonEl && typeof data.dungeonClears !== 'undefined') {
    animateCounter(dungeonEl, data.dungeonClears);
  }
  
  if (raidEl && typeof data.raidClears !== 'undefined') {
    animateCounter(raidEl, data.raidClears);
  }
  
  if (playtimeEl && typeof data.totalPlaytimeSeconds !== 'undefined') {
    const hours = Math.floor(data.totalPlaytimeSeconds / 3600);
    playtimeEl.textContent = nf.format(hours) + 'h';
  }

  if (updatedEl && data.fetchedAt) {
    const newText = new Date(data.fetchedAt).toLocaleString();
    if (updatedEl.textContent !== newText) {
      updatedEl.textContent = newText;
    }
  }
}

async function loadStats(forceRender = false) {
  const data = await fetchJson('/stats');
  if (data) {
    renderStats(data, forceRender);
  }
  return data;
}