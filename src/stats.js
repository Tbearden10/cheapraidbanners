// Stats management
let previousStatsData = null;

function renderStats(data, forceRender = false) {
  const clearsEl = $('total-count');
  const prophecyEl = $('prophecy-count');
  const updatedEl = $('last-updated');

  if (!forceRender && !dataHasChanged(data, previousStatsData)) {
    console.log('[Stats] No changes detected, skipping render');
    return;
  }
  
  previousStatsData = data ? JSON.parse(JSON.stringify(data)) : null;

  if (!data) {
    if (clearsEl) clearsEl.textContent = '—';
    if (prophecyEl) prophecyEl.textContent = '—';
    if (updatedEl) updatedEl.textContent = 'Loading...';
    return;
  }

  if (clearsEl && typeof data.clears !== 'undefined') {
    animateCounter(clearsEl, data.clears);
  }
  if (prophecyEl && typeof data.prophecyClears !== 'undefined') {
    animateCounter(prophecyEl, data.prophecyClears);
  }

  if (updatedEl && data.fetchedAt) {
    const newText = new Date(data.fetchedAt).toLocaleString();
    if (updatedEl.textContent !== newText) {
      updatedEl.textContent = newText;
    }
  }
  
  console.log('[Stats] Rendered -', data.clears, 'clears,', data.prophecyClears, 'prophecy');
}

async function loadStats(forceRender = false) {
  const data = await fetchJson('/stats');
  if (data) {
    renderStats(data, forceRender);
  }
  return data;
}