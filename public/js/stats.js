async function loadStats() {
  try {
    const res = await fetch(window.__utils.API_BASE + '/stats');
    const data = await res.json();
    document.getElementById('dungeon-count').textContent = data.clanStats?.totalFullClears ?? '—';
    document.getElementById('playtime').textContent = Math.floor((data.clanStats?.totalPlaytimeSeconds ?? 0) / 3600) + 'h';
    document.getElementById('last-updated').textContent = new Date(data.lastUpdated).toLocaleString();
  } catch {
    document.getElementById('dungeon-count').textContent = '—';
    document.getElementById('playtime').textContent = '—';
    document.getElementById('last-updated').textContent = '—';
  }
}
window.loadStats = loadStats;