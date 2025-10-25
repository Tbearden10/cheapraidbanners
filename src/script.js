// public/script.js
// Poll /stats and update DOM elements with ids 'bungie-clears' and 'bungie-updated'.

(() => {
  const STATS_PATH = '/stats';
  const POLL_INTERVAL_MS = 30 * 1000; // adjust as needed
  const CLEAR_EL_ID = 'bungie-clears';
  const UPDATED_EL_ID = 'bungie-updated';

  async function fetchStats() {
    try {
      const res = await fetch(STATS_PATH, { cache: 'no-store' });
      if (!res.ok) throw new Error('Network response not ok');
      const json = await res.json();
      updateUI(json);
    } catch (err) {
      console.error('Failed to fetch stats', err);
    }
  }

  function updateUI(json) {
    const clearEl = document.getElementById(CLEAR_EL_ID);
    const updatedEl = document.getElementById(UPDATED_EL_ID);
    if (clearEl && typeof json.clears !== 'undefined') {
      clearEl.textContent = String(json.clears);
    }
    if (updatedEl && json.updated) {
      updatedEl.textContent = new Date(json.updated).toLocaleString();
    }
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', () => {
      fetchStats();
      setInterval(fetchStats, POLL_INTERVAL_MS);
    });
  } else {
    fetchStats();
    setInterval(fetchStats, POLL_INTERVAL_MS);
  }
})();