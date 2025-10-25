// public/script.js
// Client polls /stats and updates DOM elements with id="bungie-clears" and id="bungie-updated".
// It now defaults to 0 if the response is missing or fetch fails.

(() => {
  const STATS_PATH = '/stats';
  const POLL_INTERVAL_MS = 30 * 1000; // adjust as desired
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
      // on error, ensure UI still shows 0 and a sensible updated value
      updateUI(null);
    }
  }

  function updateUI(json) {
    const clearEl = document.getElementById(CLEAR_EL_ID);
    const updatedEl = document.getElementById(UPDATED_EL_ID);

    // Default clears to 0 when JSON missing or clears undefined
    const clears = (json && typeof json.clears !== 'undefined') ? Number(json.clears) : 0;
    if (clearEl) clearEl.textContent = String(clears);

    // Show timestamp if present, otherwise show "never"
    if (updatedEl) {
      if (json && json.updated) {
        updatedEl.textContent = new Date(json.updated).toLocaleString();
      } else {
        updatedEl.textContent = 'never';
      }
    }
  }

  // Start polling once DOM is ready
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