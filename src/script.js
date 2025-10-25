// public/script.js
// Client: fetch /stats and on each successful fetch increment the on-page displayed value by +1.
// On first successful fetch we initialize the display to the authoritative value returned by /stats.

(() => {
  const PATH = '/stats';
  const CLEAR_ID = 'bungie-clears';
  const UPDATED_ID = 'bungie-updated';
  const POLL_MS = 10_000; // faster for testing; adjust as desired

  let firstLoad = true;

  async function refresh() {
    try {
      const res = await fetch(PATH, { cache: 'no-store' });
      if (!res.ok) throw new Error('network');
      const j = await res.json();

      // If this is the first successful fetch, set the displayed value to the authoritative value.
      // On subsequent successful fetches, increment the displayed value locally by +1 for testing.
      const elC = document.getElementById(CLEAR_ID);
      const elU = document.getElementById(UPDATED_ID);
      if (!elC) return;

      if (firstLoad) {
        const base = (j && typeof j.clears === 'number') ? j.clears : 0;
        elC.textContent = String(base);
        firstLoad = false;
      } else {
        const cur = Number(elC.textContent) || 0;
        elC.textContent = String(cur + 1);
      }

      if (elU) {
        elU.textContent = (j && j.updated) ? new Date(j.updated).toLocaleString() : 'never';
      }
    } catch (e) {
      console.error('fetch stats failed', e);
      // on error, do not change the displayed value; keep testing increments only on success
    }
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', () => {
      refresh();
      setInterval(refresh, POLL_MS);
    });
  } else {
    refresh();
    setInterval(refresh, POLL_MS);
  }
})();