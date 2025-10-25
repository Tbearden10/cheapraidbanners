// public/script.js
(() => {
  const PATH = '/stats';
  const CLEAR_ID = 'bungie-clears';
  const UPDATED_ID = 'bungie-updated';
  const POLL_MS = 30_000;

  async function refresh() {
    try {
      const res = await fetch(PATH, { cache: 'no-store' });
      if (!res.ok) throw new Error('network');
      const j = await res.json();
      const clears = (j && typeof j.clears === 'number') ? j.clears : 0;
      const updated = (j && j.updated) ? new Date(j.updated).toLocaleString() : 'never';
      document.getElementById(CLEAR_ID).textContent = String(clears);
      document.getElementById(UPDATED_ID).textContent = updated;
    } catch (e) {
      console.error('fetch stats failed', e);
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