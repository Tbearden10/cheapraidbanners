// public/script.js
// Fetch /stats and update the DOM. Pauses when tab is hidden.
// Poll interval default is 30s. Adds a manual "Refresh now" button.

(() => {
  const STATS_URL = '/stats';
  const CLEAR_ID = 'bungie-clears';
  const UPDATED_ID = 'bungie-updated';
  const REFRESH_BTN_ID = 'refresh-button';
  const REFRESH_STATUS_ID = 'refresh-status';
  const BASE_POLL_MS = 30_000;

  let timer = null;
  let backoffFactor = 0;

  function getElement(id) {
    return document.getElementById(id);
  }

  function setStatus(text) {
    const el = getElement(REFRESH_STATUS_ID);
    if (el) el.textContent = text;
  }

  function render(data) {
    const clearsEl = getElement(CLEAR_ID);
    const updatedEl = getElement(UPDATED_ID);
    if (!clearsEl || !updatedEl) return;

    const clears = (data && typeof data.clears === 'number') ? data.clears : 0;
    const updated = (data && data.updated) ? new Date(data.updated).toLocaleString() : 'never';

    clearsEl.textContent = String(clears);
    updatedEl.textContent = updated;
  }

  function scheduleNext() {
    clearTimeout(timer);
    const ms = backoffFactor > 0 ? Math.min(5 * 60_000, BASE_POLL_MS * (2 ** backoffFactor)) : BASE_POLL_MS;
    timer = setTimeout(() => pollOnce(false), ms);
  }

  // pollOnce(force) - if force === true, ignore document.hidden check and run immediately.
  async function pollOnce(force = false) {
    // If the tab is hidden and not forced, defer until visible
    if (!force && document.hidden) {
      scheduleNext();
      return;
    }

    const refreshBtn = getElement(REFRESH_BTN_ID);
    try {
      // UI: indicate fetching
      if (refreshBtn) refreshBtn.disabled = true;
      setStatus('refreshing…');

      const res = await fetch(STATS_URL, { cache: 'no-store' });
      const text = await res.text();

      if (!res.ok) {
        console.warn('/stats returned non-OK status', res.status, text.slice(0, 500));
        backoffFactor = Math.min(6, backoffFactor + 1);
        setStatus('error');
        scheduleNext();
        return;
      }

      // Try parse JSON, but handle HTML/error pages gracefully
      let json = null;
      try {
        json = JSON.parse(text);
      } catch (err) {
        console.error('Non-JSON /stats response:', text.slice(0, 1000));
        backoffFactor = Math.min(6, backoffFactor + 1);
        setStatus('error');
        scheduleNext();
        return;
      }

      // Success: reset backoff and render
      backoffFactor = 0;
      render(json);
      setStatus('idle');
    } catch (err) {
      console.error('Failed to fetch /stats:', err);
      backoffFactor = Math.min(6, backoffFactor + 1);
      setStatus('error');
    } finally {
      if (refreshBtn) refreshBtn.disabled = false;
      scheduleNext();
    }
  }

  // Manual refresh invoked by button - forces immediate fetch and resets backoff
  function manualRefresh() {
    backoffFactor = 0;
    // trigger polling immediately, force even if tab hidden
    pollOnce(true);
  }

  // Immediately poll once, and then start interval
  function start() {
    // Defensive: ensure DOM exists
    if (!getElement(CLEAR_ID) || !getElement(UPDATED_ID)) {
      // If DOM isn't ready, try again on DOMContentLoaded
      document.addEventListener('DOMContentLoaded', () => {
        pollOnce(false);
      }, { once: true });
      return;
    }

    // Wire up manual refresh button if present
    const btn = getElement(REFRESH_BTN_ID);
    if (btn) {
      btn.addEventListener('click', manualRefresh);
      btn.addEventListener('keydown', (e) => {
        // allow Enter/Space to trigger for accessibility
        if ((e.key === 'Enter' || e.key === ' ') && document.activeElement === btn) {
          e.preventDefault();
          manualRefresh();
        }
      });
    }

    // Initial fetch
    pollOnce(false);
  }

  // Pause/resume on visibility changes: when tab becomes visible, fetch immediately
  document.addEventListener('visibilitychange', () => {
    if (!document.hidden) {
      backoffFactor = 0;
      pollOnce(false);
    }
  });

  // Start when ready
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', start, { once: true });
  } else {
    start();
  }
})();