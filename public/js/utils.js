// url=https://github.com/Tbearden10/dungeon-info-hub/blob/master/workers/static/js/utils.js
// utils.js - shared helper utilities (updated with config constants)
// Exposes safe globals for other non-module scripts to use.

(function () {
  // Lightweight globals (exposed intentionally to other scripts)
  window.$ = function (id) { return document.getElementById(id); };
  window.nf = new Intl.NumberFormat();

  // Expose config values (frontend fallback only)
  const BUNGIE_API_KEY = '168a567f650d45af8276b6ca5d09ea56';
  const CLAN_ID = '5335552';

  // Helper functions (declared to be available on window)
  window.escapeHtml = function (text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
  };

  window.formatDuration = function (seconds) {
    const mins = Math.floor(seconds / 60);
    const secs = seconds % 60;
    if (mins === 0) return `${secs}s`;
    return `${mins}m ${secs}s`;
  };

  window.formatTimeAgo = function (date) {
    const seconds = Math.floor((new Date() - date) / 1000);
    if (seconds < 60) return 'Just now';
    if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`;
    if (seconds < 86400) return `${Math.floor(seconds / 3600)}h ago`;
    if (seconds < 604800) return `${Math.floor(seconds / 86400)}d ago`;
    return date.toLocaleDateString();
  };

  window.dataHasChanged = function (newData, oldData) {
    if (!oldData && newData) return true;
    if (!newData) return false;
    try {
      return JSON.stringify(newData) !== JSON.stringify(oldData);
    } catch (e) {
      return true;
    }
  };

  window.fetchJson = async function (url) {
    try {
      const res = await fetch(url);
      if (!res.ok) return null;
      return await res.json().catch(() => null);
    } catch (err) {
      console.error('Fetch error:', err);
      return null;
    }
  };

  window.animateCounter = function (el, target, duration = 1200) {
    if (!el) return;
    target = Number(target) || 0;
    const start = Number((el.textContent || '').replace(/[^\d]/g, '')) || 0;
    if (start === target) return;
    const startTime = performance.now();
    function step(now) {
      const elapsed = now - startTime;
      const progress = Math.min(1, elapsed / duration);
      const eased = progress < 0.5 ? 2 * progress * progress : -1 + (4 - 2 * progress) * progress;
      const value = Math.round(start + (target - start) * eased);
      el.textContent = window.nf.format(value);
      if (progress < 1) requestAnimationFrame(step);
    }
    requestAnimationFrame(step);
  };

  // Role info (exposed)
  const ROLE_INFO = {
    5: { label: 'Founder', color: '#ffd700', priority: 5 },
    4: { label: 'Acting Founder', color: '#ff8c00', priority: 4 },
    3: { label: 'Admin', color: '#dc143c', priority: 3 },
    2: { label: 'Member', color: '#4169e1', priority: 2 },
    1: { label: 'Beginner', color: '#9aa0a6', priority: 1 }
  };

  window.getRoleInfo = function (memberType) {
    const type = Number(memberType || 0);
    return ROLE_INFO[type] || { label: 'Member', color: '#4169e1', priority: 0 };
  };

  // Export a convenient namespace too
  window.__utils = {
    $: window.$,
    nf: window.nf,
    escapeHtml: window.escapeHtml,
    formatDuration: window.formatDuration,
    formatTimeAgo: window.formatTimeAgo,
    dataHasChanged: window.dataHasChanged,
    fetchJson: window.fetchJson,
    animateCounter: window.animateCounter,
    getRoleInfo: window.getRoleInfo,
    BUNGIE_API_KEY,
    CLAN_ID
  };
})();