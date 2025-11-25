// Minimal SimpleReveal + Application init (replaces previous app.js)
// - Provides a tiny replacement for the missing SimpleReveal so .scroll-reveal elements get visible
// - Keeps defensive data loading and rendering logic

// --- Minimal SimpleReveal implementation ---
// Scans for .scroll-reveal elements and adds .visible with optional delay from data-delay (ms).
class SimpleReveal {
  constructor({ selector = '.scroll-reveal', defaultDelay = 0 } = {}) {
    this.selector = selector;
    this.defaultDelay = defaultDelay;
    this.init();
  }

  init() {
    // Use requestIdleCallback when available for non-blocking init
    const runner = () => this.revealAll();
    if ('requestIdleCallback' in window) requestIdleCallback(runner, { timeout: 500 });
    else setTimeout(runner, 50);
  }

  revealAll() {
    const nodes = Array.from(document.querySelectorAll(this.selector));
    nodes.forEach((el) => {
      // If element already has visible, skip
      if (el.classList.contains('visible')) return;

      // Delay may be specified via data-delay attribute in ms
      const d = Number(el.getAttribute('data-delay')) || this.defaultDelay;
      if (d > 0) {
        setTimeout(() => el.classList.add('visible'), d);
      } else {
        el.classList.add('visible');
      }
    });
  }
}

// --- App logic (safe, with reveal fallback) ---
let currentView = 'cards';
let currentMembersData = null;
let currentStatsData = null;

// Track what's currently being updated to prevent concurrent updates
let updating = {
  members: false,
  stats: false,
};

async function updateAll(forceRender = false) {
  // Load each independently to allow partial updates
  await Promise.allSettled([
    updateMembers(forceRender),
    updateStats(forceRender),
  ]);

  // Load recent activities after members are loaded (needs online members)
  if (currentMembersData) {
    try {
      if (typeof loadRecentActivities === 'function') await loadRecentActivities();
    } catch (err) {
      console.warn('[App] loadRecentActivities failed', err);
    }
  }
}

async function updateMembers(forceRender = false) {
  if (updating.members) return;
  updating.members = true;

  try {
    const membersData = await loadMembers(forceRender);

    // Only update if data changed
    if (membersData && window.dataHasChanged(membersData, currentMembersData)) {
      currentMembersData = membersData;

      // Update member count
      const countEl = window.__utils?.$ ? window.__utils.$('members-count') : document.getElementById('members-count');
      if (countEl && membersData.members) {
        window.__utils?.animateCounter ? window.__utils.animateCounter(countEl, membersData.members.length) : (countEl.textContent = String(membersData.members.length));
      }

      // Re-render member stats if we have stats data
      if (currentStatsData) {
        renderCurrentView();
      }

      console.log('[App] Members updated');
    }
  } catch (err) {
    console.error('[App] updateMembers error', err);
  } finally {
    updating.members = false;
  }
}

async function updateStats(forceRender = false) {
  if (updating.stats) return;
  updating.stats = true;

  try {
    const statsData = await loadStats(forceRender);

    // Only update if data changed
    if (statsData && window.dataHasChanged(statsData, currentStatsData)) {
      const oldStats = currentStatsData;
      currentStatsData = statsData;

      // Update stats bar (only animate values that changed)
      updateStatsBar(statsData, oldStats);

      // Re-render member stats with new data
      if (currentMembersData) {
        renderCurrentView();
      }

      console.log('[App] Stats updated');
    }
  } catch (err) {
    console.error('[App] updateStats error', err);
  } finally {
    updating.stats = false;
  }
}

function updateStatsBar(newStats, oldStats) {
  const dungeonEl = window.__utils?.$ ? window.__utils.$('dungeon-count') : document.getElementById('dungeon-count');
  const playtimeEl = window.__utils?.$ ? window.__utils.$('playtime') : document.getElementById('playtime');
  const updatedEl = window.__utils?.$ ? window.__utils.$('last-updated') : document.getElementById('last-updated');

  // Defensive checks when oldStats may be null
  const oldDungeon = oldStats ? oldStats.dungeonClears : undefined;
  const oldPlay = oldStats ? oldStats.totalPlaytimeSeconds : undefined;

  // Only animate if values actually changed
  if (dungeonEl && typeof newStats.dungeonClears !== 'undefined' && newStats.dungeonClears !== oldDungeon) {
    if (window.__utils?.animateCounter) window.__utils.animateCounter(dungeonEl, newStats.dungeonClears);
    else dungeonEl.textContent = String(newStats.dungeonClears);
  }

  if (playtimeEl && typeof newStats.totalPlaytimeSeconds !== 'undefined' && newStats.totalPlaytimeSeconds !== oldPlay) {
    const hours = Math.floor(newStats.totalPlaytimeSeconds / 3600);
    if (window.__utils?.animateCounter) window.__utils.animateCounter(playtimeEl, hours);
    else playtimeEl.textContent = String(hours) + 'h';

    // Add 'h' suffix after animation completes
    setTimeout(() => {
      if (playtimeEl.textContent && !playtimeEl.textContent.includes('h')) {
        playtimeEl.textContent += 'h';
      }
    }, 1250);
  }

  if (updatedEl && newStats.fetchedAt) {
    updatedEl.textContent = new Date(newStats.fetchedAt).toLocaleString();
  }
}

function renderCurrentView() {
  if (!currentMembersData || !currentStatsData) return;

  if (currentView === 'bars') {
    if (typeof renderMemberStatsBars === 'function') {
      renderMemberStatsBars(currentStatsData, currentMembersData);
    }
  } else {
    if (typeof renderMemberStats === 'function') {
      renderMemberStats(currentStatsData, currentMembersData);
    }
  }
}

function setupViewToggles() {
  const toggleButtons = document.querySelectorAll('.viz-toggle');

  toggleButtons.forEach((btn) => {
    btn.addEventListener('click', () => {
      const view = btn.getAttribute('data-view');
      if (currentView === view) return; // Already in this view

      currentView = view;

      // Update active state
      toggleButtons.forEach((b) => b.classList.remove('active'));
      btn.classList.add('active');

      // Re-render with current data
      renderCurrentView();
    });
  });
}

// DOMContentLoaded init
document.addEventListener('DOMContentLoaded', () => {
  // Initialize SimpleReveal (our small implementation)
  try {
    // eslint-disable-next-line no-new
    new SimpleReveal();
    console.log('[App] SimpleReveal initialized');
  } catch (err) {
    console.warn('[App] SimpleReveal init failed', err);
    // As a last resort, make sure nothing remains permanently hidden
    Array.from(document.querySelectorAll('.scroll-reveal')).forEach(el => el.classList.add('visible'));
  }

  // Setup toggle buttons
  try {
    setupViewToggles();
  } catch (err) {
    console.warn('[App] setupViewToggles failed', err);
  }

  // Initial load with forced render - fetch data only on page load
  updateAll(true).catch((e) => console.error('[App] initial updateAll failed', e));

  console.log('[App] Frontend initialized - data will only refresh on page reload');
});