// Minimal SimpleReveal + Application init

class SimpleReveal {
  constructor({ selector = '.scroll-reveal', defaultDelay = 0 } = {}) {
    this.selector = selector;
    this.defaultDelay = defaultDelay;
    this.init();
  }
  init() {
    const runner = () => this.revealAll();
    if ('requestIdleCallback' in window) requestIdleCallback(runner, { timeout: 500 });
    else setTimeout(runner, 50);
  }
  revealAll() {
    const nodes = Array.from(document.querySelectorAll(this.selector));
    nodes.forEach((el) => {
      if (el.classList.contains('visible')) return;
      const d = Number(el.getAttribute('data-delay')) || this.defaultDelay;
      if (d > 0) setTimeout(() => el.classList.add('visible'), d);
      else el.classList.add('visible');
    });
  }
}

// --- App logic ---
let currentView = 'cards';
let currentMembersData = null;
let currentStatsData = null;
let updating = { members: false, stats: false };

async function updateAll(forceRender = false) {
  await Promise.allSettled([
    updateMembers(forceRender),
    updateStats(forceRender),
  ]);
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
    if (membersData && window.dataHasChanged(membersData, currentMembersData)) {
      currentMembersData = membersData;
      const countEl = window.__utils?.$ ? window.__utils.$('members-count') : document.getElementById('members-count');
      if (countEl && typeof membersData.memberCount !== 'undefined') {
        window.__utils?.animateCounter
          ? window.__utils.animateCounter(countEl, membersData.memberCount)
          : (countEl.textContent = String(membersData.memberCount));
      }
      if (currentStatsData) renderCurrentView();
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
    if (statsData && window.dataHasChanged(statsData, currentStatsData)) {
      const oldStats = currentStatsData;
      currentStatsData = statsData;
      updateStatsBar(statsData, oldStats);
      if (currentMembersData) renderCurrentView();
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

  const oldDungeon = oldStats ? oldStats.dungeonClears : undefined;
  const oldPlay = oldStats ? oldStats.totalPlaytimeSeconds : undefined;

  if (dungeonEl && typeof newStats.dungeonClears !== 'undefined' && newStats.dungeonClears !== oldDungeon) {
    window.__utils?.animateCounter
      ? window.__utils.animateCounter(dungeonEl, newStats.dungeonClears)
      : (dungeonEl.textContent = String(newStats.dungeonClears));
  }

  if (playtimeEl && typeof newStats.totalPlaytimeSeconds !== 'undefined' && newStats.totalPlaytimeSeconds !== oldPlay) {
    const hours = Math.floor(newStats.totalPlaytimeSeconds / 3600);
    if (window.__utils?.animateCounter) window.__utils.animateCounter(playtimeEl, hours);
    else playtimeEl.textContent = String(hours) + 'h';
    setTimeout(() => {
      if (playtimeEl.textContent && !playtimeEl.textContent.includes('h')) {
        playtimeEl.textContent += 'h';
      }
    }, 1250);
  }

  // CHANGED: Use lastUpdated instead of fetchedAt
  if (updatedEl && newStats.lastUpdated) {
    updatedEl.textContent = new Date(newStats.lastUpdated).toLocaleString();
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
      if (currentView === view) return;
      currentView = view;
      toggleButtons.forEach((b) => b.classList.remove('active'));
      btn.classList.add('active');
      renderCurrentView();
    });
  });
}

document.addEventListener('DOMContentLoaded', () => {
  try {
    new SimpleReveal();
    console.log('[App] SimpleReveal initialized');
  } catch (err) {
    console.warn('[App] SimpleReveal init failed', err);
    Array.from(document.querySelectorAll('.scroll-reveal')).forEach(el => el.classList.add('visible'));
  }
  try {
    setupViewToggles();
  } catch (err) {
    console.warn('[App] setupViewToggles failed', err);
  }
  updateAll(true).catch((e) => console.error('[App] initial updateAll failed', e));
  console.log('[App] Frontend initialized - data will only refresh on page reload');
});