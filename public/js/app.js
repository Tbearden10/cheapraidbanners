// Main application initialization and polling
const POLL_INTERVAL = 30000; // 30 seconds
let currentView = 'cards';
let currentMembersData = null;
let currentStatsData = null;
let currentClearsData = null;

// Track what's currently being updated to prevent concurrent updates
let updating = {
  members: false,
  stats: false,
  clears: false,
};

// Cron job intervals
const CRON_INTERVAL_RECENT_CLEARS = 900000; // 1 minute

async function updateAll(forceRender = false) {
  // Load each independently to allow partial updates
  await Promise.allSettled([
    updateMembers(forceRender),
    updateStats(forceRender),
    updateRecentClears(forceRender),
  ]);
}

async function updateMembers(forceRender = false) {
  if (updating.members) return;
  updating.members = true;

  try {
    const membersData = await loadMembers(forceRender);

    // Only update if data changed
    if (membersData && dataHasChanged(membersData, currentMembersData)) {
      currentMembersData = membersData;

      // Update member count
      const countEl = $('members-count');
      if (countEl && membersData.members) {
        animateCounter(countEl, membersData.members.length);
      }

      // Re-render member stats if we have stats data
      if (currentStatsData) {
        renderCurrentView();
      }

      console.log('[App] Members updated');
    }
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
    if (statsData && dataHasChanged(statsData, currentStatsData)) {
      const oldStats = currentStatsData;
      currentStatsData = statsData;

      // Update stats bar (only animate values that changed)
      updateStatsBar(statsData, oldStats);

      // Re-render member stats with new data
      if (currentMembersData) {
        renderCurrentView();
      }

      // Update recent activities gallery
      renderRecentClears(statsData, forceRender);

      console.log('[App] Stats updated');
    }
  } finally {
    updating.stats = false;
  }
}

async function updateRecentClears(forceRender = false) {
  if (updating.clears) return;
  updating.clears = true;

  try {
    const clearsData = await fetchJson('/recent-clears');

    // Only update if data changed
    if (clearsData && dataHasChanged(clearsData, currentClearsData)) {
      currentClearsData = clearsData;
      renderRecentClears(clearsData, forceRender);
      console.log('[App] Recent clears updated');
    }
  } finally {
    updating.clears = false;
  }
}

function updateStatsBar(newStats, oldStats) {
  const dungeonEl = $('dungeon-count');
  const raidEl = $('raid-count');
  const playtimeEl = $('playtime');
  const updatedEl = $('last-updated');

  // Only animate if values actually changed
  if (dungeonEl && newStats.dungeonClears !== oldStats?.dungeonClears) {
    animateCounter(dungeonEl, newStats.dungeonClears);
  }

  if (raidEl && newStats.raidClears !== oldStats?.raidClears) {
    animateCounter(raidEl, newStats.raidClears);
  }

  if (playtimeEl && newStats.totalPlaytimeSeconds !== oldStats?.totalPlaytimeSeconds) {
    const hours = Math.floor(newStats.totalPlaytimeSeconds / 3600);
    animateCounter(playtimeEl, hours);
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
    renderMemberStatsBars(currentStatsData, currentMembersData);
  } else {
    renderMemberStats(currentStatsData, currentMembersData);
  }
}

function startPolling() {
  setInterval(() => {
    updateAll(false); // Never force render on poll
  }, POLL_INTERVAL);

  console.log('[App] Polling started - checking for updates every', POLL_INTERVAL / 1000, 'seconds');
}

function startCronJobs() {
  setInterval(() => {
    updateRecentClears(false); // Periodically update recent clears
  }, CRON_INTERVAL_RECENT_CLEARS);

  console.log(
    '[App] Cron job started - updating recent clears every',
    CRON_INTERVAL_RECENT_CLEARS / 1000,
    'seconds'
  );
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

document.addEventListener('DOMContentLoaded', () => {
  window.simpleReveal = new SimpleReveal();

  // Setup toggle buttons
  setupViewToggles();

  // Initial load with forced render
  updateAll(true);

  // Start polling for updates (non-blocking)
  startPolling();

  // Start cron jobs for periodic updates
  startCronJobs();

  console.log('[App] Frontend initialized');
});