document.addEventListener('DOMContentLoaded', () => {
  if (window.loadStats) window.loadStats();
  if (window.loadMembers) window.loadMembers();
  if (window.loadRecentActivities) window.loadRecentActivities();
  document.querySelectorAll('.scroll-reveal').forEach(el => {
    const d = Number(el.getAttribute('data-delay')) || 0;
    if (d > 0) setTimeout(() => el.classList.add('visible'), d);
    else el.classList.add('visible');
  });
});