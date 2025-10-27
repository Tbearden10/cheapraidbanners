const $ = id => document.getElementById(id);
const nf = new Intl.NumberFormat();

function escapeHtml(text) {
  const div = document.createElement('div');
  div.textContent = text;
  return div.innerHTML;
}

function formatDuration(seconds) {
  const mins = Math.floor(seconds / 60);
  const secs = seconds % 60;
  if (mins === 0) return `${secs}s`;
  return `${mins}m ${secs}s`;
}

function formatTimeAgo(date) {
  const seconds = Math.floor((new Date() - date) / 1000);
  
  if (seconds < 60) return 'Just now';
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`;
  if (seconds < 86400) return `${Math.floor(seconds / 3600)}h ago`;
  if (seconds < 604800) return `${Math.floor(seconds / 86400)}d ago`;
  
  return date.toLocaleDateString();
}

function dataHasChanged(newData, oldData) {
  if (!oldData && newData) return true;
  if (!newData) return false;
  
  try {
    return JSON.stringify(newData) !== JSON.stringify(oldData);
  } catch (e) {
    return true;
  }
}

async function fetchJson(url) {
  try {
    const res = await fetch(url);
    if (!res.ok) return null;
    return await res.json();
  } catch (err) {
    console.error('Fetch error:', err);
    return null;
  }
}

function animateCounter(el, target, duration = 1200) {
  if (!el) return;
  target = Number(target) || 0;
  const start = Number(el.textContent.replace(/[^\d]/g, '')) || 0;
  
  if (start === target) return;
  
  const startTime = performance.now();
  
  function step(now) {
    const elapsed = now - startTime;
    const progress = Math.min(1, elapsed / duration);
    const eased = progress < 0.5 ? 2 * progress * progress : -1 + (4 - 2 * progress) * progress;
    const value = Math.round(start + (target - start) * eased);
    el.textContent = nf.format(value);
    if (progress < 1) requestAnimationFrame(step);
  }
  requestAnimationFrame(step);
}

const ROLE_INFO = {
  5: { label: 'Founder', color: '#ffd700', priority: 5 },
  4: { label: 'Acting Founder', color: '#ff8c00', priority: 4 },
  3: { label: 'Admin', color: '#dc143c', priority: 3 },
  2: { label: 'Member', color: '#4169e1', priority: 2 },
  1: { label: 'Beginner', color: '#9aa0a6', priority: 1 }
};

function getRoleInfo(memberType) {
  const type = Number(memberType || 0);
  return ROLE_INFO[type] || { label: 'Member', color: '#4169e1', priority: 0 };
}

class SimpleReveal {
  constructor() {
    this.revealObserver = null;
    this.init();
  }
  
  init() {
    const options = { threshold: 0.06, rootMargin: '0px 0px -120px 0px' };
    this.revealObserver = new IntersectionObserver((entries) => {
      entries.forEach(en => {
        if (en.isIntersecting) {
          const el = en.target;
          const delay = Number(el.getAttribute('data-delay') || 0);
          setTimeout(() => el.classList.add('visible'), delay);
          this.revealObserver.unobserve(el);
        }
      });
    }, options);

    document.querySelectorAll('.scroll-reveal').forEach(el => {
      this.revealObserver.observe(el);
    });
  }
}