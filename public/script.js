/* Frontend script — updated so the client requests members?fresh=1 synchronously
   and renders the fresh roster immediately (no persistent polling of members?cached=1).
   When online members are present we dispatch stats?fresh=1 (fire-and-forget, server returns 202)
   and poll stats?cached=1 a few times with backoff to pick up the new snapshot.
*/
(() => {
  // Config
  const MEMBERS_URL = '/members';
  const STATS_URL = '/stats';

  // Frontend polling intervals (keep in sync with worker env)
  const ONLINE_POLL_MS = 300_000;   // 5 minutes
  const OFFLINE_POLL_MS = 7_200_000; // 2 hours
  const SHORT_POLL_MS = 3000; // used for short waiting loops
  const STATS_SHORT_POLL_ATTEMPTS = 10; // how many short polls to attempt after dispatching stats job

  // Element IDs
  const ids = {
    membersCount: 'members-count',
    memberList: 'member-list-container',
    clears: 'total-count',
    prophecy: 'prophecy-count',
    updated: 'last-updated',
    recentActivityContent: 'recent-activity-content',
    recentActivitySource: 'recent-activity-source'
  };

  const $ = id => document.getElementById(id);
  const nf = new Intl.NumberFormat();

  // Keep members map for lookup by membershipId
  window.__membersById = new Map();

  // Track whether we've seen an online member recently (affects stats polling)
  window.__hasOnline = false;

  // Simple Reveal + Stagger (kept)
  class SimpleReveal {
    constructor() {
      this.revealObserver = null;
      this.staggerObserver = null;
      this.init();
    }
    init() {
      const revealOptions = { threshold: 0.06, rootMargin: '0px 0px -120px 0px' };
      this.revealObserver = new IntersectionObserver((entries) => {
        entries.forEach(en => {
          if (en.isIntersecting) {
            const el = en.target;
            const delay = Number(el.getAttribute('data-delay') || 0);
            setTimeout(() => el.classList.add('visible'), delay);
            this.revealObserver.unobserve(el);
          }
        });
      }, revealOptions);

      document.querySelectorAll('.scroll-reveal').forEach(el => {
        if (!el.hasAttribute('data-observed')) {
          this.revealObserver.observe(el);
          el.setAttribute('data-observed', 'true');
        }
      });

      const staggerOptions = { threshold: 0.06, rootMargin: '0px 0px -120px 0px' };
      this.staggerObserver = new IntersectionObserver((entries) => {
        entries.forEach(en => {
          if (en.isIntersecting) {
            const container = en.target;
            Array.from(container.children).forEach((child, i) => {
              child.classList.add('stagger-item');
              setTimeout(() => child.classList.add('visible'), i * 80 + (Number(container.getAttribute('data-delay') || 0)));
            });
            this.staggerObserver.unobserve(container);
          }
        });
      }, staggerOptions);

      document.querySelectorAll('.stagger-container').forEach(c => {
        if (!c.hasAttribute('data-stagger-observed')) {
          this.staggerObserver.observe(c);
          c.setAttribute('data-stagger-observed', 'true');
        }
      });
    }
    refresh() {
      document.querySelectorAll('.scroll-reveal').forEach(el => {
        if (!el.hasAttribute('data-observed')) {
          this.revealObserver.observe(el);
          el.setAttribute('data-observed', 'true');
        }
      });
      document.querySelectorAll('.stagger-container').forEach(c => {
        if (!c.hasAttribute('data-stagger-observed')) {
          this.staggerObserver.observe(c);
          c.setAttribute('data-stagger-observed', 'true');
        }
      });
    }
  }

  // Smooth counter
  function animateCounter(el, target, duration = 1200) {
    if (!el) return;
    target = Number(target) || 0;
    const start = Number(el.getAttribute('data-current') || el.textContent.replace(/[^\d]/g, '')) || 0;
    const startTime = performance.now();
    function step(now) {
      const elapsed = now - startTime;
      const t = Math.min(1, elapsed / duration);
      const eased = t < 0.5 ? 2 * t * t : -1 + (4 - 2 * t) * t;
      const value = Math.round(start + (target - start) * eased);
      el.textContent = nf.format(value);
      if (t < 1) requestAnimationFrame(step);
      else el.setAttribute('data-current', String(target));
    }
    requestAnimationFrame(step);
  }

  // Fetch helper (supports opts for headers)
  async function fetchJson(url, timeout = 7000, opts = {}) {
    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort(), timeout);
    try {
      const fetchOpts = { cache: 'no-store', signal: ctrl.signal, ...opts };
      const res = await fetch(url, fetchOpts);
      const text = await res.text().catch(() => null);
      let data = null;
      try { data = text ? JSON.parse(text) : null; } catch (e) { data = text; }
      if (!res.ok) return { ok: false, status: res.status, body: data ?? text ?? null };
      return { ok: true, data };
    } catch (err) {
      return { ok: false, status: 'network', body: String(err) };
    } finally {
      clearTimeout(timer);
    }
  }

  // Role helpers (unchanged)
  const ROLE_INFO = {
    5: { label: 'Founder', class: 'role-founder', color: '#ffd700', priority: 5 },
    4: { label: 'Acting Founder', class: 'role-acting-founder', color: '#ff8c00', priority: 4 },
    3: { label: 'Admin', class: 'role-admin', color: '#dc143c', priority: 3 },
    2: { label: 'Member', class: 'role-member', color: '#4169e1', priority: 2 },
    1: { label: 'Beginner', class: 'role-beginner', color: '#9aa0a6', priority: 1 }
  };
  function roleInfo(mt) { const n = Number(mt || 0); return ROLE_INFO[n] || { label: 'Member', class: 'role-member', color: '#4169e1', priority: 0 }; }
  function pickEmblemSrc(member) {
    const candidates = [member.emblemPath, member.emblemBackgroundPath, member.emblemUrl, member.emblem, member.emblem_path, member.emblem_background_path];
    for (const p of candidates) {
      if (!p) continue;
      if (typeof p !== 'string') continue;
      if (p.startsWith('http://') || p.startsWith('https://')) return p;
      if (p.startsWith('/')) return `https://www.bungie.net${p}`;
      return p;
    }
    return null;
  }

  // Rendering helpers (unchanged)
  function createSkeletonCard() { /* same as before */ const card = document.createElement('div'); card.className = 'member-item skeleton'; card.setAttribute('aria-hidden','true'); /* ... */ return card; }
  function showMembersLoading(count = 6) { const container = $(ids.memberList); if (!container) return; container.setAttribute('aria-busy','true'); container.innerHTML=''; for (let i=0;i<count;i++) container.appendChild(createSkeletonCard()); }
  function clearMembersLoading(){ const container = $(ids.memberList); if(!container) return; container.removeAttribute('aria-busy'); const s = container.querySelectorAll('.skeleton'); s.forEach(x=>x.remove()); }

  function createMemberCard(m) { /* same as before but ensure no reliance on mostRecentActivity.period */ 
    const card = document.createElement('div'); card.className = 'member-item';
    const r = roleInfo(m.memberType ?? m.member_type); card.style.setProperty('--role-color', r.color);
    const content = document.createElement('div'); content.className = 'member-item-content';
    const top = document.createElement('div'); top.className = 'member-top';
    const leftContainer = document.createElement('div'); leftContainer.style.display='flex'; leftContainer.style.alignItems='center'; leftContainer.style.gap='10px'; leftContainer.style.flex='1'; leftContainer.style.minWidth='0';
    const emblemSrc = pickEmblemSrc(m);
    if (emblemSrc) {
      const emblemWrapper = document.createElement('div');
      emblemWrapper.style.width='56px'; emblemWrapper.style.height='56px'; emblemWrapper.style.flex='0 0 56px';
      emblemWrapper.style.display='inline-block'; emblemWrapper.style.overflow='hidden'; emblemWrapper.style.borderRadius='8px';
      emblemWrapper.style.background='linear-gradient(180deg, rgba(0,0,0,0.06), rgba(0,0,0,0.02))';
      const emblemImg = document.createElement('img'); emblemImg.alt=''; emblemImg.decoding='async'; emblemImg.width=56; emblemImg.height=56; emblemImg.style.display='block'; emblemImg.style.width='100%'; emblemImg.style.height='100%'; emblemImg.style.objectFit='cover';
      emblemImg.src = emblemSrc; emblemWrapper.appendChild(emblemImg); leftContainer.appendChild(emblemWrapper);
    }
    const left = document.createElement('div'); left.className='member-left';
    const name = document.createElement('div'); name.className='member-name'; name.textContent = m.supplementalDisplayName ?? m.displayName ?? m.display_name ?? String(m.membershipId ?? m.membership_id ?? '');
    name.title = name.textContent; left.appendChild(name);
    if (m.joinDate || m.join_date) { const dateVal = m.joinDate ?? m.join_date; const d = new Date(dateVal); const supp = document.createElement('div'); supp.className='member-supp'; const dateStr = Number.isNaN(d) ? String(dateVal) : d.toLocaleDateString(); supp.textContent = `Joined: ${dateStr}`; supp.title = `Joined: ${dateStr}`; left.appendChild(supp); }
    leftContainer.appendChild(left);
    const right = document.createElement('div'); right.className='member-right';
    const isOnline = Boolean(m.isOnline ?? m.online ?? m.is_online ?? false);
    const statusWrap = document.createElement('div'); statusWrap.className='status-wrap';
    const statusDot = document.createElement('span'); statusDot.className = `status-dot ${isOnline ? 'online' : 'offline'}`; statusDot.setAttribute('aria-hidden','true'); statusDot.title = isOnline ? 'Online' : 'Offline';
    const statusText = document.createElement('div'); statusText.className='status-text'; statusText.textContent = isOnline ? 'Online' : 'Offline'; statusText.setAttribute('aria-label', isOnline ? 'Online' : 'Offline');
    statusWrap.appendChild(statusDot); statusWrap.appendChild(statusText); right.appendChild(statusWrap);
    top.appendChild(leftContainer); top.appendChild(right); content.appendChild(top); card.appendChild(content); return card;
  }

  function renderMembers(payload) {
    const container = $(ids.memberList); const membersCountEl = $(ids.membersCount);
    if (!container) return; clearMembersLoading(); container.innerHTML = '';
    if (payload && payload.ok === false) { const err = document.createElement('div'); err.className='member-item'; err.textContent = `Failed to load members: ${payload.body ?? payload.status ?? 'error'}`; container.appendChild(err); if (membersCountEl) membersCountEl.textContent='—'; return; }
    let members = [];
    if (Array.isArray(payload)) members = payload;
    else if (payload && Array.isArray(payload.members)) members = payload.members;
    else if (payload && Array.isArray(payload.data)) members = payload.data;
    else if (payload && payload.ok === true && Array.isArray(payload.members)) members = payload.members;
    if (!members || members.length === 0) { const empty = document.createElement('div'); empty.className='member-item'; empty.textContent='No members available.'; container.appendChild(empty); if (membersCountEl) membersCountEl.textContent='—'; return; }
    members.sort((a,b)=>{ const ra=roleInfo(a.memberType ?? a.member_type); const rb=roleInfo(b.memberType ?? b.member_type); if (rb.priority!==ra.priority) return rb.priority-ra.priority; const na=(a.supplementalDisplayName ?? a.displayName ?? '').toString().toLowerCase(); const nb=(b.supplementalDisplayName ?? b.displayName ?? '').toString().toLowerCase(); return na.localeCompare(nb); });
    window.__membersById.clear();
    for (const m of members) { const id = String(m.membershipId ?? m.membership_id ?? ''); if (id) window.__membersById.set(id,m); container.appendChild(createMemberCard(m)); }
    if (membersCountEl) { membersCountEl.textContent = nf.format(members.length); membersCountEl.setAttribute('data-target', members.length); membersCountEl.setAttribute('data-animated','false'); }
    if (window.simpleReveal instanceof SimpleReveal) window.simpleReveal.refresh();
  }

  // Render stats - unchanged (uses cached snapshot)
  function renderStats(d) {
    const elClears = $(ids.clears); const elProphecy = $(ids.prophecy); const elUpdated = $(ids.updated); const elMembersCount = $(ids.membersCount);
    if (!d) { if (elClears) elClears.textContent='—'; if (elProphecy) elProphecy.textContent='—'; if (elUpdated) elUpdated.textContent='never'; renderRecentActivity(null); return; }
    if (elClears && typeof d.clears !== 'undefined') { elClears.textContent = nf.format(d.clears); elClears.setAttribute('data-target', d.clears); elClears.setAttribute('data-animated','false'); }
    if (elProphecy && typeof d.prophecyClears !== 'undefined') { elProphecy.textContent = nf.format(d.prophecyClears); elProphecy.setAttribute('data-target', d.prophecyClears); elProphecy.setAttribute('data-animated','false'); }
    if (elMembersCount && typeof d.memberCount !== 'undefined') { elMembersCount.textContent = nf.format(d.memberCount); elMembersCount.setAttribute('data-target', d.memberCount); elMembersCount.setAttribute('data-animated','false'); }
    if (elUpdated) { const u = d.updated || d.fetchedAt || d.fetched || null; elUpdated.textContent = u ? new Date(u).toLocaleString() : 'never'; }
    renderRecentActivity(d && d.mostRecentClanActivity ? d.mostRecentClanActivity : null);
    setTimeout(observeAndAnimateStats, 100);
  }

  // Render a single most recent clan activity across the clan into the stats area
  function renderRecentActivity(mostRecent) {
    const elContent = $(ids.recentActivityContent);
    const elSource = $(ids.recentActivitySource);
    if (!elContent) return;
    if (!mostRecent || !mostRecent.instanceId) {
      elContent.innerHTML = '<div class="placeholder">No recent clan activity found.</div>';
      if (elSource) elSource.textContent = '';
      return;
    }
    const membershipIds = Array.isArray(mostRecent.membershipIds) ? mostRecent.membershipIds : (mostRecent.membershipId ? [mostRecent.membershipId] : []);
    const member = membershipIds.length ? window.__membersById.get(String(membershipIds[0])) : null;
    const memberName = member ? (member.supplementalDisplayName ?? member.displayName ?? member.display_name ?? String(membershipIds[0])) : (membershipIds.length ? `Member ${membershipIds[0]}` : 'Unknown member');
    elContent.innerHTML = '';
    const mainLine = document.createElement('div'); mainLine.style.fontWeight='700'; mainLine.textContent = `${memberName} — recent instance: ${mostRecent.instanceId}`; elContent.appendChild(mainLine);
    const btn = document.createElement('button'); btn.textContent='View details'; btn.className='btn-mini'; btn.style.marginTop='8px'; btn.style.padding='6px 10px'; btn.style.borderRadius='8px'; btn.style.border='1px solid rgba(0,0,0,0.08)'; btn.style.background='linear-gradient(180deg,#fff,#f7efe0)';
    btn.onclick = () => fetchAndShowPGCR(mostRecent.instanceId, membershipIds.map(id => (window.__membersById.get(String(id)) || {}).supplementalDisplayName || `Member ${id}`).join(', '));
    elContent.appendChild(btn);
    if (elSource) elSource.textContent = mostRecent.source ? `source: ${mostRecent.source}` : '';
  }

  function observeAndAnimateStats() {
    const statEls = Array.from(document.querySelectorAll('.stat-card .stat-value'));
    if (!statEls.length) return;
    const opts = { threshold: 0.5, rootMargin: '0px 0px -10px 0px' };
    const obs = new IntersectionObserver((entries, o) => {
      entries.forEach(en => {
        if (en.isIntersecting) {
          const el = en.target;
          if (el.getAttribute('data-animated') === 'true') { o.unobserve(el); return; }
          const targetAttr = el.getAttribute('data-target');
          const target = targetAttr ? Number(targetAttr) : Number(el.textContent.replace(/[^\d]/g, '')) || 0;
          animateCounter(el, target, 1200);
          el.setAttribute('data-animated','true');
          o.unobserve(el);
        }
      });
    }, opts);
    statEls.forEach(el => obs.observe(el));
  }

  // NEW: loadMembers now calls members?fresh=1 synchronously (server returns fresh roster + diff).
  // This prevents repeated polling of members?cached=1.
  async function loadMembers() {
    showMembersLoading(8);

    // 1) show cached quickly if available
    const cached = await fetchJson(`${MEMBERS_URL}?cached=1`, 4000);
    if (cached.ok && cached.data && Array.isArray(cached.data.members)) {
      renderMembers(cached.data);
    }

    // 2) request fresh roster synchronously (server returns fresh members + diff quickly)
    let fresh = null;
    try {
      const res = await fetchJson(`${MEMBERS_URL}?fresh=1`, 20000);
      if (res.ok && res.data) {
        // res.data expected: { ok: true, members: [...], diff: {...} }
        fresh = res.data;
      } else {
        // fallback: if server returned cached shape, try to use it
        fresh = null;
      }
    } catch (e) {
      fresh = null;
    }

    // 3) If fresh roster returned, render it immediately and avoid polling members?cached=1 repeatedly
    if (fresh && Array.isArray(fresh.members)) {
      renderMembers({ members: fresh.members });
      // update online flag
      const online = fresh.members.some(m => !!(m.isOnline ?? m.is_online ?? m.online));
      window.__hasOnline = online;

      // If online members present, dispatch stats job and poll cached stats a few times
      if (online) {
        try {
          // dispatch stats job (fire-and-forget)
          await fetchJson(`${STATS_URL}?fresh=1`, 4000);
        } catch (e) {
          // ignore dispatch error; we'll continue to poll cached snapshot
        }

        // Poll cached stats a few times with SHORT_POLL_MS interval to pick up the new snapshot if the backend finishes quickly
        let attempt = 0;
        const baseline = await fetchJson(`${STATS_URL}?cached=1`, 4000);
        const baselineFetched = baseline.ok && baseline.data ? (baseline.data.fetchedAt || baseline.data.fetched || null) : null;
        while (attempt < STATS_SHORT_POLL_ATTEMPTS) {
          await new Promise(r => setTimeout(r, SHORT_POLL_MS));
          const pol = await fetchJson(`${STATS_URL}?cached=1`, 4000);
          if (!pol.ok || !pol.data) { attempt++; continue; }
          const newFetched = pol.data.fetchedAt || pol.data.fetched || null;
          if (baselineFetched && newFetched && baselineFetched !== newFetched) {
            renderStats(pol.data);
            break;
          }
          // also consider changes in clears number itself
          if (baseline.ok && baseline.data && typeof baseline.data.clears !== 'undefined' && typeof pol.data.clears !== 'undefined' && Number(baseline.data.clears) !== Number(pol.data.clears)) {
            renderStats(pol.data); break;
          }
          attempt++;
        }
      }

      return;
    }

    // 4) If fresh failed, we already showed cached; nothing else to do (no polling loop)
    // keep window.__hasOnline as-is (from cached or previous state)
  }

  // Stats updater reads cached snapshot for display and reschedules itself based on online/offline
  async function updateStats() {
    try {
      const cached = await fetchJson(`${STATS_URL}?cached=1`, 7000);
      if (cached.ok && cached.data) renderStats(cached.data);
      else renderStats(null);
    } catch (e) {
      console.warn('updateStats error', e && e.message ? e.message : e);
    } finally {
      const nextInterval = window.__hasOnline ? ONLINE_POLL_MS : OFFLINE_POLL_MS;
      setTimeout(updateStats, nextInterval);
    }
  }

  // fetch PGCR and show a simple modal with summary (unchanged)
  async function fetchAndShowPGCR(instanceId, memberNames) { /* same as previous implementation */ }

  function escapeHtml(s){ if (s == null) return ''; return String(s).replace(/[&<>"']/g, c => ({ '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;' }[c])); }

  document.addEventListener('DOMContentLoaded', () => {
    window.simpleReveal = new SimpleReveal();
    loadMembers().catch(err => console.warn('loadMembers error', err));
    updateStats().catch(err => console.warn('updateStats error', err));
  });

  // Debug helpers
  window.__cheapRaid = {
    fetchCachedMembers: async () => fetchJson(`${MEMBERS_URL}?cached=1`),
    fetchFreshMembers: async () => {
      try {
        const res = await fetchJson(`${MEMBERS_URL}?fresh=1`, 20000);
        return { ok: res.ok, body: res.data ?? res };
      } catch (e) { return { ok: false, error: String(e) }; }
    },
    fetchCachedStats: async () => fetchJson(`${STATS_URL}?cached=1`),
    fetchFreshStats: async (wait=false) => {
      if (wait) return fetchJson(`${STATS_URL}?fresh=1&sync=1`, 120000, { headers: { 'x-wait': '1' } });
      return fetchJson(`${STATS_URL}?fresh=1`, 7000);
    }
  };
})();