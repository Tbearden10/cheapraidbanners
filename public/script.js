// Fixed frontend script: uses cached stats for display, triggers background refresh conditionally,
// polls cached snapshot until updated, preserves animations and joinDate display.
// Adds rendering for single most recent clan activity across clan (snapshot.mostRecentClanActivity)
// and PGCR modal fetch.

(() => {
  // Config
  const MEMBERS_URL = '/members';
  const STATS_URL = '/stats';
  const POLL_MS = 120_000;

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

  // Simple Reveal + Stagger (kept from your original)
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

  // Fetch helper
  async function fetchJson(url, timeout = 7000) {
    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort(), timeout);
    try {
      const res = await fetch(url, { cache: 'no-store', signal: ctrl.signal });
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

  // Skeletons and member rendering preserved (joinDate respected)
  function createSkeletonCard() {
    const card = document.createElement('div');
    card.className = 'member-item skeleton';
    card.setAttribute('aria-hidden', 'true');
    const content = document.createElement('div'); content.className = 'member-item-content';
    const top = document.createElement('div'); top.className = 'member-top';
    top.style.display = 'flex'; top.style.justifyContent = 'space-between'; top.style.alignItems = 'center';
    const left = document.createElement('div'); left.style.display = 'flex'; left.style.alignItems = 'center'; left.style.gap = '10px';
    const emblemPlaceholder = document.createElement('div'); emblemPlaceholder.style.width = '56px'; emblemPlaceholder.style.height = '56px'; emblemPlaceholder.style.borderRadius = '8px'; emblemPlaceholder.style.background = 'linear-gradient(90deg,#e9e9e9 0%,#f2f2f2 50%,#e9e9e9 100%)'; emblemPlaceholder.style.flex = '0 0 56px';
    const textPlaceholder = document.createElement('div'); textPlaceholder.style.display = 'flex'; textPlaceholder.style.flexDirection = 'column'; textPlaceholder.style.gap = '8px';
    const namePlaceholder = document.createElement('div'); namePlaceholder.style.width = '140px'; namePlaceholder.style.height = '14px'; namePlaceholder.style.borderRadius = '6px'; namePlaceholder.style.background = 'linear-gradient(90deg,#e9e9e9 0%,#f2f2f2 50%,#e9e9e9 100%)';
    const suppPlaceholder = document.createElement('div'); suppPlaceholder.style.width = '100px'; suppPlaceholder.style.height = '12px'; suppPlaceholder.style.borderRadius = '6px'; suppPlaceholder.style.background = 'linear-gradient(90deg,#eaeaea 0%,#f6f6f6 50%,#eaeaea 100%)';
    textPlaceholder.appendChild(namePlaceholder); textPlaceholder.appendChild(suppPlaceholder); left.appendChild(emblemPlaceholder); left.appendChild(textPlaceholder);
    const right = document.createElement('div'); right.style.display = 'flex'; right.style.alignItems = 'center'; right.style.gap = '8px';
    const statusDotPlaceholder = document.createElement('div'); statusDotPlaceholder.style.width = '14px'; statusDotPlaceholder.style.height = '14px'; statusDotPlaceholder.style.borderRadius = '50%'; statusDotPlaceholder.style.background = '#e9e9e9';
    const statusTextPlaceholder = document.createElement('div'); statusTextPlaceholder.style.width = '54px'; statusTextPlaceholder.style.height = '12px'; statusTextPlaceholder.style.borderRadius = '6px'; statusTextPlaceholder.style.background = 'linear-gradient(90deg,#eaeaea 0%,#f6f6f6 50%,#eaeaea 100%)';
    right.appendChild(statusDotPlaceholder); right.appendChild(statusTextPlaceholder);
    top.appendChild(left); top.appendChild(right); content.appendChild(top); card.appendChild(content); return card;
  }

  function showMembersLoading(count = 6) {
    const container = $(ids.memberList);
    if (!container) return;
    container.setAttribute('aria-busy', 'true');
    container.innerHTML = '';
    for (let i = 0; i < count; i++) container.appendChild(createSkeletonCard());
  }

  function clearMembersLoading() {
    const container = $(ids.memberList);
    if (!container) return;
    container.removeAttribute('aria-busy');
    const skeletons = container.querySelectorAll('.skeleton');
    skeletons.forEach(s => s.remove());
  }

  function createMemberCard(m) {
    const card = document.createElement('div'); card.className = 'member-item';
    const r = roleInfo(m.memberType ?? m.member_type); card.style.setProperty('--role-color', r.color);
    const content = document.createElement('div'); content.className = 'member-item-content';
    const top = document.createElement('div'); top.className = 'member-top';
    const leftContainer = document.createElement('div'); leftContainer.style.display = 'flex'; leftContainer.style.alignItems = 'center'; leftContainer.style.gap = '10px'; leftContainer.style.flex = '1'; leftContainer.style.minWidth = '0';
    const emblemSrc = pickEmblemSrc(m);
    if (emblemSrc) {
      const emblemWrapper = document.createElement('div'); emblemWrapper.style.width = '56px'; emblemWrapper.style.height = '56px'; emblemWrapper.style.flex = '0 0 56px'; emblemWrapper.style.display = 'inline-block'; emblemWrapper.style.overflow = 'hidden'; emblemWrapper.style.borderRadius = '8px'; emblemWrapper.style.background = 'linear-gradient(180deg, rgba(0,0,0,0.06), rgba(0,0,0,0.02))';
      const emblemImg = document.createElement('img'); emblemImg.alt = ''; emblemImg.decoding = 'async'; emblemImg.width = 56; emblemImg.height = 56; emblemImg.style.display = 'block'; emblemImg.style.width = '100%'; emblemImg.style.height = '100%'; emblemImg.style.objectFit = 'cover'; emblemImg.src = emblemSrc;
      emblemWrapper.appendChild(emblemImg); leftContainer.appendChild(emblemWrapper);
    }
    const left = document.createElement('div'); left.className = 'member-left';
    const name = document.createElement('div'); name.className = 'member-name'; name.textContent = m.supplementalDisplayName ?? m.displayName ?? m.display_name ?? String(m.membershipId ?? m.membership_id ?? ''); name.title = name.textContent; left.appendChild(name);
    if (m.joinDate || m.join_date) { const dateVal = m.joinDate ?? m.join_date; const d = new Date(dateVal); const supp = document.createElement('div'); supp.className = 'member-supp'; const dateStr = Number.isNaN(d) ? String(dateVal) : d.toLocaleDateString(); supp.textContent = `Joined: ${dateStr}`; supp.title = `Joined: ${dateStr}`; left.appendChild(supp); }
    // show per-member mostRecentActivity if present (small line)
    if (m.mostRecentActivity && m.mostRecentActivity.period) {
      const ma = m.mostRecentActivity;
      const maDate = new Date(ma.period);
      const maDateStr = Number.isNaN(maDate) ? String(ma.period) : maDate.toLocaleDateString();
      const actEl = document.createElement('div'); actEl.className = 'member-activity-snip'; actEl.style.fontSize = '12px'; actEl.style.marginTop = '6px';
      actEl.textContent = `Recent clan run: ${maDateStr} — ${ma.clanMemberCount ?? ma.clanMembers ?? ma.clan_members ?? 'N/A'} clan mates`;
      left.appendChild(actEl);
    }
    leftContainer.appendChild(left);
    const right = document.createElement('div'); right.className = 'member-right';
    const isOnline = Boolean(m.isOnline ?? m.online ?? m.is_online ?? false);
    const statusWrap = document.createElement('div'); statusWrap.className = 'status-wrap';
    const statusDot = document.createElement('span'); statusDot.className = `status-dot ${isOnline ? 'online' : 'offline'}`; statusDot.setAttribute('aria-hidden', 'true'); statusDot.title = isOnline ? 'Online' : 'Offline';
    const statusText = document.createElement('div'); statusText.className = 'status-text'; statusText.textContent = isOnline ? 'Online' : 'Offline'; statusText.setAttribute('aria-label', isOnline ? 'Online' : 'Offline');
    statusWrap.appendChild(statusDot); statusWrap.appendChild(statusText); right.appendChild(statusWrap);
    top.appendChild(leftContainer); top.appendChild(right); content.appendChild(top); card.appendChild(content); return card;
  }

  // Replace the existing renderMembers and updateStats functions with these versions:

function renderMembers(payload) {
  const container = $(ids.memberList); const membersCountEl = $(ids.membersCount);
  if (!container) return; clearMembersLoading(); container.innerHTML = '';
  if (payload && payload.ok === false) { const err = document.createElement('div'); err.className = 'member-item'; err.textContent = `Failed to load members: ${payload.body ?? payload.status ?? 'error'}`; container.appendChild(err); if (membersCountEl) membersCountEl.textContent = '—'; 
    // treat as no members / offline
    window.__hasOnline = false;
    return;
  }
  let members = [];
  if (Array.isArray(payload)) members = payload;
  else if (payload && Array.isArray(payload.members)) members = payload.members;
  else if (payload && Array.isArray(payload.data)) members = payload.data;
  else if (payload && payload.ok === true && Array.isArray(payload.members)) members = payload.members;
  if (!members || members.length === 0) { const empty = document.createElement('div'); empty.className = 'member-item'; empty.textContent = 'No members available.'; container.appendChild(empty); if (membersCountEl) membersCountEl.textContent = '—'; 
    window.__hasOnline = false;
    return; 
  }
  members.sort((a, b) => { const ra = roleInfo(a.memberType ?? a.member_type); const rb = roleInfo(b.memberType ?? b.member_type); if (rb.priority !== ra.priority) return rb.priority - ra.priority; const na = (a.supplementalDisplayName ?? a.displayName ?? '').toString().toLowerCase(); const nb = (b.supplementalDisplayName ?? b.displayName ?? '').toString().toLowerCase(); return na.localeCompare(nb); });
  // update members map for lookup by membershipId
  window.__membersById.clear();
  // compute online flag
  let anyOnline = false;
  for (const m of members) {
    const id = String(m.membershipId ?? m.membership_id ?? '');
    if (id) window.__membersById.set(id, m);
    if (!anyOnline && !!(m.isOnline ?? m.online ?? m.is_online)) anyOnline = true;
    container.appendChild(createMemberCard(m));
  }
  // mark online state and snapshot presence
  window.__hasOnline = Boolean(anyOnline);
  window.__hasSnapshot = true;

  if (membersCountEl) { membersCountEl.textContent = nf.format(members.length); membersCountEl.setAttribute('data-target', members.length); membersCountEl.setAttribute('data-animated', 'false'); }
  if (window.simpleReveal instanceof SimpleReveal) window.simpleReveal.refresh();
}

  // Render stats
  function renderStats(d) {
    const elClears = $(ids.clears); const elProphecy = $(ids.prophecy); const elUpdated = $(ids.updated); const elMembersCount = $(ids.membersCount);
    if (!d) { if (elClears) elClears.textContent = '—'; if (elProphecy) elProphecy.textContent = '—'; if (elUpdated) elUpdated.textContent = 'never'; renderRecentActivity(null); return; }
    if (elClears && typeof d.clears !== 'undefined') { elClears.textContent = nf.format(d.clears); elClears.setAttribute('data-target', d.clears); elClears.setAttribute('data-animated', 'false'); }
    if (elProphecy && typeof d.prophecyClears !== 'undefined') { elProphecy.textContent = nf.format(d.prophecyClears); elProphecy.setAttribute('data-target', d.prophecyClears); elProphecy.setAttribute('data-animated', 'false'); }
    if (elMembersCount && typeof d.memberCount !== 'undefined') { elMembersCount.textContent = nf.format(d.memberCount); elMembersCount.setAttribute('data-target', d.memberCount); elMembersCount.setAttribute('data-animated', 'false'); }
    if (elUpdated) { const u = d.updated || d.fetchedAt || d.fetched || null; elUpdated.textContent = u ? new Date(u).toLocaleString() : 'never'; }
    // render global most recent clan activity if present
    renderRecentActivity(d && d.mostRecentClanActivity ? d.mostRecentClanActivity : null);
    setTimeout(observeAndAnimateStats, 100);
  }

  // Render a single most recent clan activity across the clan into the stats area
  function renderRecentActivity(mostRecent) {
    const elContent = $(ids.recentActivityContent);
    const elSource = $(ids.recentActivitySource);
    if (!elContent) return;
    if (!mostRecent) {
      elContent.innerHTML = '<div class="placeholder">No recent clan activity found.</div>';
      if (elSource) elSource.textContent = '';
      return;
    }

    // expected shape: { instanceId, activity: { period, clanMemberCount, mode, activityHash }, membershipIds: [], source }
    const membershipIds = Array.isArray(mostRecent.membershipIds) ? mostRecent.membershipIds : (mostRecent.membershipId ? [mostRecent.membershipId] : []);
    const activity = mostRecent.activity ?? mostRecent;
    const period = activity && (activity.period ?? activity.activityPeriod ?? activity.timestamp ?? activity.time) ? (activity.period ?? activity.activityPeriod ?? activity.timestamp ?? activity.time) : null;
    const clanCount = activity && (activity.clanMemberCount ?? activity.clanMembers ?? activity.clan_members ?? activity.clanCount ?? null);
    const mode = activity && (activity.mode ?? activity.activityMode ?? activity.activity_mode ?? null);
    const instanceId = mostRecent.instanceId || activity?.instanceId || null;

    const member = membershipIds.length ? window.__membersById.get(String(membershipIds[0])) : null;
    const memberName = member ? (member.supplementalDisplayName ?? member.displayName ?? member.display_name ?? String(membershipIds[0])) : (membershipIds.length ? `Member ${membershipIds[0]}` : 'Unknown member');

    const dateLabel = period ? (Number.isNaN(new Date(period) ? NaN : new Date(period)) ? String(period) : new Date(period).toLocaleString()) : 'Unknown time';
    const clanLabel = clanCount != null ? `${clanCount} clan mate${Number(clanCount) === 1 ? '' : 's'}` : `${membershipIds.length} member${membershipIds.length === 1 ? '' : 's'}`;
    const modeLabel = mode ? ` — mode ${String(mode)}` : '';

    elContent.innerHTML = '';
    const mainLine = document.createElement('div');
    mainLine.style.fontWeight = '700';
    mainLine.textContent = `${memberName} — ${dateLabel} — ${clanLabel}${modeLabel}`;
    elContent.appendChild(mainLine);

    if (instanceId) {
      const btn = document.createElement('button');
      btn.textContent = 'View details';
      btn.className = 'btn-mini';
      btn.style.marginTop = '8px';
      btn.style.padding = '6px 10px';
      btn.style.borderRadius = '8px';
      btn.style.border = '1px solid rgba(0,0,0,0.08)';
      btn.style.background = 'linear-gradient(180deg,#fff,#f7efe0)';
      btn.onclick = () => fetchAndShowPGCR(instanceId, membershipIds.map(id => (window.__membersById.get(String(id)) || {}).supplementalDisplayName || `Member ${id}`).join(', '));
      elContent.appendChild(btn);
    }

    if (elSource) {
      elSource.textContent = mostRecent.source ? `source: ${mostRecent.source}` : '';
    }
  }

  function observeAndAnimateStats() {
    const statEls = Array.from(document.querySelectorAll('.stat-card .stat-value'));
    if (!statEls.length) return;

    const opts = { threshold: 0.5, rootMargin: '0px 0px -10px 0px' };
    const obs = new IntersectionObserver((entries, o) => {
      entries.forEach(en => {
        if (en.isIntersecting) {
          const el = en.target;
          if (el.getAttribute('data-animated') === 'true') {
            o.unobserve(el);
            return;
          }
          const targetAttr = el.getAttribute('data-target');
          const target = targetAttr ? Number(targetAttr) : Number(el.textContent.replace(/[^\d]/g, '')) || 0;
          animateCounter(el, target, 1200);
          el.setAttribute('data-animated', 'true');
          o.unobserve(el);
        }
      });
    }, opts);

    statEls.forEach(el => obs.observe(el));
  }

  // Load members: cached then fresh; if online users exist, trigger stats?fresh=1 and poll cached snapshot
  async function loadMembers() {
    showMembersLoading(8);

    const cached = await fetchJson(`${MEMBERS_URL}?cached=1`, 7000);
    if (cached.ok && cached.data && Array.isArray(cached.data.members)) {
      renderMembers(cached.data);
    }

    const fresh = await fetchJson(`${MEMBERS_URL}?fresh=1`, 20000);
    if (fresh.ok && fresh.data && Array.isArray(fresh.data.members)) {
      renderMembers(fresh.data);
      const online = fresh.data.members.some(m => !!(m.isOnline ?? m.is_online ?? m.online));
      if (online) {
        // request a fresh stats run (server will return cached immediately and start background work)
        await fetchJson(`${STATS_URL}?fresh=1`, 7000).catch(() => {});
        // poll cached snapshot until it changes (short timeout)
        const baselineResp = await fetchJson(`${STATS_URL}?cached=1`, 4000);
        const baseline = baselineResp.ok && baselineResp.data ? baselineResp.data : null;
        const start = Date.now();
        while (Date.now() - start < 60000) {
          await new Promise(r => setTimeout(r, 3000));
          const pol = await fetchJson(`${STATS_URL}?cached=1`, 4000);
          if (!pol.ok || !pol.data) continue;
          const baseFetched = baseline && (baseline.fetchedAt || baseline.fetched) ? (baseline.fetchedAt || baseline.fetched) : null;
          const newFetched = pol.data && (pol.data.fetchedAt || pol.data.fetched) ? (pol.data.fetchedAt || pol.data.fetched) : null;
          if (baseFetched && newFetched && baseFetched !== newFetched) { renderStats(pol.data); break; }
          if (baseline && typeof baseline.clears !== 'undefined' && typeof pol.data.clears !== 'undefined' && Number(baseline.clears) !== Number(pol.data.clears)) { renderStats(pol.data); break; }
        }
      }
    } else {
      if (!cached.ok || !cached.data || !Array.isArray(cached.data.members)) {
        await new Promise(r => setTimeout(r, 1000));
        const again = await fetchJson(`${MEMBERS_URL}?cached=1`, 4000);
        if (again.ok && again.data && Array.isArray(again.data.members)) renderMembers(again.data);
      }
    }
  }

  // Stats updater reads cached snapshot for display
  // Replace/update the existing updateStats function with this:

  async function updateStats() {
    // Don't poll aggressively while page is hidden
    if (typeof document !== 'undefined' && document.visibilityState === 'hidden') {
      // try again later, longer backoff
      setTimeout(updateStats, POLL_MS * 6);
      return;
    }

    // If we have a snapshot but no online members, poll very slowly
    const isOnline = Boolean(window.__hasOnline);
    const fastPoll = isOnline; // true = frequent (POLL_MS), false = slow

    const cached = await fetchJson(`${STATS_URL}?cached=1`, 7000).catch(() => ({ ok: false }));
    if (cached.ok && cached.data) {
      // mark that we have a snapshot so first-time loads still display it even if offline
      window.__hasSnapshot = true;
      renderStats(cached.data);
    } else {
      renderStats(null);
    }

    if (fastPoll) {
      setTimeout(updateStats, POLL_MS); // keep the original polling cadence when online
    } else {
      // slow down when everyone is offline: poll every 10 minutes (600k ms)
      // you can change this number if you prefer a different backoff
      setTimeout(updateStats, 600000);
    }
  }

  // fetch PGCR and show a simple modal with summary
  async function fetchAndShowPGCR(instanceId, memberNames) {
    const modalId = 'pgcr-modal';
    let modal = document.getElementById(modalId);
    if (!modal) {
      modal = document.createElement('div');
      modal.id = modalId;
      modal.style.position = 'fixed';
      modal.style.inset = '0';
      modal.style.display = 'flex';
      modal.style.alignItems = 'center';
      modal.style.justifyContent = 'center';
      modal.style.zIndex = 99999;
      modal.style.background = 'rgba(0,0,0,0.4)';
      modal.innerHTML = `<div id="pgcr-modal-box" style="max-width:760px; width:92%; background:#fffaf6; border-radius:12px; padding:18px; box-shadow:0 12px 36px rgba(0,0,0,0.4);"><button id="pgcr-close" style="float:right;border:none;background:transparent;font-weight:900;font-size:18px;cursor:pointer;">✕</button><div id="pgcr-modal-content" style="margin-top:4px;font-size:14px;color:#33221a;"></div></div>`;
      document.body.appendChild(modal);
      document.getElementById('pgcr-close').onclick = () => modal.remove();
      modal.addEventListener('click', (ev) => { if (ev.target === modal) modal.remove(); });
    }

    const contentEl = document.getElementById('pgcr-modal-content');
    contentEl.textContent = 'Loading activity details...';

    try {
      const res = await fetchJson(`/pgcr?instanceId=${encodeURIComponent(instanceId)}`, 10000);
      if (!res.ok || !res.data || !res.data.pgcr) {
        contentEl.textContent = `Failed to load PGCR: ${res?.data?.error ?? res?.status ?? 'unknown'}`;
        return;
      }
      const pgcr = res.data.pgcr;
      // Build summary: activityName, period, mode, list of participating clan members (matching membershipIds)
      const title = pgcr.activityName || (pgcr.raw?.Response?.activityDetails?.name ?? 'Activity');
      const period = pgcr.period || (pgcr.raw?.Response?.period ?? null);
      const entries = pgcr.entries || (pgcr.raw?.Response?.entries ?? []);
      const clanMembers = [];
      for (const e of entries) {
        const mid = e.membershipId ?? e.player?.destinyUserInfo?.membershipId ?? null;
        if (!mid) continue;
        const member = window.__membersById.get(String(mid));
        if (member) {
          const display = member.supplementalDisplayName ?? member.displayName ?? member.display_name ?? String(mid);
          clanMembers.push({ id: mid, name: display, values: e.values ?? e });
        }
      }

      let outHtml = `<div style="font-weight:900;margin-bottom:8px;font-size:16px;">${escapeHtml(title)}</div>`;
      outHtml += `<div style="color:#5a4436;margin-bottom:10px;">${period ? new Date(period).toLocaleString() : 'Time unknown'}</div>`;
      outHtml += `<div style="font-weight:800;margin-bottom:6px;">Clan members in this activity: ${clanMembers.length}</div>`;
      if (clanMembers.length) {
        outHtml += '<ul style="margin-left:16px; color:#3b2a20;">';
        for (const cm of clanMembers) {
          outHtml += `<li style="margin-bottom:6px;"><strong>${escapeHtml(cm.name)}</strong></li>`;
        }
        outHtml += '</ul>';
      } else {
        outHtml += '<div style="color:rgba(60,40,30,0.7);">No clan members found in PGCR entries.</div>';
      }

      contentEl.innerHTML = outHtml;
    } catch (err) {
      contentEl.textContent = `Error loading PGCR: ${String(err)}`;
    }
  }

  // small helper to escape user text
  function escapeHtml(s){ if (s == null) return ''; return String(s).replace(/[&<>"']/g, c => ({ '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;' }[c])); }

  document.addEventListener('DOMContentLoaded', () => {
    window.simpleReveal = new SimpleReveal();
    loadMembers().catch(err => console.warn('loadMembers error', err));
    updateStats().catch(err => console.warn('updateStats error', err));
  });

  // Debug helpers
  window.__cheapRaid = {
    fetchCachedMembers: async () => fetchJson(`${MEMBERS_URL}?cached=1`),
    fetchFreshMembers: async () => fetchJson(`${MEMBERS_URL}?fresh=1`),
    fetchCachedStats: async () => fetchJson(`${STATS_URL}?cached=1`),
    fetchFreshStats: async () => fetchJson(`${STATS_URL}?fresh=1`)
  };
})();