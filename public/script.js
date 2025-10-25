// Minimal script: poll /stats and member-list toggle with lazy-load.
// On page load we proactively fetch /members (cached KV) and update the Members stat immediately.
// When user opens the member list we reuse the cached data if present.

(() => {
  const STATS_URL = '/stats';
  const MEMBERS_URL = '/members';
  const POLL_MS = 60_000;

  const ids = {
    members: 'bungie-members',
    clears: 'bungie-clears',
    prophecy: 'prophecy-clears',
    updated: 'bungie-updated',
    toggleMembers: 'toggle-members',
    memberList: 'member-list',
    memberInner: 'member-list-inner',
    card: 'card',
    aboutExtra: 'about-extra'
  };

  const nf = new Intl.NumberFormat();
  let statsTimer = null;
  let membersLoaded = false;
  let cachedMembers = null; // store array when we fetch on load

  function $(id){ return document.getElementById(id); }

  async function fetchJson(url, timeout = 7000){
    const ctrl = new AbortController();
    const t = setTimeout(()=>ctrl.abort(), timeout);
    try{
      const res = await fetch(url, { cache: 'no-store', signal: ctrl.signal });
      clearTimeout(t);
      if (!res.ok) throw new Error('bad status');
      return await res.json();
    } finally { clearTimeout(t); }
  }

  async function updateStats(){
    try{
      const data = await fetchJson(STATS_URL).catch(()=>null);
      const m = data ? (data.memberCount ?? data.members ?? (Array.isArray(data.members)?data.members.length:undefined)) : undefined;
      const clears = data ? (data.clears ?? undefined) : undefined;
      const prophecy = data ? (data.prophecyClears ?? undefined) : undefined;
      const updated = data ? data.updated : undefined;

      if ($(ids.members)) $(ids.members).textContent = m != null ? nf.format(m) : '—';
      if ($(ids.clears)) $(ids.clears).textContent = clears != null ? nf.format(clears) : '—';
      if ($(ids.prophecy)) $(ids.prophecy).textContent = prophecy != null ? nf.format(prophecy) : '—';
      if ($(ids.updated) && updated) {
        const d = new Date(updated);
        if (!Number.isNaN(d)) $(ids.updated).textContent = d.toLocaleString();
      }
    } catch(e){
      console.warn('stats fetch failed', e);
    } finally {
      clearTimeout(statsTimer);
      statsTimer = setTimeout(updateStats, POLL_MS);
    }
  }

  // Fetch members and normalize payload shapes; returns { membersArray, memberCount }
  async function fetchMembersPayload() {
    try {
      const payload = await fetchJson(MEMBERS_URL).catch(()=>null);
      if (!payload) return { membersArray: null, memberCount: null };

      // Accept various shapes: array, { members: [...] }, { fetchedAt, members, memberCount }, or { data: [...] }
      let membersArray = null;
      if (Array.isArray(payload)) membersArray = payload;
      else if (payload && Array.isArray(payload.members)) membersArray = payload.members;
      else if (payload && Array.isArray(payload.data)) membersArray = payload.data;

      // If memberCount provided independently, use it. Otherwise derive from array.
      const providedCount = (payload && typeof payload.memberCount === 'number') ? payload.memberCount : null;
      const derivedCount = Array.isArray(membersArray) ? membersArray.length : null;
      const memberCount = providedCount != null ? providedCount : derivedCount;

      return { membersArray, memberCount };
    } catch (e) {
      console.warn('fetchMembersPayload failed', e);
      return { membersArray: null, memberCount: null };
    }
  }

  // Called on page load to populate member count immediately (no UI pop)
  async function prefetchMembersOnLoad() {
    try {
      const { membersArray, memberCount } = await fetchMembersPayload();
      if (Array.isArray(membersArray) && membersArray.length > 0) {
        cachedMembers = membersArray;
        membersLoaded = true; // indicates we have cached data
        const membersEl = $(ids.members);
        if (membersEl) membersEl.textContent = nf.format(membersArray.length);
      } else if (typeof memberCount === 'number') {
        // If server returned just memberCount, use it
        const membersEl = $(ids.members);
        if (membersEl) membersEl.textContent = nf.format(memberCount);
      }
    } catch (e) {
      // non-fatal
      console.warn('prefetchMembersOnLoad failed', e);
    }
  }

  async function toggleMembers(){
    const btn = $(ids.toggleMembers);
    const list = $(ids.memberList);
    const inner = $(ids.memberInner);
    const card = $(ids.card);
    const aboutExtra = $(ids.aboutExtra);
    const open = btn.getAttribute('aria-expanded') === 'true';
    if (open){
      btn.setAttribute('aria-expanded','false'); btn.textContent = '▶';
      list.setAttribute('aria-hidden','true'); list.style.display = 'none';
      if (aboutExtra){ aboutExtra.hidden = true; aboutExtra.style.display = 'none'; }
      card.classList.remove('members-open');
      return;
    }

    btn.setAttribute('aria-expanded','true'); btn.textContent = '◀';
    list.setAttribute('aria-hidden','false'); list.style.display = '';
    if (aboutExtra){ aboutExtra.hidden = false; aboutExtra.style.display = 'block'; }
    card.classList.add('members-open');

    // If we already have cached members from prefetch, render them
    const innerClearAndRender = (membersArray) => {
      inner.textContent = '';
      if (!Array.isArray(membersArray) || membersArray.length === 0) {
        inner.textContent = 'No members';
        return;
      }
      for (const m of membersArray){
        const d = document.createElement('div');
        d.textContent = String(m.displayName ?? m.name ?? m.membershipId ?? m.id ?? '');
        inner.appendChild(d);
      }
    };

    // if cachedMembers exists, use it and avoid network
    if (membersLoaded && Array.isArray(cachedMembers)) {
      innerClearAndRender(cachedMembers);
      return;
    }

    // otherwise fetch now, cache and render
    membersLoaded = true;
    inner.textContent = 'Loading…';
    try{
      const payload = await fetch(MEMBERS_URL, { cache: 'no-store' });
      if (!payload.ok){ inner.textContent = 'Unavailable'; return; }
      const data = await payload.json().catch(()=>null);
      inner.textContent = '';

      let membersArray = null;
      if (Array.isArray(data)) membersArray = data;
      else if (data && Array.isArray(data.members)) membersArray = data.members;
      else if (data && Array.isArray(data.data)) membersArray = data.data;

      const providedCount = (data && typeof data.memberCount === 'number') ? data.memberCount : null;

      if (Array.isArray(membersArray) && membersArray.length > 0) {
        cachedMembers = membersArray;
        const membersEl = $(ids.members);
        if (membersEl) membersEl.textContent = nf.format(membersArray.length);
        innerClearAndRender(membersArray);
        return;
      }

      if (providedCount != null) {
        const membersEl = $(ids.members);
        if (membersEl) membersEl.textContent = nf.format(providedCount);
        inner.textContent = `Members: ${providedCount}`;
        return;
      }

      inner.textContent = 'No members';
    } catch(e){
      inner.textContent = 'Failed';
      console.warn('Failed to fetch members', e);
    }
  }

  // wire up
  document.addEventListener('DOMContentLoaded', () => {
    // prefetch members to populate the member count immediately
    prefetchMembersOnLoad().catch(()=>{});

    const tm = $(ids.toggleMembers);
    if (tm) tm.addEventListener('click', toggleMembers, {passive:true});

    updateStats();
  });
})();