// Minimal script: poll /stats and member-list toggle with lazy-load.
// Stats are always visible (no toggle). Member list lazy-loads on first open.
// Frontend now updates member count immediately when the member list is loaded.

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

    if (membersLoaded) return;
    membersLoaded = true;
    inner.textContent = 'Loading…';
    try{
      const res = await fetch(MEMBERS_URL, { cache: 'no-store' });
      if (!res.ok){ inner.textContent = 'Unavailable'; return; }
      const payload = await res.json().catch(()=>null);
      inner.textContent = '';

      // Normalize payload shape:
      // - accept an array (dev/dummy) or an object { fetchedAt, members: [...] } or { members: [...], memberCount: n }
      let membersArray = null;
      if (Array.isArray(payload)) {
        membersArray = payload;
      } else if (payload && Array.isArray(payload.members)) {
        membersArray = payload.members;
      } else if (payload && Array.isArray(payload.data)) {
        membersArray = payload.data;
      }

      // If the response included a memberCount top-level, we can use that too
      const fetchedCount = (payload && typeof payload.memberCount === 'number') ? payload.memberCount : null;

      // If we have membersArray, render it and update the member-count UI immediately
      if (Array.isArray(membersArray) && membersArray.length > 0) {
        // Update members stat immediately from loaded list
        const membersEl = $(ids.members);
        if (membersEl) membersEl.textContent = nf.format(membersArray.length);

        inner.textContent = '';
        for (const m of membersArray){
          const d = document.createElement('div');
          d.textContent = String(m.displayName ?? m.name ?? m.membershipId ?? m.id ?? '');
          inner.appendChild(d);
        }
        return;
      }

      // If only memberCount was provided, update the UI and show a simple note
      if (fetchedCount != null) {
        const membersEl = $(ids.members);
        if (membersEl) membersEl.textContent = nf.format(fetchedCount);
        inner.textContent = `Members: ${fetchedCount}`;
        return;
      }

      // Otherwise no usable members
      inner.textContent = 'No members';
    } catch(e){
      inner.textContent = 'Failed';
      console.warn('Failed to fetch members', e);
    }
  }

  // wire up
  document.addEventListener('DOMContentLoaded', () => {
    const tm = $(ids.toggleMembers);
    if (tm) tm.addEventListener('click', toggleMembers, {passive:true});
    updateStats();
  });
})();