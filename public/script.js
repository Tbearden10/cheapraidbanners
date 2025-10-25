// Minimal script: poll /stats and member-list UI.
// On page load we fetch /members (KV-read) and update the Members stat immediately.
// The member-list button simply displays the cached members (no roster fetch).

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
    memberInner: 'member-list-inner'
  };

  const nf = new Intl.NumberFormat();
  let statsTimer = null;
  let cachedMembers = null; // cached array from initial KV read

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

  // updateStats only manages clears/prophecy/updated, it DOES NOT touch members count
  async function updateStats(){
    try{
      const data = await fetchJson(STATS_URL).catch(()=>null);
      const clears = data ? (data.clears ?? undefined) : undefined;
      const prophecy = data ? (data.prophecyClears ?? undefined) : undefined;
      const updated = data ? data.updated : undefined;

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

  // Prefetch members from KV on load and update the Members stat
  async function prefetchMembersOnLoad() {
    try {
      const payload = await fetchJson(MEMBERS_URL).catch(()=>null);
      if (!payload) return;
      let membersArray = null;
      if (Array.isArray(payload)) membersArray = payload;
      else if (payload && Array.isArray(payload.members)) membersArray = payload.members;

      const providedCount = (payload && typeof payload.memberCount === 'number') ? payload.memberCount : null;
      const derivedCount = Array.isArray(membersArray) ? membersArray.length : null;
      const memberCount = providedCount != null ? providedCount : derivedCount;

      if (Array.isArray(membersArray) && membersArray.length > 0) {
        cachedMembers = membersArray;
        const mEl = $(ids.members);
        if (mEl) mEl.textContent = nf.format(membersArray.length);
      } else if (typeof memberCount === 'number') {
        const mEl = $(ids.members);
        if (mEl) mEl.textContent = nf.format(memberCount);
      }
    } catch (e) {
      console.warn('prefetchMembersOnLoad failed', e);
    }
  }

  // Member button simply displays cachedMembers; no roster fetch is performed here.
  function toggleMembers(){
    const btn = $(ids.toggleMembers);
    const list = $(ids.memberList);
    const inner = $(ids.memberInner);
    const open = btn.getAttribute('aria-expanded') === 'true';
    if (open){
      btn.setAttribute('aria-expanded','false'); btn.textContent = '▶';
      list.setAttribute('aria-hidden','true'); list.style.display = 'none';
      return;
    }

    btn.setAttribute('aria-expanded','true'); btn.textContent = '◀';
    list.setAttribute('aria-hidden','false'); list.style.display = '';
    inner.textContent = '';

    if (!Array.isArray(cachedMembers) || cachedMembers.length === 0) {
      inner.textContent = 'No members cached yet. Members are updated by scheduled jobs.';
      return;
    }

    for (const m of cachedMembers) {
      const d = document.createElement('div');
      d.textContent = String(m.displayName ?? m.name ?? m.membershipId ?? m.id ?? '');
      inner.appendChild(d);
    }
  }

  document.addEventListener('DOMContentLoaded', () => {
    prefetchMembersOnLoad().catch(()=>{});
    const tm = $(ids.toggleMembers);
    if (tm) tm.addEventListener('click', toggleMembers, { passive: true });
    updateStats();
  });
})();