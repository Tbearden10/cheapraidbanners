// public/script.js
// Always show members. Fetch members on load (KV-backed /members).
// Highlight roles when available (founder/admin/officer).
// Stats polling remains (clears etc) but does NOT change member count.

(() => {
  const STATS_URL = '/stats';
  const MEMBERS_URL = '/members';
  const POLL_MS = 60_000;

  const ids = {
    membersCount: 'bungie-members',
    clears: 'bungie-clears',
    prophecy: 'prophecy-clears',
    updated: 'bungie-updated',
    memberListInner: 'member-list-inner'
  };

  const nf = new Intl.NumberFormat();

  function $(id){ return document.getElementById(id); }

  async function fetchJson(url, timeout = 7000) {
    const ctrl = new AbortController();
    const t = setTimeout(()=>ctrl.abort(), timeout);
    try {
      const res = await fetch(url, { cache: 'no-store', signal: ctrl.signal });
      clearTimeout(t);
      if (!res.ok) throw new Error('bad status');
      return await res.json();
    } finally { clearTimeout(t); }
  }

  // only update clears/prophecy/updated
  async function updateStats(){
    try {
      const data = await fetchJson(STATS_URL).catch(()=>null);
      if (!data) return;
      const clears = data.clears ?? null;
      const prophecy = data.prophecyClears ?? null;
      const updated = data.updated ?? null;

      if ($(ids.clears)) $(ids.clears).textContent = clears != null ? nf.format(clears) : '—';
      if ($(ids.prophecy)) $(ids.prophecy).textContent = prophecy != null ? nf.format(prophecy) : '—';
      if ($(ids.updated) && updated) {
        const d = new Date(updated);
        if (!Number.isNaN(d)) $(ids.updated).textContent = d.toLocaleString();
      }
    } catch (e) {
      console.warn('stats fetch failed', e);
    } finally {
      setTimeout(updateStats, POLL_MS);
    }
  }

  // normalize various payload shapes into array of member objects
  function normalizeMembersPayload(payload) {
    if (!payload) return [];
    if (Array.isArray(payload)) return payload;
    if (payload.members && Array.isArray(payload.members)) return payload.members;
    if (payload.data && Array.isArray(payload.data)) return payload.data;
    return [];
  }

  // determine role from member object. backend should ideally include member.role (founder/admin/officer).
  function memberRole(member) {
    // try common keys or heuristics
    if (!member) return null;
    const role = (member.role ?? member.rank ?? member.title ?? '').toString().toLowerCase();
    if (role.includes('founder') || role.includes('owner')) return 'founder';
    if (role.includes('admin')) return 'admin';
    if (role.includes('officer')) return 'officer';
    // sometimes API provides flags
    if (member.isFounder || member.is_owner || member.is_owner === true) return 'founder';
    if (member.isAdmin || member.is_admin === true) return 'admin';
    if (member.isOfficer || member.is_officer === true) return 'officer';
    return null;
  }

  function roleClass(role) {
    if (!role) return '';
    return role === 'founder' ? 'role-founder' : role === 'admin' ? 'role-admin' : role === 'officer' ? 'role-officer' : '';
  }

  // render members into the sidebar
  function renderMembers(members) {
    const inner = $(ids.memberListInner);
    const membersCountEl = $(ids.membersCount);
    if (!inner) return;
    inner.textContent = '';
    if (!Array.isArray(members) || members.length === 0) {
      inner.textContent = 'No members cached yet.';
      if (membersCountEl) membersCountEl.textContent = '—';
      return;
    }

    if (membersCountEl) membersCountEl.textContent = nf.format(members.length);

    for (const m of members) {
      const row = document.createElement('div');
      row.className = 'member-item';

      const name = document.createElement('div');
      name.className = 'member-name';
      // displayName is preferred; fallback to name/id
      name.textContent = m.displayName ?? m.name ?? m.display_name ?? m.username ?? String(m.membershipId ?? m.id ?? '');

      row.appendChild(name);

      // role badge
      const role = memberRole(m);
      if (role) {
        const badge = document.createElement('div');
        badge.className = 'role-badge ' + roleClass(role);
        badge.textContent = role === 'founder' ? 'Founder' : role === 'admin' ? 'Admin' : role === 'officer' ? 'Officer' : role;
        row.appendChild(badge);
      }

      inner.appendChild(row);
    }
  }

  // fetch members once on load and render them (members always shown)
  async function loadMembersOnStart() {
    try {
      const payload = await fetchJson(MEMBERS_URL).catch(()=>null);
      const members = normalizeMembersPayload(payload);
      renderMembers(members);
    } catch (e) {
      console.warn('failed fetching members', e);
    }
  }

  document.addEventListener('DOMContentLoaded', () => {
    loadMembersOnStart().catch(()=>{});
    updateStats();
  });
})();