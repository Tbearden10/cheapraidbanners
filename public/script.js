// public/script.js
// Render full member list (always visible). Parses Bungie clan members Response.results shape
// and renders: display name, supplemental display name, online, role badge, emblem background.
//
// Behavior:
//  - Uses members_list provided by GET /members (expected to be KV-backed).
//  - Expects members in one of these shapes:
//      { Response: { results: [...] } }         <-- raw Bungie roster
//      { members: [...] }                       <-- normalized backend shape
//      [ ... ]                                  <-- array
//  - Emblem/role: prefers member.emblemUrl and member.role (set by backend).
//    If not present, shows a neutral background and no badge. Backend enrichment recommended.

(() => {
  const MEMBERS_URL = '/members';
  const ids = {
    membersCount: 'bungie-members',
    memberListInner: 'member-list-inner',
    clears: 'bungie-clears',
    prophecy: 'prophecy-clears',
    updated: 'bungie-updated'
  };

  const nf = new Intl.NumberFormat();

  function $(id){ return document.getElementById(id); }

  async function fetchJson(url, timeout = 8000){
    const ctrl = new AbortController();
    const t = setTimeout(()=>ctrl.abort(), timeout);
    try {
      const res = await fetch(url, { cache: 'no-store', signal: ctrl.signal });
      clearTimeout(t);
      if (!res.ok) throw new Error('bad status: ' + res.status);
      return await res.json();
    } finally { clearTimeout(t); }
  }

  // Normalize the possible shapes into an array of minimal member objects.
  function normalizeMembers(payload) {
    if (!payload) return [];
    // raw bungie shape: { Response: { results: [...] } }
    if (payload.Response && Array.isArray(payload.Response.results)) {
      return payload.Response.results.map(mapBungieMember);
    }
    // backend normalized: { members: [...] }
    if (Array.isArray(payload.members)) return payload.members.map(ensureMinimalMember);
    // raw array
    if (Array.isArray(payload)) return payload.map(ensureMinimalMember);
    // fallback: empty
    return [];
  }

  // Map Bungie "result" object to minimal member object we expect in UI.
  function mapBungieMember(r) {
    // r contains destinyUserInfo and bungieNetUserInfo per sample
    const destiny = r.destinyUserInfo ?? {};
    const bungie = r.bungieNetUserInfo ?? {};
    const membershipId = String(destiny.membershipId ?? bungie.membershipId ?? r.membershipId ?? '');
    return {
      membershipId,
      displayName: destiny.displayName ?? bungie.displayName ?? '',
      supplementalDisplayName: bungie.supplementalDisplayName ?? '',
      isOnline: !!r.isOnline,
      joinDate: r.joinDate ?? null,
      // role and emblemUrl are not provided by this endpoint — backend should enrich:
      role: r.role ?? null,
      emblemUrl: r.emblemUrl ?? null,
      // keep raw for possible debugging
      _raw: r
    };
  }

  // Ensure already-normalized members include the keys we expect
  function ensureMinimalMember(m) {
    return {
      membershipId: String(m.membershipId ?? m.destinyUserInfo?.membershipId ?? ''),
      displayName: m.displayName ?? m.destinyUserInfo?.displayName ?? m.bungieNetUserInfo?.displayName ?? '',
      supplementalDisplayName: m.supplementalDisplayName ?? m.bungieNetUserInfo?.supplementalDisplayName ?? '',
      isOnline: !!(m.isOnline ?? false),
      joinDate: m.joinDate ?? null,
      role: m.role ?? null,           // expected: 'founder'|'admin'|'officer'|'member' or null
      emblemUrl: m.emblemUrl ?? null,
      _raw: m
    };
  }

  function roleClass(role) {
    if (!role) return '';
    return role === 'founder' ? 'role-founder' : role === 'admin' ? 'role-admin' : role === 'officer' ? 'role-officer' : 'role-member';
  }

  function formatOnlineDot(isOnline) {
    const el = document.createElement('span');
    el.className = 'online-dot' + (isOnline ? ' online' : ' offline');
    el.setAttribute('aria-hidden','true');
    return el;
  }

  function createMemberRow(m) {
    const row = document.createElement('div');
    row.className = 'member-item';
    // background emblem
    if (m.emblemUrl) {
      row.style.backgroundImage = `url("${m.emblemUrl}")`;
      row.style.backgroundSize = 'cover';
      row.style.backgroundPosition = 'center';
    } else {
      row.style.background = '#fff';
    }

    // content wrapper to ensure readability over emblem
    const content = document.createElement('div');
    content.className = 'member-item-content';

    // left: name + supplemental
    const nameWrap = document.createElement('div');
    nameWrap.className = 'member-name-wrap';

    const name = document.createElement('div');
    name.className = 'member-name';
    name.textContent = m.displayName || m.supplementalDisplayName || m.membershipId || 'Unknown';
    nameWrap.appendChild(name);

    if (m.supplementalDisplayName) {
      const supp = document.createElement('div');
      supp.className = 'member-supp';
      supp.textContent = m.supplementalDisplayName;
      nameWrap.appendChild(supp);
    }

    content.appendChild(nameWrap);

    // right: badges (role + online)
    const badges = document.createElement('div');
    badges.className = 'member-badges';

    // online dot
    badges.appendChild(formatOnlineDot(m.isOnline));

    // role badge
    if (m.role) {
      const rb = document.createElement('div');
      rb.className = 'role-badge ' + roleClass(m.role);
      rb.textContent = (m.role === 'founder' ? 'Founder' : m.role === 'admin' ? 'Admin' : m.role === 'officer' ? 'Officer' : 'Member');
      badges.appendChild(rb);
    }

    content.appendChild(badges);

    row.appendChild(content);

    return row;
  }

  function renderMembers(members) {
    const inner = $(ids.memberListInner);
    const membersCountEl = $(ids.membersCount);
    if (!inner) return;
    inner.textContent = '';

    if (!Array.isArray(members) || members.length === 0) {
      inner.textContent = 'No members.';
      if (membersCountEl) membersCountEl.textContent = '—';
      return;
    }

    if (membersCountEl) membersCountEl.textContent = nf.format(members.length);

    // optional: sort founders/admins first if role present
    members.sort((a,b) => {
      const order = { founder: 0, admin: 1, officer: 2, member: 3, null: 4 };
      const ra = a.role ?? null;
      const rb = b.role ?? null;
      return (order[ra] ?? 4) - (order[rb] ?? 4);
    });

    for (const m of members) {
      inner.appendChild(createMemberRow(m));
    }
  }

  async function loadMembers() {
    try {
      const payload = await fetchJson(MEMBERS_URL).catch(()=>null);
      const members = normalizeMembers(payload);
      renderMembers(members);
    } catch (e) {
      console.warn('loadMembers failed', e);
    }
  }

  // init
  document.addEventListener('DOMContentLoaded', () => {
    loadMembers().catch(()=>{});
    // stats polling still separate (keeps existing behavior)
    // if you want, call updateStats here (not included in this file)
  });
})();