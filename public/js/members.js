// members.js - robust members loader + default renderer
// - Loads /members from your backend (preferred).
// - Normalizes members and populates window.__membersById.
// - Provides defaultRenderMemberStats which sorts by total clears (descending).
// - Does NOT call Bungie from the browser (no CORS).

let previousMembersData = null;
window.__membersById = new Map();

/**
 * defaultRenderMemberStats(statsData, membersData)
 * Renders member cards into #member-stats-container.
 * Sorts members by clears descending using statsData.perMember if available.
 */
function defaultRenderMemberStats(statsData, membersData) {
  const container = document.getElementById('member-stats-container');
  if (!container) return;

  // Build per-member map of clears/playtime
  const perMemberMap = new Map();
  if (statsData && Array.isArray(statsData.perMember)) {
    for (const p of statsData.perMember) {
      const id = String(p.membershipId ?? p.membership_id ?? '');
      perMemberMap.set(id, {
        totalFullClears: Number(p.totalFullClears ?? p.total_full_clears ?? 0),
        totalPlaytimeSeconds: Number(p.totalPlaytimeSeconds ?? p.total_playtime_seconds ?? 0),
        // keep nested stats if present
        stats: p.stats ?? p.memberStats ?? null,
        lastProcessedDate: p.lastProcessedDate ?? p.last_processed_date ?? null
      });
    }
  }

  // Determine list of members to render: prefer provided membersData.members else window.__membersById
  let membersList = [];
  if (membersData && Array.isArray(membersData.members)) {
    membersList = membersData.members.map(m => ({
      membershipId: String(m.membershipId ?? m.membership_id ?? ''),
      membershipType: Number(m.membershipType ?? m.membership_type ?? 0),
      displayName: m.displayName ?? m.display_name ?? 'Unknown',
      isOnline: Boolean(m.isOnline ?? m.is_online ?? false),
      emblemPath: m.emblemPath ?? m.emblem_path ?? null,
      lastProcessedDate: m.lastProcessedDate ?? m.last_processed_date ?? null
    }));
  } else {
    membersList = Array.from(window.__membersById.values()).map(m => ({
      membershipId: String(m.membershipId ?? m.membership_id ?? ''),
      membershipType: Number(m.membershipType ?? m.membership_type ?? 0),
      displayName: m.displayName ?? m.display_name ?? 'Unknown',
      isOnline: Boolean(m.isOnline ?? m.is_online ?? false),
      emblemPath: m.emblemPath ?? m.emblem_path ?? null,
      lastProcessedDate: m.lastProcessedDate ?? m.last_processed_date ?? null
    }));
  }

  if (!membersList || membersList.length === 0) {
    container.innerHTML = '<div style="text-align:center;padding:40px;color:var(--chocolate);">No members to show.</div>';
    return;
  }

  // Sort by clears descending (members without stats treated as 0)
  membersList.sort((a, b) => {
    const aId = String(a.membershipId);
    const bId = String(b.membershipId);
    const aClears = perMemberMap.get(aId)?.totalFullClears ?? 0;
    const bClears = perMemberMap.get(bId)?.totalFullClears ?? 0;
    if (bClears !== aClears) return bClears - aClears;
    return String(a.displayName).localeCompare(String(b.displayName));
  });

  const escape = window.__utils?.escapeHtml || (t => String(t));
  const nf = window.__utils?.nf || new Intl.NumberFormat();

  const html = membersList.map((m, idx) => {
    const pm = perMemberMap.get(String(m.membershipId)) || { totalFullClears: 0, totalPlaytimeSeconds: 0 };
    const emblem = m.emblemPath || '';
    const clears = Number(pm.totalFullClears || 0);
    const rankClass = idx === 0 ? 'rank-1' : idx === 1 ? 'rank-2' : idx === 2 ? 'rank-3' : '';
    const rankBadge = idx < 3 ? `<div class="member-stat-rank ${rankClass}">#${idx + 1}</div>` : '';

    return `
      <div class="member-stat-card ${rankClass}">
        ${rankBadge}
        <div class="member-stat-emblem">
          ${emblem ? `<img src="${escape(emblem)}" alt="${escape(m.displayName)} emblem" />` : `<div style="width:100%;height:100%;background:linear-gradient(135deg,#eee,#ddd);"></div>`}
        </div>
        <div class="member-stat-status" style="margin-top:8px;">
          <div class="status-dot ${m.isOnline ? 'online' : 'offline'}"></div>
        </div>
        <div class="member-stat-name" title="${escape(m.displayName)}">${escape(m.displayName)}</div>
        <div class="member-stat-clears">${nf.format(clears)}</div>
        <div class="member-stat-label">Dungeon Clears</div>
      </div>
    `;
  }).join('');

  container.innerHTML = html;
}

/**
 * renderMemberStats - wrapper used by app to render current view
 * If custom renderers exist they take precedence.
 */
function renderMemberStats(statsData, membersData) {
  try {
    if (typeof window.renderMemberStatsCards === 'function') {
      return window.renderMemberStatsCards(statsData, membersData);
    }
    if (typeof window.renderMemberStatsBars === 'function') {
      return window.renderMemberStatsBars(statsData, membersData);
    }
    return defaultRenderMemberStats(statsData, membersData);
  } catch (err) {
    console.error('[Members] renderMemberStats failed', err);
  }
}

/**
 * loadMembers
 * - Loads /members from backend and normalizes into window.__membersById
 * - Does NOT call Bungie directly (no CORS)
 */
async function loadMembers(forceRender = false) {
  try {
    const fetchJson = (path, fetchOpts) => {
      const base = (window.__utils?.API_BASE || window.API_BASE || API_BASE || 'https://api.cheapraidbanners.com').replace(/\/$/, '');
      const url = new URL(path, base).toString();
      return window.__utils?.fetchJson
        ? window.__utils.fetchJson(url)
        : fetch(url, fetchOpts).then(r => r.ok ? r.json().catch(()=>null) : null).catch(()=>null);
    };
    const data = await fetchJson('/members');

    if (!data || !Array.isArray(data.members) || data.members.length === 0) {
      // Populate map empty and return null to let app handle it
      window.__membersById.clear();
      return null;
    }

    previousMembersData = JSON.parse(JSON.stringify(data));
    window.__membersById.clear();

    data.members.forEach(m => {
      const id = String(m.membershipId ?? m.membership_id ?? '');
      if (!id) return;
      const normalized = {
        membershipId: id,
    
        membershipType: Number(m.membership_type ?? m.membershipType ?? 0),
        displayName: m.displayName ?? m.display_name ?? 'Unknown',
        isOnline: Boolean(m.isOnline ?? m.is_online ?? false),
        emblemPath: m.emblemPath ?? m.emblem_path ?? null,
      };
      window.__membersById.set(id, normalized);
    });

    return data;
  } catch (err) {
    console.error('[Members] loadMembers failed', err);
    return null;
  }
}

// Expose
window.loadMembers = loadMembers;
window.renderMemberStats = renderMemberStats;