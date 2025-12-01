// ============================================================================
// FILE: src/index.ts
// Main worker entry point - handles HTTP routes and cron triggers
// Updated with extensive logging
// ============================================================================

import type { Env, MemberJob, StatsQueueJob } from './types';
import {
  fetchClanRoster,
  enrichMemberWithEmblem,
  fetchCharactersForMember,
  fetchActivitiesForCharacter,
  fetchPGCR,
  fetchActivityDefinition,
} from './api/bungieApi';
import { getMembersList, upsertClanMember } from './db/queries';
import { processMemberJob } from './processors/memberJobProcessor';
import { processStatsQueueJob } from './processors/statsQueueProcessor';
export { BatchCoordinator } from './durable-objects/BatchCoordinator';
export { RunTracker } from './durable-objects/RunTracker';

// CORS Configuration
function getCorsHeaders(request: Request, env: Env) {
  const origin = request.headers.get('Origin') || '';
  
  const allowedOrigins = [
    'https://cheapraidbanners.com',
    'https://www.cheapraidbanners.com',
  ];

  if (env.ENVIRONMENT === 'dev' || env.ENVIRONMENT === 'development') {
    if (origin.includes('localhost') || origin.includes('127.0.0.1')) {
      return {
        'Access-Control-Allow-Origin': origin,
        'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type, Authorization',
        'Access-Control-Max-Age': '86400',
        'Vary': 'Origin',
      };
    }
  }

  const allowedOrigin = allowedOrigins.find(allowed => origin === allowed);
  
  return {
    'Access-Control-Allow-Origin': allowedOrigin || allowedOrigins[0],
    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization',
    'Access-Control-Max-Age': '86400',
    'Vary': 'Origin',
  };
}

function getSecurityHeaders() {
  return {
    'X-Content-Type-Options': 'nosniff',
    'X-Frame-Options': 'DENY',
    'X-XSS-Protection': '1; mode=block',
    'Referrer-Policy': 'strict-origin-when-cross-origin',
  };
}

function jsonResponse(data: unknown, status = 200, request?: Request, env?: Env) {
  const headers: Record<string, string> = {
    'Content-Type': 'application/json',
    ...getSecurityHeaders(),
  };

  if (request && env) {
    Object.assign(headers, getCorsHeaders(request, env));
  }

  return new Response(JSON.stringify(data), { status, headers });
}

function isAuthenticated(request: Request, env: Env): boolean {
  if (env.ENVIRONMENT === 'dev' || env.ENVIRONMENT === 'development') {
    return true;
  }

  const authHeader = request.headers.get('Authorization');
  if (!authHeader) return false;

  const token = authHeader.replace(/^Bearer\s+/i, '');
  return token === env.API_TOKEN;
}

const rateLimitMap = new Map<string, { count: number; resetAt: number }>();

function checkRateLimit(identifier: string, maxRequests = 100, windowMs = 60000): boolean {
  const now = Date.now();
  const record = rateLimitMap.get(identifier);

  if (!record || now > record.resetAt) {
    rateLimitMap.set(identifier, { count: 1, resetAt: now + windowMs });
    return true;
  }

  if (record.count >= maxRequests) {
    return false;
  }

  record.count++;
  return true;
}

// ============================================================================
// MEMBER SYNC CRON - Every 30 minutes
// ============================================================================
export async function memberSyncCron(env: Env): Promise<void> {
  const startTime = Date.now();
  const clanId = env.BUNGIE_CLAN_ID;
  
  console.log(`\n${'═'.repeat(80)}`);
  console.log(`[MemberSync] STARTING Member Roster Sync`);
  console.log(`[MemberSync] Clan ID: ${clanId}`);
  console.log(`[MemberSync] Timestamp: ${new Date().toISOString()}`);
  console.log(`${'═'.repeat(80)}`);

  // Fetch current roster from Bungie
  console.log(`[MemberSync] Step 1: Fetching current clan roster from Bungie API...`);
  const roster = (await fetchClanRoster(clanId, env.BUNGIE_API_KEY)) || [];
  console.log(`[MemberSync] ✓ Fetched ${roster.length} members from Bungie`);

  console.log(`[MemberSync] Step 2: Fetching existing members from database...`);
  const dbMembers = await getMembersList(env.DB, clanId, false);
  console.log(`[MemberSync] ✓ Found ${dbMembers.length} members in database (including inactive)`);

  const dbMemberIds = new Set(dbMembers.map(m => m.membership_id));
  const rosterMemberIds = new Set((roster || []).map((m: any) => m.membershipId));

  // Detect changes
  const newMembers = roster.filter((m: any) => !dbMemberIds.has(m.membershipId));
  const leftMembers = dbMembers.filter(m => !rosterMemberIds.has(m.membership_id));
  const existingMembers = roster.filter((m: any) => dbMemberIds.has(m.membershipId));

  console.log(`\n[MemberSync] Step 3: Change Detection Summary:`);
  console.log(`[MemberSync] - New members joined: ${newMembers.length}`);
  console.log(`[MemberSync] - Members left: ${leftMembers.length}`);
  console.log(`[MemberSync] - Existing members to update: ${existingMembers.length}`);

  if (newMembers.length > 0) {
    console.log(`[MemberSync] New members:`);
    newMembers.forEach((m: any) => {
      const displayName = m.bungieGlobalDisplayNameCode
        ? `${m.bungieGlobalDisplayName}#${m.bungieGlobalDisplayNameCode}`
        : m.bungieGlobalDisplayName || m.displayName;
      console.log(`[MemberSync]   + ${displayName} (${m.membershipId})`);
    });
  }

  if (leftMembers.length > 0) {
    console.log(`[MemberSync] Members who left:`);
    leftMembers.forEach(m => {
      console.log(`[MemberSync]   - ${m.display_name} (${m.membership_id})`);
    });
  }

  // Process all roster members (new + existing)
  console.log(`\n[MemberSync] Step 4: Processing ${roster.length} roster members...`);
  let successCount = 0;
  let errorCount = 0;

  for (let i = 0; i < roster.length; i++) {
    const rawMember = roster[i];
    const member = { ...rawMember };
    const memberNum = i + 1;

    const displayName = member.bungieGlobalDisplayNameCode
      ? `${member.bungieGlobalDisplayName}#${member.bungieGlobalDisplayNameCode}`
      : member.bungieGlobalDisplayName || member.displayName;

    console.log(`[MemberSync]   Processing ${memberNum}/${roster.length}: ${displayName}...`);

    try {
      console.log(`[MemberSync]     - Enriching with emblem data...`);
      const enriched = await enrichMemberWithEmblem(member, env.BUNGIE_API_KEY);
      member.emblemPath = enriched.emblemPath ?? null;
      member.emblemBackgroundPath = enriched.emblemBackgroundPath ?? null;
      Object.assign(member, enriched);
      console.log(`[MemberSync]     - ✓ Emblem enriched`);
    } catch (err) {
      console.warn(`[MemberSync]     - ⚠️  Emblem enrich failed:`, err);
      member.emblemPath = null;
      member.emblemBackgroundPath = null;
    }

    try {
      console.log(`[MemberSync]     - Upserting to database...`);
      await upsertClanMember(env.DB, {
        clanId,
        membershipId: member.membershipId,
        membershipType: member.membershipType,
        displayName,
        isOnline: member.isOnline || false,
        lastOnlineStatusChange: member.lastOnlineStatusChange,
        joinDate: member.joinDate,
        isActive: true,
        emblemPath: member.emblemPath ?? null,
        emblemBackgroundPath: member.emblemBackgroundPath ?? null,
      });
      console.log(`[MemberSync]     - ✓ Database updated`);
      successCount++;
    } catch (err) {
      console.error(`[MemberSync]     - ❌ Database upsert failed:`, err);
      errorCount++;
    }
  }

  console.log(`[MemberSync] ✓ Member processing complete: ${successCount} success, ${errorCount} errors`);

  // Mark left members as inactive
  if (leftMembers.length > 0) {
    console.log(`\n[MemberSync] Step 5: Marking ${leftMembers.length} departed members as inactive...`);
    let inactiveSuccessCount = 0;
    let inactiveErrorCount = 0;

    for (const left of leftMembers) {
      try {
        await env.DB.prepare(
          `UPDATE clan_members SET is_active = 0, updated_at = ? WHERE clan_id = ? AND membership_id = ?`
        ).bind(Date.now(), clanId, left.membership_id).run();
        console.log(`[MemberSync]   - ✓ Marked ${left.display_name} as inactive`);
        inactiveSuccessCount++;
      } catch (err) {
        console.error(`[MemberSync]   - ❌ Failed to mark ${left.display_name} inactive:`, err);
        inactiveErrorCount++;
      }
    }
    console.log(`[MemberSync] ✓ Inactive marking complete: ${inactiveSuccessCount} success, ${inactiveErrorCount} errors`);
  }

  const duration = Date.now() - startTime;
  console.log(`\n${'═'.repeat(80)}`);
  console.log(`[MemberSync] COMPLETE`);
  console.log(`[MemberSync] Summary:`);
  console.log(`[MemberSync] - Total roster members: ${roster.length}`);
  console.log(`[MemberSync] - New members: ${newMembers.length}`);
  console.log(`[MemberSync] - Members left: ${leftMembers.length}`);
  console.log(`[MemberSync] - Successfully processed: ${successCount}`);
  console.log(`[MemberSync] - Errors: ${errorCount}`);
  console.log(`[MemberSync] - Duration: ${duration}ms (${(duration / 1000).toFixed(2)}s)`);
  console.log(`${'═'.repeat(80)}\n`);
}

// ============================================================================
// STATS SYNC CRON - Every 6 hours
// ============================================================================
async function statsSyncCron(env: Env): Promise<void> {
  const startTime = Date.now();
  const clanId = env.BUNGIE_CLAN_ID;
  
  console.log(`\n${'═'.repeat(80)}`);
  console.log(`[StatsSync] STARTING Stats Sync`);
  console.log(`[StatsSync] Clan ID: ${clanId}`);
  console.log(`[StatsSync] Timestamp: ${new Date().toISOString()}`);
  console.log(`${'═'.repeat(80)}`);

  console.log(`[StatsSync] Step 1: Fetching active members from database...`);
  const members = await getMembersList(env.DB, clanId, true);

  if (!members || members.length === 0) {
    console.log(`[StatsSync] ⚠️  No active members found - exiting`);
    console.log(`${'═'.repeat(80)}\n`);
    return;
  }

  console.log(`[StatsSync] ✓ Found ${members.length} active members`);

  // Step 2: Use last-online status change as a fast pre-check to avoid fetching activity history.
  // - If member is currently online -> always process (their last-online won't update until they go offline).
  // - If last-online is null/empty -> exclude (skip) as requested.
  // - Otherwise compare resolved or raw last-online fields vs their previous values to decide skip/process.
  console.log(`[StatsSync] Step 2: Filtering members by last-online/resume checkpoints (including online members)...`);

  const membersToProcess: typeof members = [];

  const isEmpty = (v: unknown) => v === null || v === undefined || (typeof v === 'string' && v.trim() === '');

  for (const member of members) {
    const displayName = member.display_name || member.display_name || String(member.membership_id);

    // 0) If member is currently online, we want to process them (their last-online won't move until they go offline).
    if (member.is_online) {
      console.log(`[StatsSync]   - ${displayName}: Currently online -> will process`);
      membersToProcess.push(member);
      continue;
    }

    // 1) If the current last-online is empty/null, exclude them per your request
    const currResolved = (member as any).last_online_status_change_resolved ?? null;
    const currRaw = (member as any).last_online_status_change ?? null;
    if (isEmpty(currResolved) && isEmpty(currRaw)) {
      console.log(`[StatsSync]   - ${displayName}: Current last-online is empty/null -> skipping`);
      continue;
    }

    // 2) If both resolved values (current + prev) are present, prefer them
    const prevResolved = (member as any).last_online_status_change_resolved_prev ?? null;
    const hasPrevResolved = !isEmpty(prevResolved) && !isEmpty(currResolved);
    if (hasPrevResolved) {
      if (String(currResolved) === String(prevResolved)) {
        console.log(`[StatsSync]   - ${displayName}: Skipping (resolved last-online unchanged)`);
        continue;
      } else {
        console.log(`[StatsSync]   - ${displayName}: Resolved last-online changed -> will process`);
        membersToProcess.push(member);
        continue;
      }
    }

    // 3) Fallback to raw fields (current + prev)
    const prevRaw = (member as any).last_online_status_change_prev ?? null;
    const hasPrevRaw = !isEmpty(prevRaw) && !isEmpty(currRaw);
    if (hasPrevRaw) {
      if (String(currRaw) === String(prevRaw)) {
        console.log(`[StatsSync]   - ${displayName}: Skipping (raw last-online unchanged)`);
        continue;
      } else {
        console.log(`[StatsSync]   - ${displayName}: Raw last-online changed -> will process`);
        membersToProcess.push(member);
        continue;
      }
    }

    // 4) If we don't have prev last-online values, fall back to last_processed_date vs latest known last-online:
    const lastProcessedStr = member.last_processed_date ?? null;
    let lastProcessedMs: number | null = null;
    if (lastProcessedStr) {
      const parsed = new Date(lastProcessedStr).getTime();
      if (!Number.isNaN(parsed)) lastProcessedMs = parsed;
    }

    // derive latestOnlineMs from current resolved/raw if possible
    let latestOnlineMs: number | null = null;
    const candidate = currResolved ?? currRaw ?? null;
    if (!isEmpty(candidate)) {
      const asNum = Number(candidate);
      if (Number.isFinite(asNum) && asNum > 0) {
        latestOnlineMs = asNum;
      } else {
        const parsed = new Date(String(candidate)).getTime();
        if (!Number.isNaN(parsed)) latestOnlineMs = parsed;
      }
    }

    if (lastProcessedMs && latestOnlineMs && lastProcessedMs >= latestOnlineMs) {
      console.log(`[StatsSync]   - ${displayName}: Skipping (last_processed_date ${new Date(lastProcessedMs).toISOString()} >= latest online ${new Date(latestOnlineMs).toISOString()})`);
      continue;
    }

    // Default: process (no prev snapshot available or detected change)
    console.log(`[StatsSync]   - ${displayName}: No prev-last-online snapshot or detected change -> will process`);
    membersToProcess.push(member);
  }

  console.log(`\n[StatsSync] Activity Filter Results:`);
  console.log(`[StatsSync] - Total active members: ${members.length}`);
  console.log(`[StatsSync] - Members with detected new activity: ${membersToProcess.length}`);
  console.log(`[StatsSync] - Members skipped: ${members.length - membersToProcess.length}`);

  if (membersToProcess.length === 0) {
    console.log(`[StatsSync] ✓ Nothing to process - exiting`);
    console.log(`${'═'.repeat(80)}\n`);
    return;
  }

  const runId = `run-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
  console.log(`\n[StatsSync] Step 3: Initializing run tracker`);
  console.log(`[StatsSync] Run ID: ${runId}`);
  console.log(`[StatsSync] Expected member count: ${membersToProcess.length}`);

  try {
    const trackerId = env.RUN_TRACKER.idFromName(`run-tracker-${clanId}`);
    const tracker = env.RUN_TRACKER.get(trackerId);
    await tracker.fetch('https://internal/init', {
      method: 'POST',
      body: JSON.stringify({
        runId,
        clanId,
        expectedCount: membersToProcess.length,
      }),
      headers: { 'Content-Type': 'application/json' },
    });
    console.log(`[StatsSync] ✓ RunTracker initialized`);
  } catch (err) {
    console.error(`[StatsSync] ⚠️  RunTracker init failed (continuing anyway):`, err);
  }

  // Queue members SEQUENTIALLY — pass last_processed_date from DB so MemberJob will only process newer instances.
  console.log(`\n[StatsSync] Step 4: Queueing ${membersToProcess.length} members...`);
  let queuedCount = 0;
  let queueErrorCount = 0;

  for (let i = 0; i < membersToProcess.length; i++) {
    const member = membersToProcess[i];
    const memberNum = i + 1;
    
    console.log(`[StatsSync]   Queueing ${memberNum}/${membersToProcess.length}: ${member.display_name}...`);
    
    try {
      await env.MEMBER_STATS_QUEUE.send({
        clanId,
        membershipId: member.membership_id,
        membershipType: member.membership_type,
        displayName: member.display_name,
        lastProcessedDate: member.last_processed_date ?? null,
        runId,
      });
      console.log(`[StatsSync]   ✓ Queued ${member.display_name}`);
      queuedCount++;
    } catch (err) {
      console.error(`[StatsSync]   ❌ Failed to queue ${member.display_name}:`, err);
      queueErrorCount++;
    }
  }

  const duration = Date.now() - startTime;
  console.log(`\n${'═'.repeat(80)}`);
  console.log(`[StatsSync] COMPLETE`);
  console.log(`[StatsSync] Summary:`);
  console.log(`[StatsSync] - Run ID: ${runId}`);
  console.log(`[StatsSync] - Members queued: ${queuedCount}`);
  console.log(`[StatsSync] - Queue errors: ${queueErrorCount}`);
  console.log(`[StatsSync] - Duration: ${duration}ms (${(duration / 1000).toFixed(2)}s)`);
  console.log(`[StatsSync] Note: Member processing will happen asynchronously via queue`);
  console.log(`${'═'.repeat(80)}\n`);
}

/** Helper: fetch all member stats grouped (used by /stats) */
async function fetchAllMemberStatsGrouped(db: any, clanId: string) {
  const res = await db.prepare(
    `SELECT membership_id, dungeon_hash, total_full_clears, total_playtime_seconds, last_processed_date
     FROM member_dungeon_stats
     WHERE clan_id = ?`
  ).bind(clanId).all();

  const rows = res.results || [];
  const map = new Map<string, any[]>();
  
  for (const r of rows) {
    const mid = String((r as any).membership_id);
    if (!map.has(mid)) map.set(mid, []);
    map.get(mid)!.push({
      dungeonHash: String((r as any).dungeon_hash),
      totalFullClears: Number((r as any).total_full_clears ?? 0),
      totalPlaytimeSeconds: Number((r as any).total_playtime_seconds ?? 0),
      lastProcessedDate: (r as any).last_processed_date ?? null,
    });
  }
  
  return map;
}

// ============================================================================
// HTTP HANDLER
// ============================================================================
export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    const requestId = Math.random().toString(36).slice(2, 8);

    console.log(`\n[HTTP:${requestId}] ${request.method} ${url.pathname}${url.search}`);

    // CORS preflight
    if (request.method === 'OPTIONS') {
      console.log(`[HTTP:${requestId}] CORS preflight - responding with headers`);
      return new Response(null, { 
        status: 204, 
        headers: { ...getCorsHeaders(request, env), ...getSecurityHeaders() }
      });
    }

    try {
      // Rate limiting (only in production)
      if (env.ENVIRONMENT !== 'dev' && env.ENVIRONMENT !== 'development') {
        const clientIP = request.headers.get('CF-Connecting-IP') || 'unknown';
        if (!checkRateLimit(clientIP, 100, 60000)) {
          console.log(`[HTTP:${requestId}] ❌ Rate limit exceeded for IP: ${clientIP}`);
          return jsonResponse({ error: 'Rate limit exceeded' }, 429, request, env);
        }
      }

      // Public GET endpoints
      const publicGetPaths = new Set(['/members', '/stats', '/activity-history', '/recent-activities', '/pgcr']);

      // Auth check for protected endpoints
      if (!(request.method === 'GET' && publicGetPaths.has(url.pathname))) {
        if (!isAuthenticated(request, env)) {
          console.log(`[HTTP:${requestId}] ❌ Unauthorized request`);
          return jsonResponse({ error: 'Unauthorized' }, 401, request, env);
        }
        console.log(`[HTTP:${requestId}] ✓ Authenticated`);
      } else {
        console.log(`[HTTP:${requestId}] Public endpoint - no auth required`);
      }

      // GET /members
      if (url.pathname === '/members' && request.method === 'GET') {
        const clanId = url.searchParams.get('clanId') || env.BUNGIE_CLAN_ID;
        console.log(`[HTTP:${requestId}] Fetching members for clan: ${clanId}`);
        
        const members = await getMembersList(env.DB, clanId, true);
        console.log(`[HTTP:${requestId}] ✓ Found ${members.length} active members`);
        
        return jsonResponse({ members }, 200, request, env);
      }

      // GET /stats
      if (url.pathname === '/stats' && request.method === 'GET') {
        const clanId = url.searchParams.get('clanId') || env.BUNGIE_CLAN_ID;
        console.log(`[HTTP:${requestId}] Fetching stats for clan: ${clanId}`);
        
        const members = await getMembersList(env.DB, clanId, true);
        console.log(`[HTTP:${requestId}] - Fetched ${members.length} members`);
        
        const statsByMember = await fetchAllMemberStatsGrouped(env.DB, clanId);
        console.log(`[HTTP:${requestId}] - Fetched stats for ${statsByMember.size} members`);

        const membersWithStats = members.map((member) => ({
          destinyUserInfo: {
            membershipId: member.membership_id,
            membershipType: Number(member.membership_type),
            displayName: member.display_name,
          },
          isOnline: Boolean(member.is_online),
          lastOnlineStatusChange: member.last_online_status_change ?? null,
          lastOnlineStatusChangeResolved: (member as any).last_online_status_change_resolved ?? null,
          emblemPath: member.emblem_path ?? null,
          emblemBackgroundPath: member.emblem_background_path ?? null,
          stats: statsByMember.get(member.membership_id) || [],
          lastProcessedDate: member.last_processed_date ?? null,
        }));

        const aggregateStats = await env.DB.prepare(`SELECT * FROM clan_aggregate_stats WHERE clan_id = ? ORDER BY dungeon_hash`).bind(clanId).all();
        console.log(`[HTTP:${requestId}] - Fetched ${(aggregateStats.results || []).length} aggregate stats`);
        console.log(`[HTTP:${requestId}] ✓ Returning complete stats payload`);

        return jsonResponse({
          members: membersWithStats,
          aggregateStats: (aggregateStats.results || []),
          memberCount: members.length,
          fetchedAt: new Date().toISOString(),
        }, 200, request, env);
      }

      // GET /activity-history
      if (url.pathname === '/activity-history' && request.method === 'GET') {
        const membershipType = url.searchParams.get('membershipType');
        const membershipId = url.searchParams.get('membershipId');
        const characterId = url.searchParams.get('characterId');
        const mode = url.searchParams.get('mode') || '0';
        const count = url.searchParams.get('count') || '10';
        const page = url.searchParams.get('page') || '0';

        console.log(`[HTTP:${requestId}] Params: type=${membershipType}, id=${membershipId}, char=${characterId}, mode=${mode}, count=${count}, page=${page}`);

        if (!membershipType || !membershipId || !characterId) {
          console.log(`[HTTP:${requestId}] ❌ Missing required parameters`);
          return jsonResponse({ error: 'Missing required params: membershipType, membershipId, characterId' }, 400, request, env);
        }

        try {
          console.log(`[HTTP:${requestId}] Fetching activities from Bungie API...`);
          const activities = await fetchActivitiesForCharacter(
            Number(membershipType),
            membershipId,
            characterId,
            Number(page),
            Number(mode),
            Number(count),
            env.BUNGIE_API_KEY
          );
          console.log(`[HTTP:${requestId}] ✓ Fetched ${activities?.length || 0} activities`);
          return jsonResponse(activities, 200, request, env);
        } catch (err) {
          console.error(`[HTTP:${requestId}] ❌ Error:`, err);
          return jsonResponse({ error: 'Failed to fetch activity history' }, 500, request, env);
        }
      }

      // GET /recent-activities
      if (url.pathname === '/recent-activities' && request.method === 'GET') {
        const clanId = url.searchParams.get('clanId') || env.BUNGIE_CLAN_ID;
        const MEMBERS_TO_CHECK = 10;
        const ACTIVITIES_PER_MEMBER = 1;
        const TOTAL_ACTIVITIES = 3;

        console.log(`[HTTP:${requestId}] Fetching recent activities for clan: ${clanId}`);

        try {
          const roster = await fetchClanRoster(clanId, env.BUNGIE_API_KEY);
          if (!roster || roster.length === 0) {
            console.log(`[HTTP:${requestId}] ⚠️  No roster members found`);
            return jsonResponse([], 200, request, env);
          }

          console.log(`[HTTP:${requestId}] - Fetched ${roster.length} roster members`);

          const sorted = roster
            .filter((m: any) => m.membershipId && m.membershipType)
            .sort((a: any, b: any) => {
              const aMs = (a && a.lastOnlineStatusChange) ? Number(a.lastOnlineStatusChange) : 0;
              const bMs = (b && b.lastOnlineStatusChange) ? Number(b.lastOnlineStatusChange) : 0;
              return bMs - aMs;
            })
            .slice(0, MEMBERS_TO_CHECK);

          console.log(`[HTTP:${requestId}] - Checking ${sorted.length} most recently active members`);

          const allActivities: any[] = [];
          
          for (let i = 0; i < sorted.length; i++) {
            const member = sorted[i];
            console.log(`[HTTP:${requestId}]   Member ${i + 1}/${sorted.length}: ${member.displayName}`);
            
            try {
              const characters = await fetchCharactersForMember(
                member.membershipId,
                member.membershipType,
                env.BUNGIE_API_KEY
              );
              
              if (!characters || characters.length === 0) {
                console.log(`[HTTP:${requestId}]     - No characters found`);
                continue;
              }
              
              console.log(`[HTTP:${requestId}]     - Found ${characters.length} character(s)`);
              
              const activities = await fetchActivitiesForCharacter(
                member.membershipType,
                member.membershipId,
                characters[0].characterId,
                0,
                0,
                ACTIVITIES_PER_MEMBER,
                env.BUNGIE_API_KEY
              );
              
              if (!activities || activities.length === 0) {
                console.log(`[HTTP:${requestId}]     - No activities found`);
                continue;
              }
              
              console.log(`[HTTP:${requestId}]     - ✓ Found ${activities.length} activity/activities`);
              
              for (const act of activities) {
                allActivities.push({
                  ...act,
                  memberDisplayName: member.displayName,
                  membershipId: member.membershipId,
                  membershipType: member.membershipType,
                });
              }
            } catch (err) {
              console.warn(`[HTTP:${requestId}]     - ❌ Failed:`, err);
              continue;
            }
          }

          console.log(`[HTTP:${requestId}] - Collected ${allActivities.length} total activities`);

          const sorted_activities = allActivities
            .filter(a => a.period)
            .sort((a, b) => new Date(b.period).getTime() - new Date(a.period).getTime())
            .slice(0, TOTAL_ACTIVITIES);

          console.log(`[HTTP:${requestId}] - Selected ${sorted_activities.length} most recent activities`);

          const activityDefCache = new Map<string, any>();
          
          console.log(`[HTTP:${requestId}] - Enriching with activity definitions...`);
          const enrichedActivities = await Promise.all(
            sorted_activities.map(async (act, idx) => {
              try {
                const activityHash = act.activityDetails?.directorActivityHash || act.activityDetails?.referenceId;
                
                if (!activityHash) return act;

                let activityDef = activityDefCache.get(String(activityHash));
                
                if (!activityDef) {
                  activityDef = await fetchActivityDefinition(String(activityHash), env.BUNGIE_API_KEY);
                  if (activityDef) {
                    activityDefCache.set(String(activityHash), activityDef);
                  }
                }

                if (activityDef?.pgcrImage) {
                  console.log(`[HTTP:${requestId}]     Activity ${idx + 1}: ✓ Enriched with ${activityDef.displayProperties?.name}`);
                  return {
                    ...act,
                    activityDetails: {
                      ...act.activityDetails,
                      pgcrImage: `https://www.bungie.net${activityDef.pgcrImage}`,
                      activityName: activityDef.displayProperties?.name || 'Unknown Activity',
                    }
                  };
                }

                return act;
              } catch (err) {
                console.warn(`[HTTP:${requestId}]     Activity ${idx + 1}: Failed to enrich:`, err);
                return act;
              }
            })
          );

          console.log(`[HTTP:${requestId}] ✓ Returning ${enrichedActivities.length} enriched activities`);
          return jsonResponse(enrichedActivities, 200, request, env);
        } catch (err) {
          console.error(`[HTTP:${requestId}] ❌ Error:`, err);
          return jsonResponse({ error: 'Failed to fetch recent activities' }, 500, request, env);
        }
      }

      // GET /pgcr
      if (url.pathname === '/pgcr' && request.method === 'GET') {
        const instanceId = url.searchParams.get('instanceId');
        
        if (!instanceId) {
          console.log(`[HTTP:${requestId}] ❌ Missing instanceId`);
          return jsonResponse({ error: 'Missing instanceId parameter' }, 400, request, env);
        }

        console.log(`[HTTP:${requestId}] Fetching PGCR for instance: ${instanceId}`);

        try {
          const pgcrData = await fetchPGCR(instanceId, env.BUNGIE_API_KEY);
          
          if (!pgcrData) {
            console.log(`[HTTP:${requestId}] ❌ PGCR not found`);
            return jsonResponse({ error: 'PGCR not found' }, 404, request, env);
          }

          console.log(`[HTTP:${requestId}] ✓ PGCR fetched, processing ${pgcrData.entries?.length || 0} players`);

          const activity = pgcrData.activityDetails || {};
          const activityHash = activity.directorActivityHash || activity.referenceId;

          let activityDef = null;
          if (activityHash) {
            activityDef = await fetchActivityDefinition(String(activityHash), env.BUNGIE_API_KEY);
            console.log(`[HTTP:${requestId}] ✓ Activity definition: ${activityDef?.displayProperties?.name || 'Unknown'}`);
          }

          const players = (pgcrData.entries || []).map((entry: any) => {
            const player = entry.player || {};
            const values = entry.values || {};
            const extended = entry.extended?.values || {};

            return {
              membershipId: player.destinyUserInfo?.membershipId,
              displayName: player.destinyUserInfo?.displayName,
              bungieGlobalDisplayName: player.destinyUserInfo?.bungieGlobalDisplayName,
              bungieGlobalDisplayNameCode: player.destinyUserInfo?.bungieGlobalDisplayNameCode,
              iconPath: player.destinyUserInfo?.iconPath ? `https://www.bungie.net${player.destinyUserInfo.iconPath}` : null,
              lightLevel: player.lightLevel,
              class: {
                name: player.classType === 0 ? 'Titan' : player.classType === 1 ? 'Hunter' : player.classType === 2 ? 'Warlock' : 'Unknown',
                type: player.classType
              },
              completed: values.completed?.basic?.value === 1,
              timePlayedSeconds: values.timePlayedSeconds?.basic?.value || 0,
              kills: values.kills?.basic?.value || 0,
              deaths: values.deaths?.basic?.value || 0,
              assists: values.assists?.basic?.value || 0,
              killsDeathsRatio: values.killsDeathsRatio?.basic?.value || 0,
              precisionKills: extended.precisionKills?.basic?.value || 0,
              grenadeKills: extended.weaponKillsGrenade?.basic?.value || 0,
              meleeKills: extended.weaponKillsMelee?.basic?.value || 0,
              superKills: extended.weaponKillsSuper?.basic?.value || 0,
            };
          });

          console.log(`[HTTP:${requestId}] ✓ Returning PGCR with ${players.length} players`);

          return jsonResponse({
            activity: {
              name: activityDef?.displayProperties?.name || 'Unknown Activity',
              description: activityDef?.displayProperties?.description || '',
              pgcrImage: activityDef?.pgcrImage ? `https://www.bungie.net${activityDef.pgcrImage}` : null,
              instanceId: activity.instanceId,
              mode: activity.mode,
              modes: activity.modes || [],
            },
            period: pgcrData.period,
            activityDurationSeconds: pgcrData.activityDurationSeconds || 0,
            activityWasStartedFromBeginning: pgcrData.activityWasStartedFromBeginning || false,
            startingPhaseIndex: pgcrData.startingPhaseIndex || 0,
            players,
          }, 200, request, env);
        } catch (err) {
          console.error(`[HTTP:${requestId}] ❌ Error:`, err);
          return jsonResponse({ error: 'Failed to fetch PGCR' }, 500, request, env);
        }
      }

      // POST /admin/refresh
      if (url.pathname === '/admin/refresh' && request.method === 'POST') {
        const body = await request.json().catch(() => ({} as any));
        const type = (body.type as string) || 'all';
        
        console.log(`[HTTP:${requestId}] Admin refresh requested: type=${type}`);
        const results: Record<string, unknown> = {};

        if (type === 'members' || type === 'all') {
          console.log(`[HTTP:${requestId}] Running memberSyncCron...`);
          results.members = await memberSyncCron(env);
        }
        if (type === 'stats' || type === 'all') {
          console.log(`[HTTP:${requestId}] Running statsSyncCron...`);
          results.stats = await statsSyncCron(env);
        }

        console.log(`[HTTP:${requestId}] ✓ Admin refresh complete`);
        return jsonResponse({ success: true, results }, 200, request, env);
      }

      // POST /admin/recompute
      if (url.pathname === '/admin/recompute' && request.method === 'POST') {
        const body = await request.json().catch(() => ({} as any));
        const clanId = String(body.clanId ?? env.BUNGIE_CLAN_ID);
        
        console.log(`[HTTP:${requestId}] Admin recompute requested for clan: ${clanId}`);
        await (await import('./db/aggregateHelpers')).recomputeClanAggregateStats(env.DB, clanId);
        console.log(`[HTTP:${requestId}] ✓ Recompute complete`);
        
        return jsonResponse({ success: true, clanId }, 200, request, env);
      }

      // 404 - Not Found
      console.log(`[HTTP:${requestId}] ❌ 404 Not Found`);
      return jsonResponse({ error: 'Not found' }, 404, request, env);
      
    } catch (err: any) {
      console.error(`[HTTP:${requestId}] ❌ Unhandled error:`, err);
      return jsonResponse({ 
        error: 'Internal server error', 
        message: err?.message ?? String(err) 
      }, 500, request, env);
    }
  },

  // Queue consumer
  async queue(batch: any, env: Env, ctx?: any): Promise<void> {
    const queueName = (ctx && ctx.queue) || 'unknown';
    const batchId = Math.random().toString(36).slice(2, 8);
    
    console.log(`\n[Queue:${batchId}] Processing ${batch.messages.length} message(s) from queue: ${queueName}`);
    
    // Process each message and determine type from content
    let processedCount = 0;
    let errorCount = 0;
    
    for (let i = 0; i < batch.messages.length; i++) {
      const message = batch.messages[i];
      const body = message.body;
      
      try {
        // Detect message type by checking for specific fields
        // MemberJob has: displayName, membershipId, runId
        // StatsQueueJob has: jobId, dungeonHash, activities array
        
        if (body && 'displayName' in body && 'runId' in body) {
          // This is a MemberJob
          const job = body as MemberJob;
          console.log(`[Queue:${batchId}] Message ${i + 1}/${batch.messages.length}: MemberJob for ${job.displayName}`);
          
          await processMemberJob(env, job);
          message.ack();
          processedCount++;
          console.log(`[Queue:${batchId}] ✓ Message ${i + 1} acknowledged`);
          
        } else if (body && 'jobId' in body && 'activities' in body) {
          // This is a StatsQueueJob
          const job = body as StatsQueueJob;
          console.log(`[Queue:${batchId}] Message ${i + 1}/${batch.messages.length}: StatsQueueJob ${job.jobId}`);
          
          await processStatsQueueJob(env, job);
          message.ack();
          processedCount++;
          console.log(`[Queue:${batchId}] ✓ Message ${i + 1} acknowledged`);
          
        } else {
          console.error(`[Queue:${batchId}] ❌ Message ${i + 1}: Unknown message type`, body);
          errorCount++;
          message.ack(); // Ack it anyway to avoid reprocessing
        }
        
      } catch (err) {
        errorCount++;
        console.error(`[Queue:${batchId}] ❌ Message ${i + 1} failed:`, err);
        try { message.retry(); } catch {}
      }
    }
    
    console.log(`[Queue:${batchId}] Batch complete: ${processedCount} processed, ${errorCount} errors\n`);
  },

  // CRON handler
  async scheduled(event: any, env: Env): Promise<void> {
    const cronString = event.cron || '';
    const timestamp = new Date().toISOString();
    
    console.log(`\n[CRON] Triggered at ${timestamp}`);
    console.log(`[CRON] Cron expression: ${cronString}`);
    
    if (cronString.includes('*/30')) {
      console.log(`[CRON] Type: Member Sync (every 30 minutes)`);
      await memberSyncCron(env);
    } else if (cronString.includes('0 */6')) {
      console.log(`[CRON] Type: Stats Sync (every 6 hours)`);
      await statsSyncCron(env);
    } else {
      console.log(`[CRON] Unknown cron pattern: ${cronString}`);
    }
  },
};