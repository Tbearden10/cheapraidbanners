// ============================================================================
// FILE: src/index.ts
// Main worker entry point - handles HTTP routes and cron triggers
// Logging reduced to key events only
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


// Export Durable Objects
export { RunTracker } from './durable-objects/RunTracker';
export { MemberCoordinator } from './durable-objects/MemberCoordinator'

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
  
  console.log(`[MemberSync] Starting sync for clan ${clanId}`);

  // Fetch current roster from Bungie
  const roster = (await fetchClanRoster(clanId, env.BUNGIE_API_KEY)) || [];
  const dbMembers = await getMembersList(env.DB, clanId, false);

  const dbMemberIds = new Set(dbMembers.map(m => m.membership_id));
  const rosterMemberIds = new Set((roster || []).map((m: any) => m.membershipId));

  const newMembers = roster.filter((m: any) => !dbMemberIds.has(m.membershipId));
  const leftMembers = dbMembers.filter(m => !rosterMemberIds.has(m.membership_id));

  if (newMembers.length > 0 || leftMembers.length > 0) {
    console.log(`[MemberSync] Changes: ${newMembers.length} new, ${leftMembers.length} left`);
  }

  // Process all roster members (new + existing)
  let successCount = 0;
  let errorCount = 0;

  for (let i = 0; i < roster.length; i++) {
    const rawMember = roster[i];
    const member = { ...rawMember };
    const displayName = member.bungieGlobalDisplayNameCode
      ? `${member.bungieGlobalDisplayName}#${member.bungieGlobalDisplayNameCode}`
      : member.bungieGlobalDisplayName || member.displayName;

    try {
      // Enrich emblem (best-effort) — suppress per-member debug
      try {
        const enriched = await enrichMemberWithEmblem(member, env.BUNGIE_API_KEY);
        Object.assign(member, enriched);
      } catch {
        member.emblemPath = null;
        member.emblemBackgroundPath = null;
      }

      // Upsert member into DB
      try {
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
        successCount++;
      } catch (err) {
        errorCount++;
      }
    } catch (err) {
      errorCount++;
    }
  }

  // Mark left members as inactive
  if (leftMembers.length > 0) {
    for (const left of leftMembers) {
      try {
        await env.DB.prepare(
          `UPDATE clan_members SET is_active = 0, updated_at = ? WHERE clan_id = ? AND membership_id = ?`
        ).bind(Date.now(), clanId, left.membership_id).run();
      } catch {}
    }
  }

  const duration = Date.now() - startTime;
  console.log(`[MemberSync] Complete: ${successCount} processed in ${(duration/1000).toFixed(1)}s`);
}

// ============================================================================
// STATS SYNC CRON - Every 6 hours
// ============================================================================

function shouldProcessMemberWithDb(member: any, dbLastProcessedDate: string | null): boolean {
  if (member.is_online) return true;

  const isEmpty = (v: unknown) =>
    v === null || v === undefined || (typeof v === 'string' && v.trim && v.trim() === '');

  const currResolved = (member as any).last_online_status_change_resolved ?? null;
  const currRaw = (member as any).last_online_status_change ?? null;

  if (isEmpty(currResolved) && isEmpty(currRaw) && isEmpty(dbLastProcessedDate)) return false;

  if (!isEmpty(currResolved) && !isEmpty(currResolved) && !isEmpty(dbLastProcessedDate)) {
    const lastOnlineMs = Number(currResolved);
    const lastProcessedMs = Number(new Date(dbLastProcessedDate).getTime());
    if (!Number.isNaN(lastOnlineMs) && !Number.isNaN(lastProcessedMs)) {
      return lastOnlineMs > lastProcessedMs;
    }
  }

  let lastProcessedMs: number | null = null;
  if (!isEmpty(dbLastProcessedDate)) {
    const parsed = new Date(String(dbLastProcessedDate)).getTime();
    if (!Number.isNaN(parsed)) lastProcessedMs = parsed;
  }

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

  if (lastProcessedMs && latestOnlineMs) {
    return lastProcessedMs < latestOnlineMs;
  }

  return true;
}

async function statsSyncCron(env: Env, options?: { force?: boolean }): Promise<void> {
  const force = !!options?.force;
  const startTime = Date.now();
  const clanId = env.BUNGIE_CLAN_ID;
  
  console.log(`[StatsSync] Starting sync for clan ${clanId}${force ? ' (forced)' : ''}`);

  const members = await getMembersList(env.DB, clanId, true);
  if (!members || members.length === 0) {
    console.log('[StatsSync] No active members');
    return;
  }

  // Decide which members to process (keep concise logs)
  const membersToProcess: typeof members = [];

  for (const member of members) {
    try {
      if (force) {
        membersToProcess.push(member);
        continue;
      }

      const dbSummary = await env.DB.prepare(`
        SELECT 
          MAX(last_processed_date) AS last_processed_date,
          SUM(COALESCE(total_clears, 0)) AS sum_total_clears,
          SUM(COALESCE(total_full_clears, 0)) AS sum_total_full_clears
        FROM member_dungeon_stats
        WHERE clan_id = ? AND membership_id = ?
      `).bind(clanId, member.membership_id).first();

      const dbLastProcessed = dbSummary ? (dbSummary as any).last_processed_date ?? null : null;
      const sumTotalClears = dbSummary ? Number((dbSummary as any).sum_total_clears ?? 0) : 0;
      const sumTotalFullClears = dbSummary ? Number((dbSummary as any).sum_total_full_clears ?? 0) : 0;

      const isNewMember = !dbLastProcessed && sumTotalClears === 0 && sumTotalFullClears === 0;

      const prevLastOnline = (member as any).last_online_status_change_prev ?? null;
      const currLastOnline = (member as any).last_online_status_change ?? null;
      const lastOnlineChanged = prevLastOnline && currLastOnline && String(prevLastOnline) !== String(currLastOnline);

      const shouldProcessFlag =
        Boolean(member.is_online) ||
        isNewMember ||
        Boolean(lastOnlineChanged) ||
        shouldProcessMemberWithDb(member, dbLastProcessed);

      if (shouldProcessFlag) {
        membersToProcess.push(member);
      }
    } catch (err) {
      // If decision fails, default to processing
      membersToProcess.push(member);
    }
  }

  console.log(`[StatsSync] Processing ${membersToProcess.length}/${members.length} members`);

  if (membersToProcess.length === 0) {
    console.log('[StatsSync] Nothing to process');
    return;
  }

  const runId = `run-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;

  try {
    const trackerId = env.RUN_TRACKER.idFromName(`run-tracker-${clanId}`);
    const tracker = env.RUN_TRACKER.get(trackerId);
    const res = await tracker.fetch('https://internal/init', {
      method: 'POST',
      body: JSON.stringify({
        runId,
        clanId,
        expectedCount: membersToProcess.length,
      }),
      headers: { 'Content-Type': 'application/json' },
    });


    try {
      await res.text();
    } catch (e) {
      try { res.body?.cancel(); } catch {}
    }
  } catch (err) {
    console.warn('[StatsSync] RunTracker init failed:', String(err));
  }

  // Queue members sequentially
  let queuedCount = 0;
  let queueErrorCount = 0;

  for (let i = 0; i < membersToProcess.length; i++) {
    const member = membersToProcess[i];

    try {
      const prevRow = await env.DB.prepare(`
        SELECT MAX(last_processed_date) AS last_processed_date
        FROM member_dungeon_stats
        WHERE clan_id = ? AND membership_id = ?
      `).bind(clanId, member.membership_id).first();

      const lastProcessedDate = (prevRow ? (prevRow as any).last_processed_date ?? null : null);

      await env.MEMBER_STATS_QUEUE.send({
        clanId,
        membershipId: member.membership_id,
        membershipType: member.membership_type,
        displayName: member.display_name,
        lastProcessedDate,
        runId,
      });
      queuedCount++;
    } catch (err) {
      queueErrorCount++;
    }
  }

  const duration = Date.now() - startTime;
  console.log(`[StatsSync] Complete: ${queuedCount} members queued in ${(duration/1000).toFixed(1)}s`);
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

    // CORS preflight
    if (request.method === 'OPTIONS') {
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
          return jsonResponse({ error: 'Rate limit exceeded' }, 429, request, env);
        }
      }

      // Public GET endpoints
      const publicGetPaths = new Set(['/members', '/stats', '/activity-history', '/recent-activities', '/pgcr']);

      // Auth check for protected endpoints
      if (!(request.method === 'GET' && publicGetPaths.has(url.pathname))) {
        if (!isAuthenticated(request, env)) {
          return jsonResponse({ error: 'Unauthorized' }, 401, request, env);
        }
      }

      // GET /members
      if (url.pathname === '/members' && request.method === 'GET') {
        const clanId = url.searchParams.get('clanId') || env.BUNGIE_CLAN_ID;
        const members = await getMembersList(env.DB, clanId, true);
        return jsonResponse({ members }, 200, request, env);
      }

      // GET /stats
      if (url.pathname === '/stats' && request.method === 'GET') {
        const clanId = url.searchParams.get('clanId') || env.BUNGIE_CLAN_ID;
        const members = await getMembersList(env.DB, clanId, true);
        const statsByMember = await fetchAllMemberStatsGrouped(env.DB, clanId);

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
          stats: statsByMember.get(String(member.membership_id)) || [],
          lastProcessedDate: member.last_processed_date ?? null,
        }));

        const aggregateStats = await env.DB.prepare(`SELECT * FROM clan_aggregate_stats WHERE clan_id = ? ORDER BY dungeon_hash`).bind(clanId).all();

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

        if (!membershipType || !membershipId || !characterId) {
          return jsonResponse({ error: 'Missing required params: membershipType, membershipId, characterId' }, 400, request, env);
        }

        try {
          const activities = await fetchActivitiesForCharacter(
            Number(membershipType),
            membershipId,
            characterId,
            Number(page),
            Number(mode),
            Number(count),
            env.BUNGIE_API_KEY
          );
          return jsonResponse(activities, 200, request, env);
        } catch (err) {
          return jsonResponse({ error: 'Failed to fetch activity history' }, 500, request, env);
        }
      }

      // GET /recent-activities
      if (url.pathname === '/recent-activities' && request.method === 'GET') {
        const clanId = url.searchParams.get('clanId') || env.BUNGIE_CLAN_ID;
        const MEMBERS_TO_CHECK = 10;
        const ACTIVITIES_PER_MEMBER = 1;
        const TOTAL_ACTIVITIES = 3;

        try {
          const roster = await fetchClanRoster(clanId, env.BUNGIE_API_KEY);
          if (!roster || roster.length === 0) {
            return jsonResponse([], 200, request, env);
          }

          const sorted = roster
            .filter((m: any) => m.membershipId && m.membershipType)
            .sort((a: any, b: any) => {
              const aMs = (a && a.lastOnlineStatusChange) ? Number(a.lastOnlineStatusChange) : 0;
              const bMs = (b && b.lastOnlineStatusChange) ? Number(b.lastOnlineStatusChange) : 0;
              return bMs - aMs;
            })
            .slice(0, MEMBERS_TO_CHECK);

          const allActivities: any[] = [];
          
          for (let i = 0; i < sorted.length; i++) {
            const member = sorted[i];
            try {
              const characters = await fetchCharactersForMember(
                member.membershipId,
                member.membershipType,
                env.BUNGIE_API_KEY
              );
              
              if (!characters || characters.length === 0) continue;
              
              const activities = await fetchActivitiesForCharacter(
                member.membershipType,
                member.membershipId,
                characters[0].characterId,
                0,
                0,
                ACTIVITIES_PER_MEMBER,
                env.BUNGIE_API_KEY
              );
              
              if (!activities || activities.length === 0) continue;
              
              for (const act of activities) {
                allActivities.push({
                  ...act,
                  memberDisplayName: member.displayName,
                  membershipId: member.membershipId,
                  membershipType: member.membershipType,
                });
              }
            } catch {
              continue;
            }
          }

          const sorted_activities = allActivities
            .filter(a => a.period)
            .sort((a, b) => new Date(b.period).getTime() - new Date(a.period).getTime())
            .slice(0, TOTAL_ACTIVITIES);

          const activityDefCache = new Map<string, any>();
          
          const enrichedActivities = await Promise.all(
            sorted_activities.map(async (act) => {
              try {
                const activityHash = act.activityDetails?.directorActivityHash || act.activityDetails?.referenceId;
                if (!activityHash) return act;

                let activityDef = activityDefCache.get(String(activityHash));
                if (!activityDef) {
                  activityDef = await fetchActivityDefinition(String(activityHash), env.BUNGIE_API_KEY);
                  if (activityDef) activityDefCache.set(String(activityHash), activityDef);
                }

                if (activityDef?.pgcrImage) {
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
              } catch {
                return act;
              }
            })
          );

          return jsonResponse(enrichedActivities, 200, request, env);
        } catch (err) {
          return jsonResponse({ error: 'Failed to fetch recent activities' }, 500, request, env);
        }
      }

      // GET /pgcr
      if (url.pathname === '/pgcr' && request.method === 'GET') {
        const instanceId = url.searchParams.get('instanceId');
        
        if (!instanceId) {
          return jsonResponse({ error: 'Missing instanceId parameter' }, 400, request, env);
        }

        try {
          const pgcrData = await fetchPGCR(instanceId, env.BUNGIE_API_KEY);
          
          if (!pgcrData) {
            return jsonResponse({ error: 'PGCR not found' }, 404, request, env);
          }

          const activity = pgcrData.activityDetails || {};
          const activityHash = activity.directorActivityHash || activity.referenceId;

          let activityDef = null;
          if (activityHash) {
            activityDef = await fetchActivityDefinition(String(activityHash), env.BUNGIE_API_KEY);
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
          return jsonResponse({ error: 'Failed to fetch PGCR' }, 500, request, env);
        }
      }

      // POST /admin/refresh
      if (url.pathname === '/admin/refresh' && request.method === 'POST') {
        const body = await request.json().catch(() => ({} as any));
        const type = (body.type as string) || 'all';
        const force = !!body.force;
        const clanId = String(body.clanId ?? env.BUNGIE_CLAN_ID);

        const results: Record<string, unknown> = {};

        // If force is requested and stats should be refreshed, clear existing stats for a fresh sync
        if (force) {
          try {
            await env.DB.prepare(`DELETE FROM member_dungeon_stats WHERE clan_id = ?`).bind(clanId).run();
            await env.DB.prepare(`DELETE FROM clan_aggregate_stats WHERE clan_id = ?`).bind(clanId).run();
            results.cleared = true;
          } catch (err) {
            results.cleared = false;
            results.clearError = (err as any)?.message ?? String(err);
          }
        }

        if (type === 'members' || type === 'all') {
          results.members = await memberSyncCron(env);
        }
        if (type === 'stats' || type === 'all') {
          results.stats = await statsSyncCron(env, { force });
        }

        return jsonResponse({ success: true, results }, 200, request, env);
      }

      // POST /admin/recompute
      if (url.pathname === '/admin/recompute' && request.method === 'POST') {
        const body = await request.json().catch(() => ({} as any));
        const clanId = String(body.clanId ?? env.BUNGIE_CLAN_ID);
        
        await (await import('./db/aggregateHelpers')).recomputeClanAggregateStats(env.DB, clanId);
        
        return jsonResponse({ success: true, clanId }, 200, request, env);
      }

      // GET /debug/user-completions - Debug endpoint to check completion counts for a single user
      if (url.pathname === '/debug/user-completions' && request.method === 'GET') {
        const membershipId = url.searchParams.get('membershipId');
        const membershipType = url.searchParams.get('membershipType');
        const clanId = url.searchParams.get('clanId') || env.BUNGIE_CLAN_ID;

        if (!membershipId || !membershipType) {
          return jsonResponse({ 
            error: 'Missing required params: membershipId, membershipType' 
          }, 400, request, env);
        }

        try {
          const { ACTIVITY_REFERENCE_MAP } = await import('./constants/activityReferenceMap');
          const { withRateLimit } = await import('./api/bungieApi');
          
          console.log(`[Debug] Fetching completions for ${membershipId}`);

          // Track network requests and rate limit retries
          const networkLog: any[] = [];
          const rateLimitRetries: any[] = [];
          let totalPagesFetched = 0;
          let totalRateLimitRetries = 0;

          // Fetch characters
          const characters = await withRateLimit(
            () => fetchCharactersForMember(membershipId, Number(membershipType), env.BUNGIE_API_KEY),
            3
          );

          if (!characters || characters.length === 0) {
            return jsonResponse({ 
              error: 'No characters found',
              membershipId,
              membershipType,
            }, 404, request, env);
          }

          // Initialize activity storage
          const activitiesByChar: Record<string, any[]> = {};
          for (const char of characters) {
            activitiesByChar[char.characterId] = [];
          }

          const modes = [82, 2]; // Dungeon, Story
          for (const mode of modes) {
            for (const char of characters) {
              let page = 0;
              const pageSize = 250;
              
              while (true) {
                totalPagesFetched++;
                
                // Fetch with rate limit handling and retry tracking
                const activities = await withRateLimit(
                  async () => {
                    try {
                      return await fetchActivitiesForCharacter(
                        Number(membershipType),
                        membershipId,
                        char.characterId,
                        page,
                        mode,
                        pageSize,
                        env.BUNGIE_API_KEY
                      );
                    } catch (err: any) {
                      if (err.message && (err.message.includes('429') || err.message.includes('rate limit'))) {
                        totalRateLimitRetries++;
                      }
                      throw err;
                    }
                  },
                  3
                ).catch(() => []);

                if (activities && activities.length > 0) {
                  activitiesByChar[char.characterId].push(...activities);
                }

                if (!activities || activities.length < pageSize) {
                  break;
                }
                page++;
              }
            }
          }

          const totalActivitiesFetched = Object.values(activitiesByChar).reduce(
            (sum, acts) => sum + acts.length, 0
          );

          // Group by dungeon hash
          const activitiesByDungeon: Record<string, any[]> = {};
          const ungroupedActivities: any[] = [];
          const missingRefIdActivities: any[] = [];
          let totalProcessed = 0;
          
          for (const dungeon of ACTIVITY_REFERENCE_MAP) {
            activitiesByDungeon[dungeon.hash] = [];
          }

          for (const charId of Object.keys(activitiesByChar)) {
            for (const activity of activitiesByChar[charId]) {
              totalProcessed++;
              const refId = String(activity?.activityDetails?.referenceId || '');
              
              if (!refId) {
                missingRefIdActivities.push({
                  characterId: charId,
                  period: activity.period,
                  instanceId: activity?.activityDetails?.instanceId || activity?.instanceId,
                });
                continue;
              }
              
              let grouped = false;
              for (const dungeon of ACTIVITY_REFERENCE_MAP) {
                if (dungeon.referenceIds.includes(refId)) {
                  activitiesByDungeon[dungeon.hash].push({
                    ...activity,
                    characterId: charId,
                  });
                  grouped = true;
                  break;
                }
              }
              
              if (!grouped) {
                ungroupedActivities.push({
                  referenceId: refId,
                  characterId: charId,
                  period: activity.period,
                  instanceId: activity?.activityDetails?.instanceId || activity?.instanceId,
                });
              }
            }
          }

          // Deduplicate per dungeon by instanceId
          const deduplicationStats: Record<string, { before: number; after: number; removed: number; missingInstanceId: number }> = {};
          let totalBeforeDedup = 0;
          let totalAfterDedup = 0;
          let totalMissingInstanceIds = 0;
          
          for (const hash of Object.keys(activitiesByDungeon)) {
            const beforeCount = activitiesByDungeon[hash].length;
            totalBeforeDedup += beforeCount;
            
            const map = new Map<string, any>();
            const instanceIdsWithoutId: any[] = [];
            
            for (const act of activitiesByDungeon[hash]) {
              const id = act.activityDetails?.instanceId || act.instanceId;
              
              if (!id) {
                instanceIdsWithoutId.push({
                  characterId: act.characterId,
                  period: act.period,
                  referenceId: act.activityDetails?.referenceId,
                });
                continue;
              }
              
              const existing = map.get(id);
              if (!existing) {
                map.set(id, act);
              } else {
                const existingCompleted = !!(existing?.values?.completed?.basic?.value === 1);
                const newCompleted = !!(act?.values?.completed?.basic?.value === 1);
                if (!existingCompleted && newCompleted) {
                  map.set(id, act);
                }
              }
            }
            
            activitiesByDungeon[hash] = Array.from(map.values());
            const afterCount = activitiesByDungeon[hash].length;
            totalAfterDedup += afterCount;
            totalMissingInstanceIds += instanceIdsWithoutId.length;
            
            deduplicationStats[hash] = {
              before: beforeCount,
              after: afterCount,
              removed: beforeCount - afterCount - instanceIdsWithoutId.length,
              missingInstanceId: instanceIdsWithoutId.length,
            };
          }

          // Build results per dungeon
          const dungeonResults = [];
          
          for (const dungeon of ACTIVITY_REFERENCE_MAP) {
            const dungeonHash = dungeon.hash;
            const activities = activitiesByDungeon[dungeonHash] || [];
            
            const countsByRefId: Record<string, number> = {};
            for (const refId of dungeon.referenceIds) {
              countsByRefId[refId] = 0;
            }
            for (const act of activities) {
              const refId = String(act?.activityDetails?.referenceId || '');
              if (countsByRefId.hasOwnProperty(refId)) {
                countsByRefId[refId]++;
              }
            }
            
            const completed = activities.filter(a => a?.values?.completed?.basic?.value === 1);
            const incomplete = activities.filter(a => a?.values?.completed?.basic?.value !== 1);
            
            completed.sort((a, b) => {
              const ta = a.period ? new Date(a.period).getTime() : 0;
              const tb = b.period ? new Date(b.period).getTime() : 0;
              return ta - tb;
            });

            // Get DB stats for comparison
            const dbRow = await env.DB.prepare(`
              SELECT total_clears, total_full_clears, last_processed_date
              FROM member_dungeon_stats
              WHERE clan_id = ? AND membership_id = ? AND dungeon_hash = ?
            `).bind(clanId, membershipId, dungeonHash).first();

            const dbTotalClears = dbRow ? Number((dbRow as any).total_clears ?? 0) : 0;
            const dbFullClears = dbRow ? Number((dbRow as any).total_full_clears ?? 0) : 0;
            const dbLastProcessedDate = dbRow ? (dbRow as any).last_processed_date : null;

            const dedupInfo = deduplicationStats[dungeonHash] || { before: 0, after: 0, removed: 0 };

            dungeonResults.push({
              dungeonName: dungeon.displayName,
              dungeonHash,
              referenceIds: dungeon.referenceIds,
              deduplication: dedupInfo,
              countsByReferenceId: countsByRefId,
              bungie: {
                totalActivities: activities.length,
                completedActivities: completed.length,
                incompleteActivities: incomplete.length,
                oldestCompletion: completed.length > 0 ? completed[0].period : null,
                newestCompletion: completed.length > 0 ? completed[completed.length - 1].period : null,
              },
              database: {
                totalClears: dbTotalClears,
                fullClears: dbFullClears,
                lastProcessedDate: dbLastProcessedDate,
              },
              comparison: {
                missingInDb: completed.length - dbTotalClears,
                needsSync: completed.length > dbTotalClears,
              },
              recentCompletions: completed.slice(-5).map(a => ({
                instanceId: a.activityDetails?.instanceId || a.instanceId,
                period: a.period,
                characterId: (a as any).characterId,
                referenceId: a.activityDetails?.referenceId,
              })),
            });
          }

          // Final summary
          const totalBungieCompletions = dungeonResults.reduce((sum, d) => sum + d.bungie.completedActivities, 0);
          const totalDbClears = dungeonResults.reduce((sum, d) => sum + d.database.totalClears, 0);
          const needsSyncCount = dungeonResults.filter(d => d.comparison.needsSync).length;

          console.log(`[Debug] Complete: ${totalActivitiesFetched} activities, ${totalBungieCompletions} completions`);

          // Group ungrouped by reference ID for response
          const ungroupedByRefId: Record<string, number> = {};
          for (const act of ungroupedActivities) {
            const refId = act.referenceId;
            ungroupedByRefId[refId] = (ungroupedByRefId[refId] || 0) + 1;
          }
          
          // Group missing refId activities for response
          const missingRefIdByChar: Record<string, number> = {};
          for (const act of missingRefIdActivities) {
            const charId = act.characterId;
            missingRefIdByChar[charId] = (missingRefIdByChar[charId] || 0) + 1;
          }

          return jsonResponse({
            membershipId,
            membershipType: Number(membershipType),
            clanId,
            characters: characters.map((c: any) => ({
              characterId: c.characterId,
              deleted: c.deleted || false,
              activitiesFetched: activitiesByChar[c.characterId]?.length || 0,
            })),
            totalActivitiesFetched,
            networkSummary: {
              totalPagesFetched,
              totalRateLimitRetries,
            },
            missingRefIdActivities: {
              count: missingRefIdActivities.length,
              byCharacterId: missingRefIdByChar,
              samples: missingRefIdActivities.slice(0, 10).map(a => ({
                characterId: a.characterId,
                period: a.period,
                instanceId: a.instanceId,
              })),
            },
            ungroupedActivities: {
              count: ungroupedActivities.length,
              byReferenceId: ungroupedByRefId,
              samples: ungroupedActivities.slice(0, 10).map(a => ({
                referenceId: a.referenceId,
                characterId: a.characterId,
                period: a.period,
                instanceId: a.instanceId,
              })),
            },
            dungeons: dungeonResults,
            summary: {
              totalBungieCompletions,
              totalDbClears,
              needsSyncCount,
              missingInDb: totalBungieCompletions - totalDbClears,
            },
          }, 200, request, env);
        } catch (err) {
          return jsonResponse({ 
            error: 'Failed to fetch user completions',
            message: (err as any)?.message ?? String(err)
          }, 500, request, env);
        }
      }

      // 404 - Not Found
      return jsonResponse({ error: 'Not found' }, 404, request, env);
      
    } catch (err: any) {
      return jsonResponse({ 
        error: 'Internal server error', 
        message: err?.message ?? String(err) 
      }, 500, request, env);
    }
  },

  // Queue consumer
  async queue(batch: any, env: Env, ctx?: any): Promise<void> {
    const queueName = (ctx && ctx.queue) || 'unknown';
    
    // Process each message and determine type from content
    let processedCount = 0;
    let errorCount = 0;
    
    for (let i = 0; i < batch.messages.length; i++) {
      const message = batch.messages[i];
      const body = message.body;
      
      try {
        // Detect message type by checking for specific fields
        if (body && 'displayName' in body && 'runId' in body) {
          const job = body as MemberJob;
          await processMemberJob(env, job);
          message.ack();
          processedCount++;
          
        } else if (body && 'jobId' in body && 'activities' in body) {
          const job = body as StatsQueueJob;
          await processStatsQueueJob(env, job);
          message.ack();
          processedCount++;
          
        } else {
          errorCount++;
          message.ack(); // Ack it anyway to avoid reprocessing
        }
        
      } catch (err) {
        errorCount++;
        try { message.retry(); } catch {}
      }
    }
    
    if (errorCount > 0) {
      console.log(`[Queue] Processed ${processedCount}/${batch.messages.length} messages (${errorCount} errors)`);
    }
  },

  async scheduled(event: any, env: Env): Promise<void> {
    const cron = event.cron || 'unknown';
    
    try {
      // Stats sync: runs daily at 12 PM MST (19:00 UTC)
      if (cron === '0 19 * * *') {
        console.log('[CRON] Running stats sync');
        await statsSyncCron(env);
      } 
      // Member sync: runs every hour
      else if (cron === '0 * * * *') {
        console.log('[CRON] Running member sync');
        await memberSyncCron(env);
      } 
      // Aggregate recompute: runs daily at 1 PM MST (20:00 UTC)
      else if (cron === '0 20 * * *') {
        console.log('[CRON] Running aggregate recompute');
        const clanId = env.BUNGIE_CLAN_ID;
        await (await import('./db/aggregateHelpers')).recomputeClanAggregateStats(env.DB, clanId);
      }
    } catch (err) {
      console.error('[CRON] Error:', err);
    }
  }
};