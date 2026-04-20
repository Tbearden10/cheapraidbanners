// Main worker entry point - handles HTTP routes and cron triggers

import type { Env, MemberJob, StatsQueueJob } from './types';
import {
  fetchClanRoster,
  enrichMemberWithEmblem,
  fetchCharactersForMember,
  fetchActivitiesForCharacter,
  fetchPGCR,
  fetchActivityDefinition,
} from './api/bungieApi';
import { getClanAggregateStats, getMembersList, getMemberStats, upsertClanMember } from './db/queries';
import { processMemberJob } from './processors/memberJobProcessor';
import { processStatsQueueJob } from './processors/statsQueueProcessor';

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

function formatDisplayName(member: any): string {
  return member.bungieGlobalDisplayNameCode
    ? `${member.bungieGlobalDisplayName}#${member.bungieGlobalDisplayNameCode}`
    : member.bungieGlobalDisplayName || member.displayName;
}

// ============================================================================
// MEMBER SYNC CRON - Every hour
// ============================================================================
export async function memberSyncCron(env: Env): Promise<void> {
  const startTime = Date.now();
  const clanId = env.BUNGIE_CLAN_ID;
  
  console.log(`[MemberSync] Starting sync for clan ${clanId}`);

  // Fetch current roster from Bungie
  const rosterStartTime = Date.now();
  const roster = (await fetchClanRoster(clanId, env.BUNGIE_API_KEY)) || [];
  console.log(`[MemberSync] Roster fetch took ${Date.now() - rosterStartTime}ms (${roster.length} members)`);
  
  // If roster fetch failed or returned empty, abort to prevent marking all members inactive
  if (!roster || roster.length === 0) {
    console.log('[MemberSync] Failed to fetch roster - aborting sync');
    return;
  }
  
  const dbStartTime = Date.now();
  const dbMembers = await getMembersList(env.DB, clanId, false);
  console.log(`[MemberSync] DB fetch took ${Date.now() - dbStartTime}ms (${dbMembers.length} members)`);

  const dbMemberIds = new Set(dbMembers.map(m => m.membership_id));
  const rosterMemberIds = new Set((roster || []).map((m: any) => m.membershipId));

  const newMembers = roster.filter((m: any) => !dbMemberIds.has(m.membershipId));
  const leftMembers = dbMembers.filter(m => !rosterMemberIds.has(m.membership_id));

  if (newMembers.length > 0 || leftMembers.length > 0) {
    console.log(`[MemberSync] Changes: ${newMembers.length} new, ${leftMembers.length} left`);
  }

  // Parallel emblem enrichment with concurrency limit
  const emblemStartTime = Date.now();
  const CONCURRENCY = 20; // Process 20 at a time
  console.log(`[MemberSync] Starting emblem enrichment for ${roster.length} members (concurrency: ${CONCURRENCY})`);
  
  const enrichedMembers: any[] = [];
  
  // Process in batches
  for (let i = 0; i < roster.length; i += CONCURRENCY) {
    const batch = roster.slice(i, i + CONCURRENCY);
    const batchStartTime = Date.now();
    
    const batchResults = await Promise.all(
      batch.map(async (rawMember: any) => {
        const member = { ...rawMember };
        try {
          return await enrichMemberWithEmblem(member, env.BUNGIE_API_KEY);
        } catch {
          return {
            ...member,
            emblemPath: null,
            emblemBackgroundPath: null,
          };
        }
      })
    );
    
    enrichedMembers.push(...batchResults);
    
    const batchDuration = Date.now() - batchStartTime;
    console.log(`[MemberSync] Batch ${Math.floor(i/CONCURRENCY) + 1}/${Math.ceil(roster.length/CONCURRENCY)}: ${batch.length} members in ${batchDuration}ms (${(batchDuration/batch.length).toFixed(0)}ms avg)`);
  }
  
  const emblemDuration = Date.now() - emblemStartTime;
  const emblemSuccessCount = enrichedMembers.filter(m => m.emblemPath || m.emblemBackgroundPath).length;
  console.log(`[MemberSync] Emblem enrichment complete: ${emblemDuration}ms total (${emblemSuccessCount}/${roster.length} successful, ${(emblemDuration/roster.length).toFixed(0)}ms avg)`);

  // Process all roster members (upsert to DB)
  const upsertStartTime = Date.now();
  let successCount = 0;
  let errorCount = 0;

  for (const member of enrichedMembers) {
    const displayName = formatDisplayName(member);
    
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
      console.error(`[MemberSync] Failed to upsert ${displayName}:`, err);
    }
  }
  
  const upsertDuration = Date.now() - upsertStartTime;
  console.log(`[MemberSync] DB upserts: ${upsertDuration}ms total, ${(upsertDuration/roster.length).toFixed(0)}ms avg (${successCount} success, ${errorCount} failed)`);

  // Mark left members as inactive
  if (leftMembers.length > 0) {
    const inactiveStartTime = Date.now();
    console.log(`[MemberSync] Marking ${leftMembers.length} members as inactive`);
    for (const left of leftMembers) {
      try {
        await env.DB.prepare(
          `UPDATE clan_members SET is_active = 0, updated_at = ? WHERE clan_id = ? AND membership_id = ?`
        ).bind(Date.now(), clanId, left.membership_id).run();
      } catch {}
    }
    console.log(`[MemberSync] Inactive marking took ${Date.now() - inactiveStartTime}ms`);
  }

  // Queue new members and await completion so sends are not dropped
  if (newMembers.length > 0) {
    console.log(`[MemberSync] Queuing ${newMembers.length} new members for processing`);

    const queueResults = await Promise.allSettled(
      newMembers.map((newMember: { membershipId: any; membershipType: any }) => {
        const displayName = formatDisplayName(newMember);
        return env.MEMBER_STATS_QUEUE.send({
          clanId,
          membershipId: newMember.membershipId,
          membershipType: newMember.membershipType,
          displayName,
          lastProcessedDate: null, // Let the job processor fetch this
        });
      })
    );

    let queuedCount = 0;
    let failedCount = 0;

    queueResults.forEach((result, index) => {
      if (result.status === 'fulfilled') {
        queuedCount++;
      } else {
        failedCount++;
        const failedMember = newMembers[index];
        console.warn(
          `[MemberSync] Failed to queue new member ${failedMember.membershipId}:`,
          result.reason
        );
      }
    });

    if (failedCount > 0) {
      console.warn(
        `[MemberSync] Queueing complete with failures: ${queuedCount}/${newMembers.length} queued, ${failedCount} failed`
      );
    } else {
      console.log(`[MemberSync] Successfully queued ${queuedCount} new members`);
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
    const lastProcessedMs = Number(new Date(dbLastProcessedDate!).getTime());
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
        // Example for /members
        const cleanMembers = members.map(m => ({
          membershipId: m.membership_id,
          membershipType: m.membership_type,
          displayName: m.display_name,
          isOnline: m.is_online,
          emblemPath: m.emblem_path
        }));
        return jsonResponse({ members: cleanMembers }, 200, request, env);
      }

      // GET /stats
      if (url.pathname === '/stats' && request.method === 'GET') {
        try {
          const clanId = env.BUNGIE_CLAN_ID;
          
          // Get members
          const members = await getMembersList(env.DB, clanId, true); // active only
          
          // Get aggregate stats (direct aggregation)
          const aggregates = await getClanAggregateStats(env.DB, clanId);
          
          // Get member stats
          const membersWithStats = [];
          for (const member of members) {
            const stats = await getMemberStats(env.DB, clanId, member.membership_id);
            membersWithStats.push({
              membershipId: member.membership_id,
              displayName: member.display_name,
              stats: stats.map(s => ({
                dungeonHash: s.dungeon_hash,
                totalClears: s.total_clears,
                totalFullClears: s.total_full_clears,
                totalPlaytimeSeconds: s.total_playtime_seconds,
                lastProcessedDate: s.last_processed_date,
              })),
            });
          }

          return jsonResponse({
            members: membersWithStats,
            aggregateStats: [
              // Per-dungeon stats
              ...aggregates.perDungeon.map(d => ({
                dungeon_hash: d.dungeonHash,
                total_clears: d.totalClears,
                total_full_clears: d.totalFullClears,
                total_playtime_seconds: d.totalPlaytimeSeconds,
                active_member_count: d.activeMemberCount,
              })),
              // Overall stats as 'all'
              {
                dungeon_hash: 'all',
                total_clears: aggregates.overall.totalClears,
                total_full_clears: aggregates.overall.totalFullClears,
                total_playtime_seconds: aggregates.overall.totalPlaytimeSeconds,
                active_member_count: aggregates.overall.activeMemberCount,
              }
            ],
            memberCount: members.length,
            fetchedAt: new Date().toISOString(),
          }, 200, request, env);
        } catch (err) {
          console.error('[Stats] Error:', err);
          return jsonResponse({ error: 'Failed to fetch stats' }, 500, request, env);
        }
      }

      // GET /recent-activities
      if (url.pathname === '/recent-activities' && request.method === 'GET') {
        const clanId = url.searchParams.get('clanId') || env.BUNGIE_CLAN_ID;
        const ACTIVITIES_TO_RETURN = 3;

        try {
          // Step 1: Get 10 most recent active members
          const recentMembers = await env.DB.prepare(`
            SELECT membership_id
            FROM clan_members
            WHERE clan_id = ? AND is_active = 1
            ORDER BY last_online_status_change DESC
            LIMIT 10
          `).bind(clanId).all();

          if (!recentMembers.results || recentMembers.results.length === 0) {
            return jsonResponse([], 200, request, env);
          }

          const memberIds = recentMembers.results.map(m => m.membership_id);

          // Step 2: Get the most recent dungeon activity for each of these members
          const recentStats = await env.DB.prepare(`
            SELECT membership_id, dungeon_hash, last_processed_instance_id
            FROM member_dungeon_stats
            WHERE clan_id = ? AND membership_id IN (${memberIds.map(() => '?').join(',')})
              AND last_processed_instance_id IS NOT NULL
            ORDER BY last_processed_date DESC
            LIMIT ?
          `).bind(clanId, ...memberIds, ACTIVITIES_TO_RETURN * 2).all();

          if (!recentStats.results || recentStats.results.length === 0) {
            return jsonResponse([], 200, request, env);
          }

          // Step 3: Fetch PGCRs and activity images
          const activities = await Promise.all(
            recentStats.results.slice(0, ACTIVITIES_TO_RETURN).map(async (stat: any) => {
              let duration = 0;
              let completed = false;
              let image = '';

              try {
                // Fetch PGCR
                const pgcr = await fetchPGCR(stat.last_processed_instance_id, env.BUNGIE_API_KEY);
                if (pgcr) {
                  // Duration = longest timePlayedSeconds among all players
                  const playerDurations = pgcr.entries?.map((e: { values: { timePlayedSeconds: { basic: { value: any; }; }; }; }) => e?.values?.timePlayedSeconds?.basic?.value ?? 0) || [];
                  duration = Math.max(...playerDurations, 0);

                  // Completion = did this member complete the dungeon?
                  const memberEntry = pgcr.entries?.find(
                    (                    e: { player: { destinyUserInfo: { membershipId: any; }; }; }) => e?.player?.destinyUserInfo?.membershipId === stat.membership_id
                  );
                  completed = memberEntry?.values?.completed?.basic?.value === 1;

                  // Fetch activity definition for image only
                  const activityDef = await fetchActivityDefinition(String(stat.dungeon_hash), env.BUNGIE_API_KEY);
                  if (activityDef?.pgcrImage) {
                    image = `https://www.bungie.net${activityDef.pgcrImage}`;
                  }
                }
              } catch (err) {
                console.warn('[RecentActivities] Failed to fetch PGCR/activity image', stat.last_processed_instance_id, err);
              }

              return {
                instanceId: stat.last_processed_instance_id,
                completed,
                duration,
                image
              };
            })
          );

          // Only return valid activities
          const validActivities = activities.filter(a => a.instanceId).slice(0, ACTIVITIES_TO_RETURN);

          return jsonResponse(validActivities, 200, request, env);

        } catch (err) {
          console.error('[RecentActivities] Error:', err);
          return jsonResponse({ error: 'Failed to fetch recent activities' }, 500, request, env);
        }
      }




      // GET /pgcr
      if (url.pathname === '/pgcr' && request.method === 'GET') {
        const instanceId = url.searchParams.get('instanceId');
        if (!instanceId) return jsonResponse({ error: 'Missing instanceId parameter' }, 400, request, env);

        try {
          const pgcrData = await fetchPGCR(instanceId, env.BUNGIE_API_KEY);
          if (!pgcrData) return jsonResponse({ error: 'PGCR not found' }, 404, request, env);

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

            const displayName = player.destinyUserInfo?.bungieGlobalDisplayName || player.destinyUserInfo?.displayName;
            const nameCode = player.destinyUserInfo?.bungieGlobalDisplayNameCode;

            return {
              bungieGlobalDisplayName: displayName,
              bungieGlobalDisplayNameCode: nameCode,
              iconPath: player.destinyUserInfo?.iconPath ? `https://www.bungie.net${player.destinyUserInfo.iconPath}` : null,
              lightLevel: player.lightLevel,
              class: {
                name: player.characterClass || 'Unknown'
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

          // --- FIX: compute activityDurationSeconds as longest timePlayedSeconds ---
          const activityDurationSeconds = Math.max(...players.map((p: { timePlayedSeconds: any; }) => p.timePlayedSeconds), 0);

          return jsonResponse({
            activity: {
              name: activityDef?.displayProperties?.name || 'Unknown Activity',
              pgcrImage: activityDef?.pgcrImage ? `https://www.bungie.net${activityDef.pgcrImage}` : null,
            },
            period: pgcrData.period,
            activityDurationSeconds,
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
    const queueName = batch.queue || 'unknown';

    let processedCount = 0;
    let errorCount = 0;

    for (const message of batch.messages) {
      const body = message.body;

      try {
        // --- MEMBER JOB ---
        if (
          body &&
          'membershipId' in body &&
          'clanId' in body &&
          'displayName' in body
        ) {
          // This covers both new members and existing members updates
          const job = body as MemberJob;
          await processMemberJob(env, job);
          message.ack();
          processedCount++;

        // --- STATS QUEUE JOB ---
        } else if (
          body &&
          'jobId' in body &&
          'activities' in body
        ) {
          const job = body as StatsQueueJob;
          await processStatsQueueJob(env, job);
          message.ack();
          processedCount++;

        // --- UNKNOWN FORMAT ---
        } else {
          errorCount++;
          console.error(`[Queue][${queueName}] Unknown message format:`, body);
          message.ack(); // Ack anyway to avoid retry loops
        }

      } catch (err) {
        errorCount++;
        console.error(`[Queue][${queueName}] Error processing message:`, body, '\n', err);

        try {
          message.retry();
        } catch (retryErr) {
          console.error(`[Queue][${queueName}] Failed to retry message:`, retryErr);
        }
      }
    }

    console.log(
      `[Queue][${queueName}] Processed ${processedCount}/${batch.messages.length} messages (${errorCount} errors)`
    );
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
    } catch (err) {
      console.error('[CRON] Error:', err);
    }
  }
};