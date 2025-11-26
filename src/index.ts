// Main Cloudflare Worker for Clan Stats - Production Ready with CORS & Security
export { BatchCoordinator } from './durable-objects/BatchCoordinator';
export { RunTracker } from './durable-objects/RunTracker';

import type { Env } from './types';
import { fetchClanRoster, enrichMemberWithEmblem, fetchCharactersForMember, fetchActivitiesForCharacter, fetchPGCR, fetchActivityDefinition } from './api/bungieApi';
import {
  getMembersList,
  upsertClanMember,
  getClanAggregateStats,
} from './db/queries';
import { promisePool } from './processors/memberStatsProcessor';

// CORS Configuration
function getCorsHeaders(request: Request, env: Env) {
  const origin = request.headers.get('Origin') || '';
  
  // Allowed origins list
  const allowedOrigins = [
    'https://cheapraidbanners.com',
    'https://www.cheapraidbanners.com',
  ];

  // Development mode: allow localhost
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

  // Check if origin is allowed
  const allowedOrigin = allowedOrigins.find(allowed => origin === allowed);
  
  return {
    'Access-Control-Allow-Origin': allowedOrigin || allowedOrigins[0],
    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization',
    'Access-Control-Max-Age': '86400',
    'Vary': 'Origin',
  };
}

// Security headers
function getSecurityHeaders() {
  return {
    'X-Content-Type-Options': 'nosniff',
    'X-Frame-Options': 'DENY',
    'X-XSS-Protection': '1; mode=block',
    'Referrer-Policy': 'strict-origin-when-cross-origin',
  };
}

// JSON response helper
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

// Authentication check
function isAuthenticated(request: Request, env: Env): boolean {
  // Development mode: bypass auth
  if (env.ENVIRONMENT === 'dev' || env.ENVIRONMENT === 'development') {
    return true;
  }

  const authHeader = request.headers.get('Authorization');
  if (!authHeader) return false;

  const token = authHeader.replace(/^Bearer\s+/i, '');
  return token === env.API_TOKEN;
}

// Rate limiting helper (simple in-memory, upgrade to KV for production scale)
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

/** Member sync cron */
async function memberSyncCron(env: Env) {
  const clanId = env.BUNGIE_CLAN_ID;
  console.log('[MemberSync] fetching roster for clan', clanId);
  
  const roster = (await fetchClanRoster(clanId, env.BUNGIE_API_KEY)) || [];
  const dbMembers = await getMembersList(env.DB, clanId, false);
  const dbMemberIds = new Set(dbMembers.map((m) => m.membership_id));
  const rosterMemberIds = new Set((roster || []).map((m: any) => m.membershipId));

  const newMembers = (roster || []).filter((m: any) => !dbMemberIds.has(m.membershipId));
  const leftMembers = dbMembers.filter((m) => !rosterMemberIds.has(m.membership_id));

  const ENRICH_CONC = Number((env as any).ENRICH_CONCURRENCY) || 4;
  const enriched: any[] = [];
  
  await promisePool(
    roster,
    async (member: any) => {
      try {
        const e = await enrichMemberWithEmblem(member, env.BUNGIE_API_KEY);
        enriched.push(e);
      } catch (err) {
        console.warn('[MemberSync] enrich failed', member.membershipId, err);
        enriched.push({ ...member, emblemPath: null, emblemBackgroundPath: null });
      }
      return null;
    },
    ENRICH_CONC
  );

  await promisePool(
    enriched,
    async (member: any) => {
      const bungieGlobalDisplayName = member.bungieGlobalDisplayName || member.displayName;
      const bungieGlobalDisplayNameCode = member.bungieGlobalDisplayNameCode;
      const displayName = bungieGlobalDisplayNameCode 
        ? `${bungieGlobalDisplayName}#${bungieGlobalDisplayNameCode}` 
        : bungieGlobalDisplayName;

      await upsertClanMember(env.DB, {
        clanId,
        membershipId: member.membershipId,
        membershipType: Number(member.membershipType),
        displayName: displayName,
        isOnline: Boolean(member.isOnline),
        lastOnlineStatusChange: member.lastOnlineStatusChange ?? null,
        joinDate: member.joinDate ?? null,
        emblemPath: member.emblemPath ?? null,
        emblemBackgroundPath: member.emblemBackgroundPath ?? null,
        isActive: true,
      });
      return null;
    },
    4
  );

  for (const left of leftMembers) {
    try {
      await env.DB.prepare(
        `DELETE FROM member_dungeon_stats WHERE clan_id = ? AND membership_id = ?`
      ).bind(clanId, left.membership_id).run();

      await env.DB.prepare(
        `UPDATE clan_members SET is_active = 0, updated_at = ? WHERE clan_id = ? AND membership_id = ?`
      ).bind(Date.now(), clanId, left.membership_id).run();

      console.log('[MemberSync] Removed member', left.membership_id);
    } catch (err) {
      console.warn('[MemberSync] Error removing left member', left.membership_id, err);
    }
  }

  return { success: true, newMembers: newMembers.length, removedMembers: leftMembers.length };
}

/** Stats sync cron */
async function statsSyncCron(env: Env) {
  const clanId = env.BUNGIE_CLAN_ID;
  const members = await getMembersList(env.DB, clanId, true);
  
  if (!members || members.length === 0) {
    console.log('[StatsSync] No active members to queue');
    return { success: true, queued: 0 };
  }

  const runId = `run-${Date.now().toString(36)}-${Math.random().toString(36).slice(2,8)}`;
  
  try {
    const trackerId = env.RUN_TRACKER.idFromName(`run-tracker-${clanId}`);
    const tracker = env.RUN_TRACKER.get(trackerId);
    await tracker.fetch('https://internal/init', {
      method: 'POST',
      body: JSON.stringify({ runId, clanId, expectedCount: members.length }),
      headers: { 'Content-Type': 'application/json' },
    });
    console.log(`[StatsSync] initialized RunTracker for run ${runId} expected=${members.length}`);
  } catch (err) {
    console.warn('[StatsSync] Failed to init RunTracker', err);
  }

  const sendConcurrency = Number((env as any).QUEUE_SEND_CONCURRENCY) || 4;
  let queued = 0;

  await promisePool(
    members,
    async (member) => {
      try {
        await env.MEMBER_STATS_QUEUE.send({
          clanId,
          membershipId: member.membership_id,
          membershipType: Number(member.membership_type),
          displayName: member.display_name,
          lastProcessedDate: member.last_processed_date ?? null,
          runId,
        });
        queued++;
      } catch (err) {
        console.warn('[StatsSync] Failed to send queue message for', member.membership_id, err);
      }
      return null;
    },
    sendConcurrency
  );

  console.log('[StatsSync] queued', queued, 'members for run', runId);
  return { success: true, queued, runId };
}

/** Helper: fetch all member stats grouped */
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

/** MAIN WORKER EXPORT */
export default {
  async fetch(request: Request, env: Env) {
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
        const activeParam = url.searchParams.get('active');
        const activeOnly = activeParam === null ? true : activeParam === 'true';
        
        const members = await getMembersList(env.DB, clanId, activeOnly);
        
        return jsonResponse({ 
          members, 
          memberCount: members.length 
        }, 200, request, env);
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
          emblemPath: member.emblem_path ?? null,
          emblemBackgroundPath: member.emblem_background_path ?? null,
          stats: statsByMember.get(member.membership_id) || [],
          lastProcessedDate: member.last_processed_date ?? null,
        }));

        const aggregateStats = await getClanAggregateStats(env.DB, clanId);

        return jsonResponse({
          members: membersWithStats,
          aggregateStats,
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
          return jsonResponse({ 
            error: 'Missing required params: membershipType, membershipId, characterId' 
          }, 400, request, env);
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
          console.error('[ActivityHistory] error', err);
          return jsonResponse({ error: 'Failed to fetch activity history' }, 500, request, env);
        }
      }

      // GET /recent-activities
      if (url.pathname === '/recent-activities' && request.method === 'GET') {
        const clanId = url.searchParams.get('clanId') || env.BUNGIE_CLAN_ID;
        const MEMBERS_TO_CHECK = 5;
        const ACTIVITIES_PER_MEMBER = 2;
        const TOTAL_ACTIVITIES = 5;

        try {
          const roster = await fetchClanRoster(clanId, env.BUNGIE_API_KEY);
          if (!roster || roster.length === 0) {
            return jsonResponse([], 200, request, env);
          }

          const sorted = roster
            .filter((m: any) => m.membershipId && m.membershipType)
            .sort((a: any, b: any) => {
              const aTs = a.lastOnlineStatusChange || 0;
              const bTs = b.lastOnlineStatusChange || 0;
              return bTs - aTs;
            })
            .slice(0, MEMBERS_TO_CHECK);

          const allActivities: any[] = [];
          
          for (const member of sorted) {
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
            } catch (err) {
              console.warn('[RecentActivities] Failed for member', member.membershipId, err);
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
                  if (activityDef) {
                    activityDefCache.set(String(activityHash), activityDef);
                  }
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
              } catch (err) {
                console.warn('[RecentActivities] Failed to enrich', act.activityDetails?.instanceId, err);
                return act;
              }
            })
          );

          return jsonResponse(enrichedActivities, 200, request, env);
        } catch (err) {
          console.error('[RecentActivities] error', err);
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
          console.error('[PGCR] error', err);
          return jsonResponse({ error: 'Failed to fetch PGCR' }, 500, request, env);
        }
      }

      // POST /admin/refresh
      if (url.pathname === '/admin/refresh' && request.method === 'POST') {
        const body = await request.json().catch(() => ({} as any));
        const type = (body.type as string) || 'all';
        const results: Record<string, unknown> = {};

        if (type === 'members' || type === 'all') {
          results.members = await memberSyncCron(env);
        }
        if (type === 'stats' || type === 'all') {
          results.stats = await statsSyncCron(env);
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

      // 404 - Not Found
      return jsonResponse({ error: 'Not found' }, 404, request, env);
      
    } catch (err: any) {
      console.error('[Worker] Error:', err);
      return jsonResponse({ 
        error: 'Internal server error', 
        message: err?.message ?? String(err) 
      }, 500, request, env);
    }
  },

  async queue(batch: any, env: Env) {
    console.log(`\n[Queue] Received batch with ${batch.messages.length} message(s)`);

    const queueConcurrency = Number((env as any).QUEUE_PROCESS_CONCURRENCY) || 2;

    await promisePool(
      batch.messages,
      async (message: any) => {
        try {
          const job = message.body as any;
          console.log(`[Queue] Processing member: ${job.displayName}`);

          const { processMemberStats } = await import('./processors/memberStatsProcessor');
          await processMemberStats(env, job);

          if (job.runId && env.RUN_TRACKER) {
            try {
              const trackerId = env.RUN_TRACKER.idFromName(`run-tracker-${job.clanId}`);
              const tracker = env.RUN_TRACKER.get(trackerId);
              await tracker.fetch('https://internal/complete', {
                method: 'POST',
                body: JSON.stringify({ runId: job.runId, membershipId: job.membershipId }),
                headers: { 'Content-Type': 'application/json' },
              });
            } catch (err) {
              console.warn('[Queue] Failed to notify RunTracker', job.membershipId, err);
            }
          }

          try {
            message.ack();
          } catch (e) {
            console.warn('Failed to ack message:', e);
          }
          console.log(`[Queue] ✅ Completed: ${job.displayName}`);
        } catch (error) {
          console.error('[Queue] ❌ Failed to process message:', error);
          try {
            message.retry();
          } catch (e) {
            console.warn('Failed to retry message:', e);
          }
        }
        return null;
      },
      queueConcurrency
    );
  },

  async scheduled(event: any, env: Env) {
    console.log('[Cron] Triggered:', event.cron, 'scheduledTime:', event.scheduledTime);
    
    try {
      const cronString = event.cron || '';
      
      // More specific pattern matching - check stats sync first (more specific)
      if (cronString.includes('0 */6')) {
        console.log('[Cron] Running stats sync (every 6 hours)');
        await statsSyncCron(env);
      } 
      // Then check member sync
      else if (cronString.includes('*/30')) {
        console.log('[Cron] Running member sync (every 30 min)');
        await memberSyncCron(env);
      }
      // Fallback: if cron string doesn't match, determine by time
      else {
        const now = new Date(event.scheduledTime || Date.now());
        const minutes = now.getMinutes();
        const hours = now.getHours();
        
        // Stats sync runs at minute 0 every 6 hours
        if (minutes === 0 && hours % 6 === 0) {
          console.log('[Cron] Running stats sync (fallback time-based detection)');
          await statsSyncCron(env);
        } else {
          console.log('[Cron] Running member sync (fallback)');
          await memberSyncCron(env);
        }
      }
      
      console.log('[Cron] Completed successfully');
    } catch (err) {
      console.error('[Cron] Error:', err);
    }
  },
};