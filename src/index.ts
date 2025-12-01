// ============================================================================
// FILE: src/index.ts
// Main worker entry point - reduced logging to essentials, but more verbose
// when running member sync (enrichment of emblems runs in parallel batches).
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
import { trackRunStart, trackRunProgress, trackRunComplete, getRunInfo, getLatestRuns } from './kv/runTracker'
import { getMembersList, upsertClanMember } from './db/queries';
import { processMemberJob } from './processors/memberJobProcessor';
import { processStatsQueueJob } from './processors/statsQueueProcessor';
export { BatchCoordinator } from './durable-objects/BatchCoordinator';

async function promisePool<T, R>(
  items: T[], 
  worker: (item: T) => Promise<R>, 
  concurrency: number
) {
  const results: R[] = new Array(items.length);
  let i = 0;
  const runners = new Array(Math.min(concurrency, items.length)).fill(0).map(async () => {
    while (true) {
      const idx = i++;
      if (idx >= items.length) return;
      try {
        results[idx] = await worker(items[idx]);
      } catch (err) {
        (results as any)[idx] = undefined as any;
      }
    }
  });
  await Promise.all(runners);
  return results;
}

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

// MEMBER SYNC CRON - Every 30 minutes
export async function memberSyncCron(env: Env): Promise<void> {
  const clanId = env.BUNGIE_CLAN_ID;
  const runId = `members-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
  console.log(`[MemberSync] START clan=${clanId} run=${runId} time=${new Date().toISOString()}`);

  const roster = (await fetchClanRoster(clanId, env.BUNGIE_API_KEY)) || [];
  const dbMembers = await getMembersList(env.DB, clanId, false);

  const dbMemberIds = new Set(dbMembers.map(m => m.membership_id));
  const rosterMemberIds = new Set((roster || []).map((m: any) => m.membershipId));

  const newMembers = roster.filter((m: any) => !dbMemberIds.has(m.membershipId));
  const leftMembers = dbMembers.filter(m => !rosterMemberIds.has(m.membership_id));
  const existingMembers = roster.filter((m: any) => dbMemberIds.has(m.membershipId));

  console.log(
    `[MemberSync] roster=${roster.length} new=${newMembers.length} left=${leftMembers.length} update=${existingMembers.length}`
  );

  // Process roster members
  let successCount = 0;
  let errorCount = 0;
  let enrichmentErrorCount = 0;

  // We'll fetch/enrich emblems in parallel in batches to avoid hammering Bungie and to speed up the process.
  const EMBLEM_BATCH_SIZE = 7;
  const totalBatches = Math.max(1, Math.ceil(roster.length / EMBLEM_BATCH_SIZE));
  console.log(`[MemberSync] Enriching emblems in batches of ${EMBLEM_BATCH_SIZE} (total batches=${totalBatches})`);

  await trackRunStart(env.RUN_TRACKING_KV, runId, 'members', roster.length);

  for (let batchIndex = 0; batchIndex < totalBatches; batchIndex++) {
    const start = batchIndex * EMBLEM_BATCH_SIZE;
    const batch = roster.slice(start, start + EMBLEM_BATCH_SIZE);
    console.log(
      `[MemberSync] Starting enrichment batch ${batchIndex + 1}/${totalBatches} members=${batch.length}`
    );

    // Use Promise.allSettled so one failure doesn't short-circuit the batch.
    const settled = await Promise.allSettled(
      batch.map((member: any) =>
        enrichMemberWithEmblem(member, env.BUNGIE_API_KEY).catch((err) => {
          // Catch to avoid Promise rejection; we'll handle via allSettled as well.
          console.error(`[MemberSync] ❌ Enrichment failed for ${member.membershipId}:`, err);
          return null;
        })
      )
    );

    const enrichedBatch: any[] = batch.map((originalMember: any, idx: any) => {
      const result = settled[idx];
      if (result.status === 'fulfilled' && result.value) {
        return { ...originalMember, ...result.value };
      }
      // If enrichment failed, return original member with null emblem fields
      enrichmentErrorCount++;
      return {
        ...originalMember,
        emblemPath: null,
        emblemBackgroundPath: null,
      };
    });

    console.log(
      `[MemberSync] Completed enrichment batch ${batchIndex + 1}/${totalBatches} - proceeding to upsert ${enrichedBatch.length} members`
    );

    // Upsert the enriched members sequentially to avoid DB contention; log each upsert result.
    for (const member of enrichedBatch) {
      const displayName = member.bungieGlobalDisplayNameCode
        ? `${member.bungieGlobalDisplayName}#${member.bungieGlobalDisplayNameCode}`
        : member.bungieGlobalDisplayName || member.displayName;

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
        await trackRunProgress(env.RUN_TRACKING_KV, runId, { processed: 1 });
        console.log(`[MemberSync] ✔ Upserted ${displayName} (${member.membershipId})`);
      } catch (err) {
        errorCount++;
        await trackRunProgress(env.RUN_TRACKING_KV, runId, { errors: 1 });
        console.error(`[MemberSync] ❌ Failed to upsert ${displayName} (${member.membershipId}):`, err);
      }
    }

    // Small delay between batches to be nice to the API and DB (configurable if needed).
    if (batchIndex < totalBatches - 1) {
      await new Promise((r) => setTimeout(r, 150));
    }
  }

  // Mark left members as inactive
  if (leftMembers.length > 0) {
    console.log(`[MemberSync] Marking ${leftMembers.length} left members inactive`);
    for (const left of leftMembers) {
      try {
        await env.DB.prepare(
          `UPDATE clan_members SET is_active = 0, updated_at = ? WHERE clan_id = ? AND membership_id = ?`
        ).bind(Date.now(), clanId, left.membership_id).run();
        console.log(`[MemberSync] ✔ Marked inactive ${left.display_name} (${left.membership_id})`);
      } catch (err) {
        console.error(`[MemberSync] ❌ Failed to mark ${left.display_name} inactive:`, err);
      }
    }
  }

  // At the end, before the final console.log
  await trackRunComplete(
    env.RUN_TRACKING_KV, 
    runId, 
    errorCount > 0 ? 'failed' : 'completed'
  );

  console.log(
    `[MemberSync] COMPLETE success=${successCount} upsertErrors=${errorCount} enrichmentErrors=${enrichmentErrorCount}`
  );
}

export async function statsSyncCron(env: Env) {
  const clanId = env.BUNGIE_CLAN_ID;
  const members = await getMembersList(env.DB, clanId, true);
  
  if (!members || members.length === 0) {
    console.log('[StatsSync] No active members to queue');
    return { success: true, queued: 0 };
  }

  // Filter to members who have actually played since last processing
  const membersToProcess = members.filter((member) => {
    if (!member.last_processed_date) return true;

    const currentResolved = (member as any).last_online_status_change_resolved ?? null;
    const prevResolved = (member as any).last_online_status_change_resolved_prev ?? null;

    if (currentResolved !== null && prevResolved !== null) {
      return currentResolved !== prevResolved;
    }

    const currentRaw = member.last_online_status_change ?? null;
    const prevRaw = (member as any).last_online_status_change_prev ?? null;
    if (prevRaw === null || prevRaw === undefined) return true;
    return String(currentRaw) !== String(prevRaw);
  });

  console.log(
    `[StatsSync] Filtered: ${members.length} total → ${membersToProcess.length} with activity`
  );

  if (membersToProcess.length === 0) {
    console.log('[StatsSync] No members with recent activity, skipping queue');
    return { success: true, queued: 0, filtered: members.length };
  }

  const runId = `run-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;

  await trackRunStart(env.RUN_TRACKING_KV, runId, 'stats', membersToProcess.length);


  // CRITICAL CHANGE: Queue messages WITHOUT waiting for them to process
  // Each queue.send() just adds to the queue and returns immediately
  const sendConcurrency = Number((env as any).QUEUE_SEND_CONCURRENCY) || 5;
  let queued = 0;

  // Send messages in parallel batches for faster queueing
  const BATCH_SIZE = 50; // Send 50 at a time
  for (let i = 0; i < membersToProcess.length; i += BATCH_SIZE) {
    const batch = membersToProcess.slice(i, i + BATCH_SIZE);
    
    await promisePool(
      batch,
      async (member) => {
        try {
          // This returns as soon as message is queued (milliseconds)
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
    
    // Log progress for large batches
    console.log(`[StatsSync] Queued ${Math.min(i + BATCH_SIZE, membersToProcess.length)}/${membersToProcess.length}`);
  }

  console.log(`[StatsSync] ✅ Queued ${queued}/${membersToProcess.length} members for run ${runId}`);

  // IMPORTANT: Return immediately - queue consumers will process in background
  return {
    success: true,
    queued,
    runId,
    totalMembers: members.length,
    filteredOut: members.length - membersToProcess.length,
    note: 'Messages queued successfully. Processing will continue in background via queue consumers.',
  };
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

// HTTP handler with minimal logging
export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    const requestId = Math.random().toString(36).slice(2, 8);

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
          stats: statsByMember.get(member.membership_id) || [],
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
          console.error(`[HTTP:${requestId}] ❌ Error fetching activity history:`, err);
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
          console.error(`[HTTP:${requestId}] ❌ Error fetching recent activities:`, err);
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
          console.error(`[HTTP:${requestId}] ❌ Error fetching PGCR:`, err);
          return jsonResponse({ error: 'Failed to fetch PGCR' }, 500, request, env);
        }
      }

      // POST /admin/refresh
      if (url.pathname === '/admin/refresh' && request.method === 'POST') {
        const body = await request.json().catch(() => ({} as any)) as any;
        const type = (body.type as string) || 'all';
        
        const results: Record<string, any> = {};
        const startTime = Date.now();

        try {
          // These functions now return immediately after queueing
          if (type === 'members' || type === 'all') {
            console.log('[Refresh] Starting member sync...');
            results.members = await memberSyncCron(env);
            console.log('[Refresh] Member sync queuing completed in', Date.now() - startTime, 'ms');
          }
          
          if (type === 'stats' || type === 'all') {
            console.log('[Refresh] Starting stats sync...');
            results.stats = await statsSyncCron(env);
            console.log('[Refresh] Stats sync queuing completed in', Date.now() - startTime, 'ms');
          }

          // In /admin/refresh endpoint, update the monitoring object
          return jsonResponse({ 
            success: true, 
            results,
            queuedAt: new Date().toISOString(),
            durationMs: Date.now() - startTime,
            message: 'All operations queued successfully. Processing will continue in background.',
            monitoring: {
              queues: 'Check Cloudflare dashboard > Queues for live metrics',
              logs: 'Use `wrangler tail` to monitor real-time processing',
              stats: results.stats?.runId 
                ? `GET /admin/run/${results.stats.runId} to check progress`
                : 'Use GET /admin/queue-stats to see latest runs'
            }
          }, 200, request, env);
          
        } catch (err: any) {
          console.error('[Refresh] Error during queueing:', err);
          return jsonResponse({ 
            error: 'Failed to queue operations', 
            message: err?.message ?? String(err),
            partialResults: results
          }, 500, request, env);
        }
      }

      // GET /admin/queue-stats
      if (url.pathname === '/admin/queue-stats' && request.method === 'GET') {
        try {
          const latest = await getLatestRuns(env.RUN_TRACKING_KV);
          
          return jsonResponse({
            latestRuns: latest,
            monitoring: {
              note: 'Runs auto-expire after 24 hours',
              dashboard: 'Cloudflare Dashboard > Queues for live queue metrics',
              logs: 'Use `wrangler tail` for real-time logs'
            }
          }, 200, request, env);
        } catch (err: any) {
          return jsonResponse({ 
            error: 'Failed to get queue stats',
            message: err?.message ?? String(err)
          }, 500, request, env);
        }
      }

      // GET /admin/run/:runId
      if (url.pathname.startsWith('/admin/run/') && request.method === 'GET') {
        const runId = url.pathname.split('/').pop();
        
        if (!runId) {
          return jsonResponse({ error: 'Missing runId' }, 400, request, env);
        }
        
        try {
          const info = await getRunInfo(env.RUN_TRACKING_KV, runId);
          
          if (!info) {
            return jsonResponse({ 
              error: 'Run not found',
              note: 'Runs expire after 24 hours'
            }, 404, request, env);
          }
          
          return jsonResponse(info, 200, request, env);
        } catch (err: any) {
          return jsonResponse({ 
            error: 'Failed to get run info',
            message: err?.message ?? String(err)
          }, 500, request, env);
        }
      }

      // POST /admin/recompute
      if (url.pathname === '/admin/recompute' && request.method === 'POST') {
        const body = await request.json().catch(() => ({} as any)) as any;
        const clanId = String(body.clanId ?? env.BUNGIE_CLAN_ID);
        await (await import('./db/aggregateHelpers')).recomputeClanAggregateStats(env.DB, clanId);
        return jsonResponse({ success: true, clanId }, 200, request, env);
      }

      // 404 - Not Found
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
    console.log(`\n[Queue] Received batch with ${batch.messages.length} message(s)`);
    const batchId = Math.random().toString(36).slice(2, 8);

    let processedCount = 0;
    let errorCount = 0;

    for (let i = 0; i < batch.messages.length; i++) {
      const message = batch.messages[i];
      const body = message.body;

      try {
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
          message.ack();
        }
      } catch (err) {
        errorCount++;
        console.error(`[Queue:${batchId}] Message ${i + 1} failed:`, err);
        try { message.retry(); } catch {}
      }
    }

    // minimal summary
    console.log(`[Queue:${batchId}] processed=${processedCount} errors=${errorCount}`);
  },

  // Better pattern matching
  async scheduled(event: any, env: Env): Promise<void> {
    console.log('[Cron] Triggered:', event.cron);
    const cronString = event.cron || '';
    
    // Check for stats sync first (more specific pattern)
    if (cronString.includes('0 */6') || cronString.includes('0 */1')) {
      console.log('[Cron] Running stats sync');
      await statsSyncCron(env);
    } 
    // Then check for member sync
    else if (cronString.includes('*/30')) {
      console.log('[Cron] Running member sync');
      await memberSyncCron(env);
    }
    else {
      console.warn('[Cron] Unknown cron pattern:', cronString);
    }
  },
};