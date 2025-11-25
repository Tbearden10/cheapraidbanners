// Main Cloudflare Worker for Clan Stats (TypeScript) - updated with /recent-activities endpoint
// Expose Durable Object classes so Wrangler can find them
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

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type, x-admin-token',
};

function jsonResponse(data: unknown, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { 'Content-Type': 'application/json', ...corsHeaders },
  });
}

/** Member sync cron: update clan_members and handle left members by removing their DB rows */
async function memberSyncCron(env: Env) {
  const clanId = env.BUNGIE_CLAN_ID;
  console.log('[MemberSync] fetching roster for clan', clanId);
  const roster = (await fetchClanRoster(clanId, env.BUNGIE_API_KEY)) || [];
  const dbMembers = await getMembersList(env.DB, clanId, false); // all members in DB
  const dbMemberIds = new Set(dbMembers.map((m) => m.membership_id));
  const rosterMemberIds = new Set((roster || []).map((m: any) => m.membershipId));

  const newMembers = (roster || []).filter((m: any) => !dbMemberIds.has(m.membershipId));
  const leftMembers = dbMembers.filter((m) => !rosterMemberIds.has(m.membership_id));

  // Enrich emblems in parallel (bounded)
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

  // Upsert members (bounded)
  await promisePool(
    enriched,
    async (member: any) => {
      await upsertClanMember(env.DB, {
        clanId,
        membershipId: member.membershipId,
        membershipType: Number(member.membershipType),
        displayName: member.displayName,
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

  // For left members: remove their member_dungeon_stats rows and mark inactive
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

/** Stats sync cron: enqueue member stat jobs */
async function statsSyncCron(env: Env) {
  const clanId = env.BUNGIE_CLAN_ID;
  const members = await getMembersList(env.DB, clanId, true); // active only
  if (!members || members.length === 0) {
    console.log('[StatsSync] No active members to queue');
    return { success: true, queued: 0 };
  }

  // create a run id and init RunTracker DO for this clan run
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
    // proceed anyway
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
          runId, // include run id so worker can notify RunTracker
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

/** Helper: retrieve all member stats grouped by membership_id in a single DB query to avoid N+1 */
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
      return new Response(null, { status: 204, headers: corsHeaders });
    }

    try {
      // GET /members?active=true|false
      if (url.pathname === '/members' && request.method === 'GET') {
        const clanId = url.searchParams.get('clanId') || env.BUNGIE_CLAN_ID;
        const activeParam = url.searchParams.get('active');
        const activeOnly = activeParam === null ? true : activeParam === 'true';
        const members = await getMembersList(env.DB, clanId, activeOnly);
        return jsonResponse({ members, memberCount: members.length });
      }

      // GET /stats  -> returns aggregate stats + per-member stats
      if (url.pathname === '/stats' && request.method === 'GET') {
        const clanId = url.searchParams.get('clanId') || env.BUNGIE_CLAN_ID;

        // 1) members (active only)
        const members = await getMembersList(env.DB, clanId, true);

        // 2) all member dungeon stats in one query
        const statsByMember = await fetchAllMemberStatsGrouped(env.DB, clanId);

        // 3) assemble members with stats
        const membersWithStats = members.map((member) => ({
          membershipId: member.membership_id,
          membershipType: Number(member.membership_type),
          displayName: member.display_name,
          isOnline: Boolean(member.is_online),
          lastOnlineStatusChange: member.last_online_status_change ?? null,
          emblemPath: member.emblem_path ?? null,
          emblemBackgroundPath: member.emblem_background_path ?? null,
          stats: statsByMember.get(member.membership_id) || [],
        }));

        // 4) clan aggregate stats
        const aggregateStats = await getClanAggregateStats(env.DB, clanId);

        return jsonResponse({
          members: membersWithStats,
          aggregateStats,
          memberCount: members.length,
          fetchedAt: new Date().toISOString(),
        });
      }

      /**
       * GET /activity-history
       * - Fetches activity history directly from Bungie API for a given member/character
       * - Query params: membershipType, membershipId, characterId, mode (optional), count (optional), page (optional)
       * - No DB usage - simple proxy to Bungie activity history endpoint
       */
      if (url.pathname === '/activity-history' && request.method === 'GET') {
        const membershipType = url.searchParams.get('membershipType');
        const membershipId = url.searchParams.get('membershipId');
        const characterId = url.searchParams.get('characterId');
        const mode = url.searchParams.get('mode') || '0'; // 0 = all activities
        const count = url.searchParams.get('count') || '10';
        const page = url.searchParams.get('page') || '0';

        if (!membershipType || !membershipId || !characterId) {
          return jsonResponse({ error: 'Missing required params: membershipType, membershipId, characterId' }, 400);
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
          return jsonResponse(activities);
        } catch (err) {
          console.error('[ActivityHistory] error', err);
          return jsonResponse({ error: 'Failed to fetch activity history' }, 500);
        }
      }

      /**
       * GET /recent-activities
       * - Fetches recent activities for clan members directly from Bungie API
       * - Enriches with activity definitions to get PGCR images
       * - Returns activities sorted by period (most recent first)
       */
      if (url.pathname === '/recent-activities' && request.method === 'GET') {
        const clanId = url.searchParams.get('clanId') || env.BUNGIE_CLAN_ID;
        const MEMBERS_TO_CHECK = 5;
        const ACTIVITIES_PER_MEMBER = 3;
        const TOTAL_ACTIVITIES = 5;

        try {
          // 1) Fetch clan roster directly from Bungie API (no DB)
          const roster = await fetchClanRoster(clanId, env.BUNGIE_API_KEY);
          if (!roster || roster.length === 0) {
            return jsonResponse([]);
          }

          // 2) Sort by last online and pick top members
          const sorted = roster
            .filter((m: any) => m.membershipId && m.membershipType)
            .sort((a: any, b: any) => {
              const aTs = a.lastOnlineStatusChange || 0;
              const bTs = b.lastOnlineStatusChange || 0;
              return bTs - aTs;
            })
            .slice(0, MEMBERS_TO_CHECK);

          // 3) For each member, get their first character and fetch activities
          const allActivities: any[] = [];
          
          for (const member of sorted) {
            try {
              // Get characters for this member
              const characters = await fetchCharactersForMember(
                member.membershipId,
                member.membershipType,
                env.BUNGIE_API_KEY
              );
              
              if (!characters || characters.length === 0) continue;
              
              // Get activities for first character
              const activities = await fetchActivitiesForCharacter(
                member.membershipType,
                member.membershipId,
                characters[0].characterId,
                0, // page
                0, // mode (0 = all)
                ACTIVITIES_PER_MEMBER,
                env.BUNGIE_API_KEY
              );
              
              if (!activities || activities.length === 0) continue;
              
              // Add member info to each activity
              for (const act of activities) {
                allActivities.push({
                  ...act,
                  memberDisplayName: member.displayName,
                  membershipId: member.membershipId,
                  membershipType: member.membershipType,
                });
              }
            } catch (err) {
              console.warn('[RecentActivities] Failed to fetch for member', member.membershipId, err);
              continue;
            }
          }

          // 4) Sort all activities by period (most recent first) and limit
          const sorted_activities = allActivities
            .filter(a => a.period)
            .sort((a, b) => new Date(b.period).getTime() - new Date(a.period).getTime())
            .slice(0, TOTAL_ACTIVITIES);

          // 5) Enrich with activity definitions to get PGCR images
          // Create a map to cache activity definitions
          const activityDefCache = new Map<string, any>();
          
          const enrichedActivities = await Promise.all(
            sorted_activities.map(async (act) => {
              try {
                const activityHash = act.activityDetails?.directorActivityHash || act.activityDetails?.referenceId;
                
                if (!activityHash) {
                  console.warn('[RecentActivities] No activity hash for activity', act.activityDetails?.instanceId);
                  return act;
                }

                // Check cache first
                let activityDef = activityDefCache.get(String(activityHash));
                
                // Fetch if not cached
                if (!activityDef) {
                  activityDef = await fetchActivityDefinition(String(activityHash), env.BUNGIE_API_KEY);
                  if (activityDef) {
                    activityDefCache.set(String(activityHash), activityDef);
                  }
                }

                // Add pgcr image to activityDetails
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
                console.warn('[RecentActivities] Failed to enrich activity', act.activityDetails?.instanceId, err);
                return act;
              }
            })
          );

          return jsonResponse(enrichedActivities);
        } catch (err) {
          console.error('[RecentActivities] error', err);
          return jsonResponse({ error: 'Failed to fetch recent activities' }, 500);
        }
      }

      // POST /admin/refresh  -> body: { type: 'members'|'stats'|'all' }
      if (url.pathname === '/admin/refresh' && request.method === 'POST') {
        const token = request.headers.get('x-admin-token');
        if (!env.ADMIN_TOKEN || token !== env.ADMIN_TOKEN) {
          return jsonResponse({ error: 'Unauthorized' }, 401);
        }

        const body = await request.json().catch(() => ({} as any));
        const type = (body.type as string) || 'all';
        const results: Record<string, unknown> = {};

        if (type === 'members' || type === 'all') {
          results.members = await memberSyncCron(env);
        }
        if (type === 'stats' || type === 'all') {
          results.stats = await statsSyncCron(env);
        }

        return jsonResponse({ success: true, results });
      }

      // Admin recompute (legacy path) POST /admin/recompute
      if (url.pathname === '/admin/recompute' && request.method === 'POST') {
        const token = request.headers.get('x-admin-token');
        if (!env.ADMIN_TOKEN || token !== env.ADMIN_TOKEN) {
          return jsonResponse({ error: 'Unauthorized' }, 401);
        }
        const body = await request.json().catch(() => ({} as any));
        const clanId = String(body.clanId ?? env.BUNGIE_CLAN_ID);
        await (await import('./db/aggregateHelpers')).recomputeClanAggregateStats(env.DB, clanId);
        return jsonResponse({ success: true, clanId });
      }

      /**
       * GET /pgcr?instanceId=...
       * - Fetches Post Game Carnage Report for a specific activity instance
       * - Returns detailed player stats and activity information
       */
      if (url.pathname === '/pgcr' && request.method === 'GET') {
        const instanceId = url.searchParams.get('instanceId');
        
        if (!instanceId) {
          return jsonResponse({ error: 'Missing instanceId parameter' }, 400);
        }

        try {
          const pgcrData = await fetchPGCR(instanceId, env.BUNGIE_API_KEY);
          
          if (!pgcrData) {
            return jsonResponse({ error: 'PGCR not found' }, 404);
          }

          // Extract activity info
          const activity = pgcrData.activityDetails || {};
          const activityHash = activity.directorActivityHash || activity.referenceId;

          // Fetch activity definition for name and image
          let activityDef = null;
          if (activityHash) {
            activityDef = await fetchActivityDefinition(String(activityHash), env.BUNGIE_API_KEY);
          }

          // Process players
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
          });
        } catch (err) {
          console.error('[PGCR] error', err);
          return jsonResponse({ error: 'Failed to fetch PGCR' }, 500);
        }
      }

      return jsonResponse({ error: 'Not found' }, 404);
    } catch (err: any) {
      console.error('[Worker] Error:', err);
      return jsonResponse({ error: 'Internal server error', message: err?.message ?? String(err) }, 500);
    }
  },

  // Queue handler: process batch of queued member jobs
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

          // Notify RunTracker if present (best-effort; don't fail the job if notify fails)
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
              console.warn('[Queue] Failed to notify RunTracker for', job.membershipId, err);
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

  // Scheduled: invoke crons
  async scheduled(event: any, env: Env) {
    console.log('[Cron] Triggered:', event.cron);
    try {
      if (typeof event.cron === 'string' && event.cron.includes('*/30')) {
        await memberSyncCron(env);
      } else if (typeof event.cron === 'string' && event.cron.includes('*/6')) {
        await statsSyncCron(env);
      }
    } catch (err) {
      console.error('[Cron] Error:', err);
    }
  },
};