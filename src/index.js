// File: index.js
// Main Cloudflare Worker - Handles HTTP requests and cron triggers

export { StatsCoordinator } from './stats_coordinator.js';
import { fetchEnrichedPGCR } from './enhanced_pgcr.js';

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const path = url.pathname;

    const corsHeaders = {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type',
    };

    if (request.method === 'OPTIONS') {
      return new Response(null, { headers: corsHeaders });
    }

    try {
      // GET /members - Returns cached member list
      if (path === '/members' && request.method === 'GET') {
        const members = await env.BUNGIE_STATS.get('members', 'json');
        
        if (!members) {
          await triggerMembersRefresh(env);
          return jsonResponse({ 
            members: [], 
            fetchedAt: null, 
            loading: true 
          }, 202, corsHeaders);
        }

        return jsonResponse(members, 200, corsHeaders);
      }

      // GET /stats - Returns cached stats snapshot
      if (path === '/stats' && request.method === 'GET') {
        const stats = await env.BUNGIE_STATS.get('stats_snapshot', 'json');
        
        if (!stats) {
          await triggerStatsRefresh(env);
          return jsonResponse({ 
            raidClears: 0, 
            dungeonClears: 0, 
            totalPlaytimeSeconds: 0,
            memberCount: 0,
            fetchedAt: null,
            loading: true 
          }, 202, corsHeaders);
        }

        return jsonResponse(stats, 200, corsHeaders);
      }

      // GET /recent-clears - Fetch recent activities for clan members
      if (path === '/recent-clears' && request.method === 'GET') {
        try {
          const count = parseInt(url.searchParams.get('count')) || 30;
          const maxCount = Math.min(count, 30);


          console.log('[COUNT]:')
          
          const recentClears = await fetchRecentClanClears(maxCount, env);
          return jsonResponse(recentClears, 200, corsHeaders);
        } catch (error) {
          console.error('[RecentClears] Fetch error:', error);
          return jsonResponse({ 
            error: error.message || 'Failed to fetch recent clears',
            clears: []
          }, 500, corsHeaders);
        }
      }

      // GET /pgcr - Get enhanced activity details
      if (path === '/pgcr' && request.method === 'GET') {
        const instanceId = url.searchParams.get('instanceId');
        if (!instanceId) {
          return jsonResponse({ error: 'Missing instanceId' }, 400, corsHeaders);
        }

        try {
          const enrichedPGCR = await fetchEnrichedPGCRWithCache(instanceId, env);
          return jsonResponse(enrichedPGCR, 200, corsHeaders);
        } catch (error) {
          console.error('[PGCR] Fetch error:', error);
          return jsonResponse({ 
            error: error.message || 'Failed to fetch PGCR' 
          }, 500, corsHeaders);
        }
      }

      // POST /admin/refresh - Manual refresh trigger
      if (path === '/admin/refresh' && request.method === 'POST') {
        const token = request.headers.get('x-admin-token');
        if (!env.ADMIN_TOKEN || token !== env.ADMIN_TOKEN) {
          return jsonResponse({ error: 'Unauthorized' }, 401, corsHeaders);
        }

        const body = await request.json().catch(() => ({}));
        const type = body.type || 'all';

        if (type === 'members' || type === 'all') {
          await triggerMembersRefresh(env);
        }
        if (type === 'stats' || type === 'all') {
          await triggerStatsRefresh(env);
        }

        return jsonResponse({ 
          ok: true, 
          message: `Refresh triggered for: ${type}` 
        }, 200, corsHeaders);
      }

      return new Response('Not Found', { status: 404, headers: corsHeaders });

    } catch (error) {
      console.error('Worker error:', error);
      return jsonResponse({ 
        error: error.message || 'Internal server error' 
      }, 500, corsHeaders);
    }
  },

  // Cron - runs every 5 minutes
  async scheduled(event, env, ctx) {
    console.log('[Cron] Triggered at:', new Date().toISOString());
    
    try {
      // Always refresh members (lightweight, tracks online status)
      await triggerMembersRefresh(env);
      
      // Get current members to check online status
      const members = await env.BUNGIE_STATS.get('members', 'json');
      const hasOnlineMembers = members?.members?.some(m => m.isOnline) || false;
      const onlineCount = members?.members?.filter(m => m.isOnline).length || 0;
      
      // Get last stats update time
      const stats = await env.BUNGIE_STATS.get('stats_snapshot', 'json');
      const lastUpdate = stats?.fetchedAt ? new Date(stats.fetchedAt).getTime() : 0;
      const timeSinceUpdate = Date.now() - lastUpdate;
      
      // Adaptive polling based on activity:
      // - Online members: Update every 5 minutes
      // - No online members: Update every 2 hours (to catch recently completed activities)
      const updateInterval = hasOnlineMembers ? 5 * 60 * 1000 : 2 * 60 * 60 * 1000;
      const shouldUpdate = timeSinceUpdate > updateInterval;
      
      if (shouldUpdate) {
        console.log(`[Cron] Triggering stats refresh (${onlineCount} online, last update: ${Math.floor(timeSinceUpdate / 60000)}m ago)`);
        await triggerStatsRefresh(env);
      } else {
        console.log(`[Cron] Skipping stats refresh (${onlineCount} online, last update: ${Math.floor(timeSinceUpdate / 60000)}m ago, next in: ${Math.floor((updateInterval - timeSinceUpdate) / 60000)}m)`);
      }
      
    } catch (error) {
      console.error('[Cron] Error:', error);
    }
  }
};

// Helper: Trigger members refresh
async function triggerMembersRefresh(env) {
  const id = env.STATS_COORDINATOR.idFromName('coordinator');
  const stub = env.STATS_COORDINATOR.get(id);
  
  try {
    await stub.fetch('https://internal/refresh-members', { method: 'POST' });
  } catch (err) {
    console.error('[triggerMembersRefresh] Failed:', err);
  }
}

// Helper: Trigger stats refresh
async function triggerStatsRefresh(env) {
  const id = env.STATS_COORDINATOR.idFromName('coordinator');
  const stub = env.STATS_COORDINATOR.get(id);
  
  try {
    await stub.fetch('https://internal/refresh-stats', { method: 'POST' });
  } catch (err) {
    console.error('[triggerStatsRefresh] Failed:', err);
  }
}

// Helper: Fetch recent clan clears - one activity per member from their most recent character
async function fetchRecentClanClears(maxCount, env) {
  console.log('[RecentClears] Fetching the most recent activity for each member...');

  // Get the cached members list
  const members = await env.BUNGIE_STATS.get('members', 'json');
  if (!members || !members.members || members.members.length === 0) {
    console.log('[RecentClears] No members available to fetch activities.');
    return { clears: [], fetchedAt: new Date().toISOString() };
  }

  const recentActivities = await Promise.all(
    members.members.map(async (member) => {
      try {
        // Step 1: Fetch all characters for the member
        const charactersUrl = `https://www.bungie.net/Platform/Destiny2/${member.membershipType}/Profile/${member.membershipId}/?components=200`;
        const response = await fetch(charactersUrl, {
          headers: { 'X-API-Key': env.BUNGIE_API_KEY },
        });

        if (!response.ok) {
          console.warn(`[RecentClears] Failed to fetch characters for member ${member.membershipId}. Status: ${response.status}`);
          return null;
        }

        const data = await response.json();
        const characters = data.Response?.characters?.data;
        if (!characters || Object.keys(characters).length === 0) {
          console.warn(`[RecentClears] No characters found for member ${member.membershipId}`);
          return null;
        }

        // Step 2: Determine the most recent character
        let mostRecentCharacter = null;
        let latestDate = 0;
        for (const charId in characters) {
          const char = characters[charId];
          const lastPlayed = new Date(char.dateLastPlayed).getTime();
          if (lastPlayed > latestDate) {
            latestDate = lastPlayed;
            mostRecentCharacter = char;
          }
        }

        if (!mostRecentCharacter) {
          console.warn(`[RecentClears] Could not determine the most recent character for member ${member.membershipId}`);
          return null;
        }

        // Step 3: Fetch the most recent activity for the most recent character
        const activityUrl = `https://www.bungie.net/Platform/Destiny2/${member.membershipType}/Account/${member.membershipId}/Character/${mostRecentCharacter.characterId}/Stats/Activities/?count=1`;
        const activityResponse = await fetch(activityUrl, {
          headers: { 'X-API-Key': env.BUNGIE_API_KEY },
        });

        if (!activityResponse.ok) {
          console.warn(`[RecentClears] Failed to fetch recent activity for member ${member.membershipId}, character ${mostRecentCharacter.characterId}. Status: ${activityResponse.status}`);
          return null;
        }

        const activityData = await activityResponse.json();
        const activity = activityData.Response?.activities?.[0];
        if (!activity) {
          console.warn(`[RecentClears] No recent activity found for member ${member.membershipId}, character ${mostRecentCharacter.characterId}`);
          return null;
        }

        // Return the most recent activity
        return {
          membershipId: member.membershipId,
          membershipType: member.membershipType,
          characterId: mostRecentCharacter.characterId,
          instanceId: activity.activityDetails.instanceId,
          activityHash: activity.activityDetails.referenceId,
          period: activity.period,
          completed: activity.values?.completionReason?.basic?.value === 0,
          activityDurationSeconds: activity.values?.activityDurationSeconds?.basic?.value,
        };
      } catch (error) {
        console.error(`[RecentClears] Error fetching activity for member ${member.membershipId}:`, error);
        return null;
      }
    })
  );

  // Filter out null values and limit to maxCount
  const filteredActivities = recentActivities.filter((activity) => activity).slice(0, maxCount);

  console.log(`[RecentClears] Successfully fetched ${filteredActivities.length} activities.`);
  return {
    clears: filteredActivities,
    count: filteredActivities.length,
    fetchedAt: new Date().toISOString(),
  };
}

// Helper: JSON response
function jsonResponse(data, status = 200, additionalHeaders = {}) {
  return new Response(JSON.stringify(data), {
    status,
    headers: {
      'Content-Type': 'application/json',
      ...additionalHeaders
    }
  });
}