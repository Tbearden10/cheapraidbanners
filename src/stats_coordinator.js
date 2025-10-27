// File: stats_coordinator.js
// Parallel processing - all members start at once with Promise.allSettled
// Uses mode=82 (dungeon) + mode=18 (story) filters to catch all dungeon activities
// Some dungeons appear in story mode due to legacy reasons

import { ACTIVITY_REFERENCE_MAP } from './activityReferenceMap.js';

// Build a Set of ALL dungeon reference IDs for filtering story activities
const ALL_DUNGEON_REFERENCE_IDS = new Set(
  ACTIVITY_REFERENCE_MAP.flatMap(d => d.referenceIds)
);

export class StatsCoordinator {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.refreshing = { members: false, stats: false };
  }

  async fetch(request) {
    const url = new URL(request.url);
    const path = url.pathname;

    try {
      if (path === '/refresh-members' && request.method === 'POST') {
        return await this.refreshMembers();
      }

      if (path === '/refresh-stats' && request.method === 'POST') {
        return await this.refreshStats();
      }

      return new Response('Not Found', { status: 404 });
    } catch (error) {
      console.error('StatsCoordinator error:', error);
      return new Response(JSON.stringify({ error: error.message }), {
        status: 500,
        headers: { 'Content-Type': 'application/json' }
      });
    }
  }

  dataHasChanged(newData, oldData) {
    if (!oldData && newData) return true;
    if (!newData) return false;
    
    try {
      // For members: check count, online status, and basic equality
      if (newData.members && oldData.members) {
        if (newData.members.length !== oldData.members.length) {
          console.log('[dataHasChanged] Member count changed:', oldData.members.length, '->', newData.members.length);
          return true;
        }
        
        const newOnline = newData.members.filter(m => m.isOnline).length;
        const oldOnline = oldData.members.filter(m => m.isOnline).length;
        if (newOnline !== oldOnline) {
          console.log('[dataHasChanged] Online member count changed:', oldOnline, '->', newOnline);
          return true;
        }
        
        // Check if any member's online status changed
        const onlineStatusChanged = newData.members.some((newMember, idx) => {
          const oldMember = oldData.members.find(m => m.membershipId === newMember.membershipId);
          return oldMember && oldMember.isOnline !== newMember.isOnline;
        });
        
        if (onlineStatusChanged) {
          console.log('[dataHasChanged] Individual member online status changed');
          return true;
        }
        
        return false; // Members haven't changed meaningfully
      }
      
      // For stats: check specific numeric values
      if (typeof newData.raidClears !== 'undefined' && typeof oldData.raidClears !== 'undefined') {
        if (newData.raidClears !== oldData.raidClears) {
          console.log('[dataHasChanged] Raid clears changed:', oldData.raidClears, '->', newData.raidClears);
          return true;
        }
        if (newData.dungeonClears !== oldData.dungeonClears) {
          console.log('[dataHasChanged] Dungeon clears changed:', oldData.dungeonClears, '->', newData.dungeonClears);
          return true;
        }
        if (newData.totalPlaytimeSeconds !== oldData.totalPlaytimeSeconds) {
          console.log('[dataHasChanged] Playtime changed:', oldData.totalPlaytimeSeconds, '->', newData.totalPlaytimeSeconds);
          return true;
        }
        
        // Check if recent activities changed
        const newActivityIds = new Set(newData.recentClanActivities?.map(a => a.instanceId) || []);
        const oldActivityIds = new Set(oldData.recentClanActivities?.map(a => a.instanceId) || []);
        
        if (newActivityIds.size !== oldActivityIds.size) {
          console.log('[dataHasChanged] Recent activities count changed:', oldActivityIds.size, '->', newActivityIds.size);
          return true;
        }
        
        // Check if any new activities appeared
        for (const id of newActivityIds) {
          if (!oldActivityIds.has(id)) {
            console.log('[dataHasChanged] New activity detected:', id);
            return true;
          }
        }
        
        return false; // Stats haven't changed
      }
      
      // Fallback: deep equality check
      const changed = JSON.stringify(newData) !== JSON.stringify(oldData);
      if (changed) {
        console.log('[dataHasChanged] Data structure changed (deep check)');
      }
      return changed;
      
    } catch (e) {
      console.warn('[dataHasChanged] Comparison error:', e);
      return true; // Assume changed on error
    }
  }

  async refreshMembers() {
    if (this.refreshing.members) {
      return new Response(JSON.stringify({ ok: true, status: 'already_refreshing' }), {
        headers: { 'Content-Type': 'application/json' }
      });
    }

    this.refreshing.members = true;
    console.log('[Members] Starting refresh...');

    try {
      const roster = await this.fetchClanRoster();
      const members = await this.enrichMembersWithEmblems(roster);
      
      const snapshot = {
        members,
        memberCount: members.length,
        fetchedAt: new Date().toISOString()
      };

      const cached = await this.env.BUNGIE_STATS.get('members', 'json');
      
      if (this.dataHasChanged(snapshot, cached)) {
        await this.env.BUNGIE_STATS.put('members', JSON.stringify(snapshot));
        console.log('[Members] Data changed, wrote to KV:', members.length, 'members');
      } else {
        console.log('[Members] No changes detected, skipping KV write');
      }
      
      return new Response(JSON.stringify({ 
        ok: true, 
        memberCount: members.length,
        changed: this.dataHasChanged(snapshot, cached)
      }), {
        headers: { 'Content-Type': 'application/json' }
      });

    } catch (error) {
      console.error('[Members] Refresh failed:', error);
      throw error;
    } finally {
      this.refreshing.members = false;
    }
  }

  // Parallel stats processing - counts clears only
  async refreshStats() {
    if (this.refreshing.stats) {
      return new Response(JSON.stringify({ ok: true, status: 'already_refreshing' }), {
        headers: { 'Content-Type': 'application/json' }
      });
    }

    this.refreshing.stats = true;
    console.log('[Stats] ═══════════════════════════════════');
    console.log('[Stats] Starting PARALLEL refresh...');

    try {
      const memberData = await this.env.BUNGIE_STATS.get('members', 'json');
      const members = memberData?.members || [];
      
      if (members.length === 0) {
        console.log('[Stats] No members to process');
        return new Response(JSON.stringify({ ok: true, processed: 0 }), {
          headers: { 'Content-Type': 'application/json' }
        });
      }

      console.log(`[Stats] 📊 Total members: ${members.length}`);
      console.log(`[Stats] 🟢 Online: ${members.filter(m => m.isOnline).length}`);
      console.log(`[Stats] ⚫ Offline: ${members.filter(m => !m.isOnline).length}`);
      console.log('[Stats] ─────────────────────────────────');
      console.log('[Stats] 🚀 Launching all member jobs NOW...');

      // Process ALL members at once with Promise.allSettled
      const results = await Promise.allSettled(
        members.map((member, idx) => {
          console.log(`[Stats] Job ${idx + 1}/${members.length}: Starting ${member.displayName || member.membershipId} (${member.isOnline ? 'ONLINE' : 'offline'})`);
          return this.processMemberActivities(member, idx + 1, members.length);
        })
      );

      // Aggregate results
      let totalDungeonClears = 0;
      let totalRaidClears = 0;
      let totalPlaytimeSeconds = 0;
      const perMember = [];
      let successCount = 0;
      let failCount = 0;
      
      
      // Track all COMPLETED activities for finding recent clan clears
      const activityByType = new Map(); // activityHash -> {period, instanceId, activityHash, membershipIds, completedBy}

      console.log('[Stats] ─────────────────────────────────');
      console.log('[Stats] 📈 Processing results...');

      results.forEach((result, idx) => {
        const member = members[idx];

        if (result.status === 'fulfilled') {
          const data = result.value;
          successCount++;

          totalRaidClears += data.raidClears;
          totalDungeonClears += data.dungeonClears;
          totalPlaytimeSeconds += data.totalPlaytimeSeconds;

          perMember.push({
            membershipId: member.membershipId,
            membershipType: member.membershipType,
            raidClears: data.raidClears,
            dungeonClears: data.dungeonClears,
            totalPlaytimeSeconds: data.totalPlaytimeSeconds
          });

          console.log(`[Stats] ✅ Job ${idx + 1}: ${member.displayName || member.membershipId} → ${data.charactersProcessed} chars`);

          // Track activities for finding the most recent one of each type
          if (data.recentActivities && data.recentActivities.length > 0) {
            data.recentActivities.forEach(activity => {
              const activityHash = activity.activityHash;
              const period = new Date(activity.period);
              const instanceId = activity.instanceId;

              if (!activityByType.has(activityHash)) {
                // If this activity type isn't recorded yet, add it
                activityByType.set(activityHash, {
                  period,
                  instanceId,
                  activityHash,
                  membershipIds: new Set([member.membershipId]),
                  completedBy: new Set(activity.completed ? [member.membershipId] : [])
                });
              } else {
                const existingActivity = activityByType.get(activityHash);
                // Update only if the new activity is more recent
                if (period > existingActivity.period) {
                  existingActivity.period = period;
                  existingActivity.instanceId = instanceId;
                  existingActivity.membershipIds = new Set([member.membershipId]); // Reset participants
                  existingActivity.completedBy = new Set(activity.completed ? [member.membershipId] : []); // Reset completedBy
                } else {
                  // Add member to participants and completedBy if applicable
                  existingActivity.membershipIds.add(member.membershipId);
                  if (activity.completed) {
                    existingActivity.completedBy.add(member.membershipId);
                  }
                }
              }
            });
          }
        } else {
          failCount++;
          console.error(`[Stats] ❌ Job ${idx + 1}: ${member.displayName || member.membershipId} FAILED → ${result.reason?.message || 'Unknown error'}`);
        }
      });

      // Build recent clan activities - most recent instance of each unique activity type
      // Filter: must have at least 1 clan member who completed it
       const recentClanActivities = Array.from(activityByType.values())
        .filter(data => data.completedBy.size > 0) // At least one completion
        .map(data => ({
          instanceId: data.instanceId,
          period: data.period.toISOString(),
          activityHash: data.activityHash,
          clanMemberCount: data.membershipIds.size,
          membershipIds: Array.from(data.membershipIds),
          completedByCount: data.completedBy.size
        }))
        .sort((a, b) => new Date(b.period) - new Date(a.period)) // Sort by most recent
        .slice(0, 30); // Keep top 30 most recent unique activities

      const snapshot = {
        raidClears: totalRaidClears,
        dungeonClears: totalDungeonClears,
        totalPlaytimeSeconds: totalPlaytimeSeconds,
        memberCount: members.length,
        processedCount: successCount,
        failedCount: failCount,
        recentClanActivities, // Array of recent clan activities for /recent-clears endpoint
        perMember,
        fetchedAt: new Date().toISOString(),
      };

      const cached = await this.env.BUNGIE_STATS.get('stats_snapshot', 'json');
      
      if (this.dataHasChanged(snapshot, cached)) {
        await this.env.BUNGIE_STATS.put('stats_snapshot', JSON.stringify(snapshot));
        console.log('[Stats] Data changed, wrote to KV');
      } else {
        console.log('[Stats] No changes detected, skipping KV write');
      }
      
      return new Response(JSON.stringify({ 
        ok: true, 
        raidClears: snapshot.raidClears,
        dungeonClears: snapshot.dungeonClears,
        totalPlaytimeSeconds: snapshot.totalPlaytimeSeconds,
        processed: successCount,
        failed: failCount,
        changed: this.dataHasChanged(snapshot, cached)
      }), {
        headers: { 'Content-Type': 'application/json' }
      });

    } catch (error) {
      console.error('[Stats] Refresh failed:', error);
      throw error;
    } finally {
      this.refreshing.stats = false;
    }
  }

  // Process activities for a single member - count clears AND track recent COMPLETED activities
  async processMemberActivities(member, jobNumber, totalJobs) {
    const jobId = `Job ${jobNumber}/${totalJobs}`;
    const memberName = member.displayName || member.membershipId;
    
    try {
      console.log(`[${jobId}] 🔍 Fetching characters for ${memberName}...`);
      
      // Get all characters
      const characters = await this.fetchCharactersForMember(
        member.membershipId,
        member.membershipType
      );

      if (!characters || characters.length === 0) {
        console.log(`[${jobId}] ⚠️ No characters found for ${memberName}`);
        return { 
          raidClears: 0,
          dungeonClears: 0, 
          totalPlaytimeSeconds: 0,
          charactersProcessed: 0,
          recentActivities: []
        };
      }

      console.log(`[${jobId}] 👥 ${memberName} has ${characters.length} characters`);

      let totalRaidClears = 0;
      let totalDungeonClears = 0;
      let totalPlaytimeSeconds = 0;
      const maxPages = 50;
      const recentActivitiesMap = new Map(); // Use Map to dedupe by instanceId

      // Process all characters for this member
      for (let charIdx = 0; charIdx < characters.length; charIdx++) {
        const character = characters[charIdx];
       
        let charClearCount = 0;
        
        
        // Fetch both dungeon mode AND story mode activities
        const dungeonActivities = await this.fetchCharacterActivitiesByMode(
          member.membershipType,
          member.membershipId,
          character.characterId,
          82, // Dungeon mode
          maxPages
        );

        const raidActivities = await this.fetchCharacterActivitiesByMode(
          member.membershipType,
          member.membershipId,
          character.characterId,
          4, // raid mode
          maxPages
        );
        
        const storyActivities = await this.fetchCharacterActivitiesByMode(
          member.membershipType,
          member.membershipId,
          character.characterId,
          2, // Story mode
          maxPages
        );

        // Filter story activities to only include dungeons
        const filteredStoryActivities = storyActivities.filter(activity => {
          const refId = String(activity.activityDetails?.referenceId || '');
          return ALL_DUNGEON_REFERENCE_IDS.has(refId);
        });

        console.log(`[${jobId}] 🔎 ${memberName} - Character ${charIdx + 1}: Found ${filteredStoryActivities.length} dungeon activities in story mode`);

        // Combine both sources (use Map to deduplicate by instanceId)
        const activityMap = new Map();
        
        [...dungeonActivities, ...raidActivities, ...filteredStoryActivities].forEach(activity => {
          const instanceId = activity.activityDetails?.instanceId;
          if (instanceId) {
            activityMap.set(instanceId, activity);
          }
        });

        const allCharActivities = Array.from(activityMap.values());

        // Process combined activities
        for (const activity of allCharActivities) {
          const mode = activity.activityDetails?.mode;
          const hash = Number(activity.activityDetails?.referenceId || 0);
          const instanceId = activity.activityDetails?.instanceId;
          const period = activity.period;
          const isCompleted = activity.values?.completed?.basic?.value === 1;

          // Track ALL recent activities (completed or not) for clan activity detection
          // But mark which ones were completed
          if (instanceId && period) {
            if (!recentActivitiesMap.has(instanceId)) {
              recentActivitiesMap.set(instanceId, {
                instanceId: instanceId,
                period: period,
                mode: mode,
                activityHash: hash,
                membershipId: member.membershipId,
                completed: isCompleted
              });
            } else {
              // If we see this activity again and it's completed, update the flag
              const existing = recentActivitiesMap.get(instanceId);
              if (isCompleted) {
                existing.completed = true;
              }
            }
          }

          // Count ONLY completed activities for stats
          if (isCompleted) {
            totalPlaytimeSeconds += activity.values?.timePlayedSeconds.basic.value;
            if (mode == 4) {
              totalRaidClears++;
            } 
            else if (mode == 2 || mode == 82) {
              totalDungeonClears++;
            }
            charClearCount++;
            
          }
        }

        console.log(`[${jobId}] 📊 ${memberName} - Character ${charIdx + 1}: ${charClearCount} clears`);

        // Delay between characters
        await this.sleep(150);
      }

      // Convert map to array and sort by date (most recent first)
      const recentActivities = Array.from(recentActivitiesMap.values())
        .sort((a, b) => new Date(b.period) - new Date(a.period))
        .slice(0, 50); // Keep top 50 most recent per member

      return {
        raidClears: totalRaidClears,
        dungeonClears: totalDungeonClears,
        totalPlaytimeSeconds: totalPlaytimeSeconds,
        charactersProcessed: characters.length,
        recentActivities // Return recent activities for aggregation
      };

    } catch (error) {
      console.error(`[${jobId}] 💥 ${memberName} ERROR:`, error.message);
      throw error;
    }
  }

  // Fetch ALL characters for a member (including deleted ones) using Stats endpoint
  async fetchCharactersForMember(membershipId, membershipType) {
    try {
      // Use the Stats endpoint which returns ALL characters including deleted
      const url = `https://www.bungie.net/Platform/Destiny2/${membershipType}/Account/${membershipId}/Stats/`;
      
      const response = await fetch(url, {
        headers: {
          'X-API-Key': this.env.BUNGIE_API_KEY,
          'Accept': 'application/json'
        }
      });

      if (!response.ok) {
        console.warn(`Failed to fetch characters for ${membershipId}: ${response.status}`);
        return [];
      }

      const data = await response.json();
      
      if (data.ErrorCode !== 1) {
        console.warn(`Bungie API error for ${membershipId}: ${data.Message}`);
        return [];
      }
      
      // The Stats endpoint returns characters as an array OR object
      let charactersArr = [];
      const characters = data.Response?.characters;
      
      if (Array.isArray(characters)) {
        // If it's an array, use it directly
        charactersArr = characters.map(char => ({
          characterId: char.characterId,
          deleted: !!char.deleted
        }));
      } else if (characters && typeof characters === 'object') {
        // If it's an object, convert to array with characterId as key
        charactersArr = Object.entries(characters).map(([characterId, char]) => ({
          characterId,
          deleted: !!(char).deleted
        }));
      }

      console.log(`[fetchCharacters] Found ${charactersArr.length} total characters for ${membershipId} (including ${charactersArr.filter(c => c.deleted).length} deleted)`);

      return charactersArr.map(char => ({
        characterId: char.characterId,
        membershipId,
        membershipType,
        deleted: char.deleted
      }));

    } catch (error) {
      console.error(`Error fetching characters for ${membershipId}:`, error);
      return [];
    }
  }

  // Fetch all activities for a character by mode (with pagination)
  async fetchCharacterActivitiesByMode(membershipType, membershipId, characterId, mode, maxPages = 50) {
    const allActivities = [];
    let page = 0;
    let hasMore = true;

    while (hasMore && page < maxPages) {
      const activities = await this.fetchActivitiesForCharacter(
        membershipType,
        membershipId,
        characterId,
        page,
        mode
      );

      if (!activities || activities.length === 0) {
        hasMore = false;
        break;
      }

      allActivities.push(...activities);

      if (activities.length < 250) {
        hasMore = false;
      } else {
        page++;
      }

      // Small delay between pages
      await this.sleep(100);
    }

    return allActivities;
  }

  // Fetch activities for a character with mode filter
  async fetchActivitiesForCharacter(membershipType, membershipId, characterId, page, mode = 82) {
    try {
      // mode=82 for dungeons, mode=4 for raids, mode=2 for story
      const url = `https://www.bungie.net/Platform/Destiny2/${membershipType}/Account/${membershipId}/Character/${characterId}/Stats/Activities/?mode=${mode}&count=250&page=${page}`;
      
      const response = await fetch(url, {
        headers: {
          'X-API-Key': this.env.BUNGIE_API_KEY,
          'Accept': 'application/json'
        }
      });

      if (!response.ok) {
        if (response.status === 404) {
          return [];
        }
        console.warn(`Failed to fetch activities (mode=${mode}) for character ${characterId}, page ${page}: ${response.status}`);
        return [];
      }

      const data = await response.json();
      return data.Response?.activities || [];

    } catch (error) {
      console.error(`Error fetching activities (mode=${mode}) for character ${characterId}, page ${page}:`, error);
      return [];
    }
  }

  // Fetch clan roster
  async fetchClanRoster() {
    const url = `https://www.bungie.net/Platform/GroupV2/${this.env.BUNGIE_CLAN_ID}/Members/`;
    
    const response = await fetch(url, {
      headers: {
        'X-API-Key': this.env.BUNGIE_API_KEY,
        'Accept': 'application/json'
      }
    });

    if (!response.ok) {
      throw new Error(`Bungie API error: ${response.status}`);
    }

    const data = await response.json();
    const results = data.Response?.results || [];

    return results.map(r => ({
      membershipId: r.destinyUserInfo?.membershipId,
      membershipType: r.destinyUserInfo?.membershipType,
      displayName: r.destinyUserInfo?.displayName,
      supplementalDisplayName: r.bungieNetUserInfo?.supplementalDisplayName,
      stat: data.dungeonClears + data.raidClears,
      isOnline: r.isOnline || false,
      joinDate: r.joinDate,
      memberType: r.memberType
    }));
  }

  // Enrich members with emblems
  async enrichMembersWithEmblems(members) {
    const enriched = [];
    const batchSize = 5; // Process 5 at a time
    
    for (let i = 0; i < members.length; i += batchSize) {
      const batch = members.slice(i, i + batchSize);
      const results = await Promise.allSettled(
        batch.map(m => this.fetchMemberEmblem(m))
      );
      
      results.forEach((result, idx) => {
        if (result.status === 'fulfilled') {
          enriched.push(result.value);
        } else {
          // If emblem fetch fails, still include member without emblem
          enriched.push({ ...batch[idx], emblemPath: null });
        }
      });
      
      if (i + batchSize < members.length) {
        await this.sleep(200);
      }
    }
    
    return enriched;
  }

  

  // Fetch emblem for member
  async fetchMemberEmblem(member) {
    try {
      const url = `https://www.bungie.net/Platform/Destiny2/${member.membershipType}/Profile/${member.membershipId}/?components=200`;
      
      const response = await fetch(url, {
        headers: {
          'X-API-Key': this.env.BUNGIE_API_KEY,
          'Accept': 'application/json'
        }
      });

      if (!response.ok) {
        return { ...member, emblemPath: null };
      }

      const data = await response.json();
      const characters = data.Response?.characters?.data || {};
      
      let mostRecent = null;
      let latestDate = 0;
      
      for (const charId in characters) {
        const char = characters[charId];
        const date = new Date(char.dateLastPlayed).getTime();
        if (date > latestDate) {
          latestDate = date;
          mostRecent = char;
        }
      }

      const emblemPath = mostRecent?.emblemPath 
        ? `https://www.bungie.net${mostRecent.emblemPath}`
        : null;

      return {
        ...member,
        emblemPath,
        emblemBackgroundPath: mostRecent?.emblemBackgroundPath 
          ? `https://www.bungie.net${mostRecent.emblemBackgroundPath}`
          : null
      };

    } catch (error) {
      console.error(`Failed to fetch emblem for ${member.membershipId}:`, error);
      return { ...member, emblemPath: null };
    }
  }

  sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }
}