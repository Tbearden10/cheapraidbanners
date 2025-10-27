// File: lib/bungieApi.js
// Enhanced with tracking of most recent clan activity with 2+ members and additional logs

export const ACTIVITY_REFERENCE_MAP = {
  2032534090: { name: 'Shattered Throne', isProphecy: false },
  1375089621: { name: 'Pit of Heresy', isProphecy: false },
  2582501063: { name: 'Pit of Heresy', isProphecy: false },
  1077850348: { name: 'Prophecy', isProphecy: true },
  4148187374: { name: 'Prophecy', isProphecy: true },
  4078656646: { name: 'Grasp of Avarice', isProphecy: false },
  2823159265: { name: 'Duality', isProphecy: false },
  1262462921: { name: 'Spire of the Watcher', isProphecy: false },
  1668217731: { name: 'Ghosts of the Deep', isProphecy: false },
  1042180643: { name: 'Warlord\'s Ruin', isProphecy: false },
  442850873: { name: 'Vesper\'s Host', isProphecy: false }
};

const DUNGEON_HASHES = Object.keys(ACTIVITY_REFERENCE_MAP).map(Number);

export async function computeClearsForMembers(members, env, options = {}) {
  const {
    concurrency = 2,
    pageSize = 250,
    maxPages = 50,
    includeDeletedCharacters = false,
    fetchAllActivitiesForCharacter = false
  } = options;

  console.log(`[computeClearsForMembers] Starting computation for ${members.length} members...`);

  let totalClears = 0;
  let totalProphecyClears = 0;
  const perMember = [];
  let processedCount = 0;

  const allClanActivities = new Map();
  const memberActivityMap = new Map();

  for (let i = 0; i < members.length; i += concurrency) {
    const batch = members.slice(i, i + concurrency);
    console.log(`[computeClearsForMembers] Processing batch ${i / concurrency + 1} of ${Math.ceil(members.length / concurrency)}...`);

    const results = await Promise.all(
      batch.map(m => computeClearsForMember(m, env, {
        pageSize,
        maxPages,
        includeDeletedCharacters,
        fetchAllActivitiesForCharacter,
        trackAllActivities: true
      }))
    );

    results.forEach((result, idx) => {
      const member = batch[idx];
      console.log(`[computeClearsForMembers] Processed member ${member.membershipId}, Clears: ${result.clears}, Prophecy Clears: ${result.prophecyClears}`);

      totalClears += result.clears;
      totalProphecyClears += result.prophecyClears;
      processedCount++;

      perMember.push({
        membershipId: member.membershipId,
        membershipType: member.membershipType,
        clears: result.clears,
        prophecyClears: result.prophecyClears,
        mostRecentActivity: result.mostRecentActivity
      });

      if (result.allActivities) {
        result.allActivities.forEach(activity => {
          const instanceId = activity.instanceId;

          if (!allClanActivities.has(instanceId)) {
            allClanActivities.set(instanceId, {
              period: activity.period,
              instanceId: activity.instanceId,
              activityHash: activity.activityHash,
              completed: activity.completed
            });
          }

          if (!memberActivityMap.has(instanceId)) {
            memberActivityMap.set(instanceId, new Set());
          }
          memberActivityMap.get(instanceId).add(member.membershipId);
        });
      }
    });

    if (i + concurrency < members.length) {
      console.log(`[computeClearsForMembers] Sleeping between batches...`);
      await sleep(300);
    }
  }

  const mostRecentClanActivity = findMostRecentClanActivity(allClanActivities, memberActivityMap);

  console.log(`[computeClearsForMembers] Completed processing. Total Clears: ${totalClears}, Total Prophecy Clears: ${totalProphecyClears}`);
  return {
    clears: totalClears,
    prophecyClears: totalProphecyClears,
    memberCount: members.length,
    processedCount,
    mostRecentClanActivity,
    perMember,
    fetchedAt: new Date().toISOString()
  };
}

function findMostRecentClanActivity(allClanActivities, memberActivityMap) {
  let mostRecent = null;
  let mostRecentDate = null;

  for (const [instanceId, activity] of allClanActivities.entries()) {
    const clanMemberCount = memberActivityMap.get(instanceId)?.size || 0;

    if (clanMemberCount < 2 || !activity.completed) {
      continue;
    }

    const activityDate = new Date(activity.period);

    if (!mostRecentDate || activityDate > mostRecentDate) {
      mostRecentDate = activityDate;
      mostRecent = {
        period: activity.period,
        instanceId: activity.instanceId,
        activityHash: activity.activityHash,
        clanMemberCount,
        membershipIds: Array.from(memberActivityMap.get(instanceId))
      };
    }
  }

  console.log(`[findMostRecentClanActivity] Most recent activity:`, mostRecent);
  return mostRecent;
}

async function fetchCharactersForMember(membershipId, membershipType, env, includeDeleted = false) {
  try {
    console.log(`[fetchCharactersForMember] Fetching characters for member ${membershipId}...`);
    const url = `https://www.bungie.net/Platform/Destiny2/${membershipType}/Profile/${membershipId}/?components=100`;

    const response = await fetch(url, {
      headers: {
        'X-API-Key': env.BUNGIE_API_KEY,
        'Accept': 'application/json'
      }
    });

    if (!response.ok) {
      console.warn(`[fetchCharactersForMember] Failed to fetch characters for ${membershipId}: ${response.status}`);
      return [];
    }

    const data = await response.json();
    const charactersData = data.Response?.profile?.data?.characterIds || [];

    console.log(`[fetchCharactersForMember] Found ${charactersData.length} characters for member ${membershipId}`);
    return charactersData.map(charId => ({
      characterId: charId,
      membershipId,
      membershipType
    }));
  } catch (error) {
    console.error(`[fetchCharactersForMember] Error fetching characters for ${membershipId}:`, error);
    return [];
  }
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}