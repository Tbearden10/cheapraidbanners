// Enhanced PGCR endpoint with activity definition data
// Returns a cleaned up response with only essential player and activity info

/**
 * Fetch and enrich PGCR data with activity definitions
 * @param {string} instanceId - The activity instance ID
 * @param {Object} env - Environment with BUNGIE_API_KEY
 * @returns {Promise<Object>} Enriched PGCR data
 */
export async function fetchEnrichedPGCR(instanceId, env) {
  if (!instanceId) {
    throw new Error('Missing instanceId');
  }

  // Fetch the raw PGCR data
  const pgcrUrl = `https://www.bungie.net/Platform/Destiny2/Stats/PostGameCarnageReport/${instanceId}/`;
  
  const pgcrResponse = await fetch(pgcrUrl, {
    headers: {
      'X-API-Key': env.BUNGIE_API_KEY,
      'Accept': 'application/json'
    }
  });

  if (!pgcrResponse.ok) {
    throw new Error(`Failed to fetch PGCR: ${pgcrResponse.status}`);
  }

  const pgcrData = await pgcrResponse.json();
  
  if (pgcrData.ErrorCode !== 1) {
    throw new Error(pgcrData.Message || 'Bungie API error');
  }

  const response = pgcrData.Response;
  
  // Extract activity hash for definition lookup
  const activityHash = response.activityDetails?.referenceId;
  
  if (!activityHash) {
    throw new Error('No activity hash found in PGCR');
  }

  // Fetch activity definition
  const activityDef = await fetchActivityDefinition(activityHash, env);

  // Collect all unique hashes we need to look up
  const classHashes = new Set();
  const emblemHashes = new Set();

  response.entries?.forEach(entry => {
    if (entry.player?.classHash) classHashes.add(entry.player.classHash);
    if (entry.player?.emblemHash) emblemHashes.add(entry.player.emblemHash);
  });

  // Fetch all definitions in parallel
  const [classDefs, emblemDefs] = await Promise.all([
    fetchManifestDefinitions('DestinyClassDefinition', Array.from(classHashes), env),
    fetchManifestDefinitions('DestinyInventoryItemDefinition', Array.from(emblemHashes), env)
  ]);

  // Build cleaned up response
  return {
    instanceId,
    period: response.period,
    activityDurationSeconds: response.activityDetails?.activityDurationSeconds || 
      response.entries?.[0]?.values?.activityDurationSeconds?.basic?.value || 0,
    
    // Activity information from definition
    activity: {
      hash: activityHash,
      name: activityDef?.displayProperties?.name || 'Unknown Activity',
      description: activityDef?.displayProperties?.description || '',
      icon: activityDef?.displayProperties?.icon 
        ? `https://www.bungie.net${activityDef.displayProperties.icon}` 
        : null,
      pgcrImage: activityDef?.pgcrImage 
        ? `https://www.bungie.net${activityDef.pgcrImage}` 
        : null,
      activityTypeHash: activityDef?.activityTypeHash,
      destinationHash: activityDef?.destinationHash,
      placeHash: activityDef?.placeHash
    },

    // Cleaned up player entries
    players: response.entries?.map(entry => ({
      // Player identity
      membershipId: entry.player?.destinyUserInfo?.membershipId,
      membershipType: entry.player?.destinyUserInfo?.membershipType,
      displayName: entry.player?.destinyUserInfo?.displayName,
      bungieGlobalDisplayName: entry.player?.destinyUserInfo?.bungieGlobalDisplayName,
      bungieGlobalDisplayNameCode: entry.player?.destinyUserInfo?.bungieGlobalDisplayNameCode,
      iconPath: entry.player?.destinyUserInfo?.iconPath 
        ? `https://www.bungie.net${entry.player.destinyUserInfo.iconPath}` 
        : null,
      
      // Character info
      characterId: entry.characterId,
      lightLevel: entry.player?.lightLevel,
      characterLevel: entry.player?.characterLevel,
      
      // Class with definition
      class: {
        hash: entry.player?.classHash,
        name: classDefs[entry.player?.classHash]?.displayProperties?.name || entry.player?.characterClass
      },
      
      emblem: {
        hash: entry.player?.emblemHash,
        name: emblemDefs[entry.player?.emblemHash]?.displayProperties?.name || 'Unknown',
        icon: emblemDefs[entry.player?.emblemHash]?.displayProperties?.icon
          ? `https://www.bungie.net${emblemDefs[entry.player.emblemHash].displayProperties.icon}`
          : null
      },

      // Activity performance
      completed: entry.values?.completed?.basic?.value === 1,
      timePlayedSeconds: entry.values?.timePlayedSeconds?.basic?.value || 0,
      startSeconds: entry.values?.startSeconds?.basic?.value || 0,
      
      // Combat stats (for detailed modal view)
      kills: entry.values?.kills?.basic?.value || 0,
      deaths: entry.values?.deaths?.basic?.value || 0,
      assists: entry.values?.assists?.basic?.value || 0,
      killsDeathsRatio: entry.values?.killsDeathsRatio?.basic?.value || 0,
      
      // Extended stats
      precisionKills: entry.extended?.values?.precisionKills?.basic?.value || 0,
      grenadeKills: entry.extended?.values?.weaponKillsGrenade?.basic?.value || 0,
      meleeKills: entry.extended?.values?.weaponKillsMelee?.basic?.value || 0,
      superKills: entry.extended?.values?.weaponKillsSuper?.basic?.value || 0,
      
      // Weapons used (top 3)
      weapons: entry.extended?.weapons?.slice(0, 3).map(weapon => ({
        referenceId: weapon.referenceId,
        kills: weapon.values?.uniqueWeaponKills?.basic?.value || 0,
        precisionKills: weapon.values?.uniqueWeaponPrecisionKills?.basic?.value || 0
      })) || []
    })) || []
  };
}

/**
 * Fetch activity definition from Bungie API
 */
async function fetchActivityDefinition(activityHash, env) {
  const url = `https://www.bungie.net/Platform/Destiny2/Manifest/DestinyActivityDefinition/${activityHash}`;
  
  const response = await fetch(url, {
    headers: {
      'X-API-Key': env.BUNGIE_API_KEY,
      'Accept': 'application/json'
    }
  });

  if (!response.ok) {
    console.warn(`Failed to fetch activity definition for ${activityHash}`);
    return null;
  }

  const data = await response.json();
  
  if (data.ErrorCode !== 1) {
    console.warn(`Bungie API error for activity definition: ${data.Message}`);
    return null;
  }

  return data.Response;
}

/**
 * Fetch multiple manifest definitions of a given type
 */
async function fetchManifestDefinitions(type, hashes, env) {
  if (!hashes || hashes.length === 0) {
    return {};
  }

  const results = {};

  // Fetch definitions in parallel (with reasonable concurrency)
  const chunkSize = 5;
  for (let i = 0; i < hashes.length; i += chunkSize) {
    const chunk = hashes.slice(i, i + chunkSize);
    
    const chunkResults = await Promise.all(
      chunk.map(async (hash) => {
        try {
          const url = `https://www.bungie.net/Platform/Destiny2/Manifest/${type}/${hash}`;
          
          const response = await fetch(url, {
            headers: {
              'X-API-Key': env.BUNGIE_API_KEY,
              'Accept': 'application/json'
            }
          });

          if (!response.ok) {
            console.warn(`Failed to fetch ${type} for hash ${hash}`);
            return [hash, null];
          }

          const data = await response.json();
          
          if (data.ErrorCode !== 1) {
            console.warn(`Bungie API error for ${type} ${hash}: ${data.Message}`);
            return [hash, null];
          }

          return [hash, data.Response];
        } catch (error) {
          console.error(`Error fetching ${type} for hash ${hash}:`, error);
          return [hash, null];
        }
      })
    );

    chunkResults.forEach(([hash, definition]) => {
      if (definition) {
        results[hash] = definition;
      }
    });

    // Rate limiting between chunks
    if (i + chunkSize < hashes.length) {
      await sleep(100);
    }
  }

  return results;
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}
