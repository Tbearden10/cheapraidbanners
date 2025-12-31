// ============================================================================
// FILE: src/api/bungieApi.ts
// Clean, simple Bungie API client with basic retry logic
// ============================================================================

const sleep = (ms: number) => new Promise(r => setTimeout(r, ms));

async function fetchWithRetry(
  url: string,
  options: RequestInit = {},
  retries = 2,
  debug = false
): Promise<Response> {
  let lastError: Error | null = null;

  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      if (debug && attempt > 0) {
        console.log(`[BungieAPI] Retry attempt ${attempt}/${retries} for ${url}`);
      }
      
      const response = await fetch(url, options);

      if (response.ok) {
        return response;
      }

      // Retry on 429 or 5xx
      if (response.status === 429) {
        const retryAfter = response.headers.get('Retry-After');
        const waitMs = retryAfter ? parseInt(retryAfter) * 1000 : 2000;
        
        if (debug) {
          console.warn(`[BungieAPI] Rate limited (429), waiting ${waitMs}ms before retry`);
        }
        
        if (attempt < retries) {
          await sleep(waitMs);
          continue;
        }
      }

      if (response.status >= 500 && response.status < 600) {
        lastError = new Error(`HTTP ${response.status}`);
        
        if (debug) {
          console.warn(`[BungieAPI] Server error ${response.status}, retrying...`);
        }
        
        if (attempt < retries) {
          await sleep(1000 * (attempt + 1));
          continue;
        }
      }

      // Non-retriable error - return response
      if (debug) {
        console.warn(`[BungieAPI] Non-retriable error: ${response.status} ${response.statusText}`);
      }
      return response;
      
    } catch (error) {
      lastError = error instanceof Error ? error : new Error(String(error));
      
      if (debug) {
        console.error(`[BungieAPI] Fetch exception:`, error);
      }
      
      if (attempt < retries) {
        await sleep(500 * (attempt + 1));
        continue;
      }
    }
  }

  if (debug) {
    console.error(`[BungieAPI] All retry attempts exhausted for ${url}`);
  }
  
  throw lastError || new Error('Fetch failed after retries');
}

export async function fetchClanRoster(clanId: string, apiKey: string) {
  const url = `https://www.bungie.net/Platform/GroupV2/${clanId}/Members/`;
  const response = await fetchWithRetry(url, {
    headers: { 'X-API-Key': apiKey, Accept: 'application/json' },
  });

  const data = await response.json();
  return (data?.Response?.results || []).map((result: any) => ({
    membershipId: result.destinyUserInfo?.membershipId,
    membershipType: result.destinyUserInfo?.membershipType,
    displayName: result.destinyUserInfo?.displayName,
    bungieGlobalDisplayName: result.destinyUserInfo?.bungieGlobalDisplayName,
    bungieGlobalDisplayNameCode: result.destinyUserInfo?.bungieGlobalDisplayNameCode,
    isOnline: result.isOnline,
    lastOnlineStatusChange: result.lastOnlineStatusChange,
    joinDate: result.joinDate,
  }));
}

export async function enrichMemberWithEmblem(member: any, apiKey: string) {
  try {
    const url = `https://www.bungie.net/Platform/Destiny2/${member.membershipType}/Profile/${member.membershipId}/?components=200`;
    const response = await fetchWithRetry(url, {
      headers: { 'X-API-Key': apiKey, Accept: 'application/json' }
    });
    
    const data = await response.json();
    const characters = data.Response?.characters?.data || {};
    
    let mostRecent: any = null;
    let latestDate = 0;
    
    for (const charId in characters) {
      const char = characters[charId];
      const date = new Date(char.dateLastPlayed).getTime();
      if (!Number.isNaN(date) && date > latestDate) {
        latestDate = date;
        mostRecent = char;
      }
    }
    
    return {
      ...member,
      emblemPath: mostRecent?.emblemPath ? `https://www.bungie.net${mostRecent.emblemPath}` : null,
      emblemBackgroundPath: mostRecent?.emblemBackgroundPath ? `https://www.bungie.net${mostRecent.emblemBackgroundPath}` : null,
    };
  } catch (error) {
    console.error(`Failed to fetch emblem for ${member.membershipId}:`, error);
    return { ...member, emblemPath: null, emblemBackgroundPath: null };
  }
}

export async function fetchCharactersForMember(
  membershipId: string,
  membershipType: number,
  apiKey: string,
  debug = false
) {
  const url = `https://www.bungie.net/Platform/Destiny2/${membershipType}/Account/${membershipId}/Stats/`;
  
  if (debug) {
    console.log(`[BungieAPI] Fetching characters: membershipId=${membershipId}, membershipType=${membershipType}`);
  }
  
  const response = await fetchWithRetry(url, {
    headers: { 'X-API-Key': apiKey, Accept: 'application/json' }
  }, 2, debug);
  
  if (debug) {
    console.log(`[BungieAPI] Response status: ${response.status} ${response.statusText}`);
  }
  
  const data = await response.json();
  
  if (data.ErrorCode !== 1) {
    if (debug) {
      console.error(`[BungieAPI] Bungie API error: ErrorCode=${data.ErrorCode}, Message=${data.Message}, ErrorStatus=${data.ErrorStatus}`);
    }
    throw new Error(data.Message || 'Bungie API error');
  }
  
  let charactersArr: any[] = [];
  const characters = data.Response?.characters;
  
  if (Array.isArray(characters)) {
    charactersArr = characters.map((char: any) => ({
      characterId: char.characterId,
      deleted: !!char.deleted,
      dateLastPlayed: char.dateLastPlayed
    }));
  } else if (characters && typeof characters === 'object') {
    charactersArr = Object.entries(characters).map(([characterId, char]: [string, any]) => ({
      characterId,
      deleted: !!char.deleted,
      dateLastPlayed: char.dateLastPlayed,
    }));
  }
  
  if (debug) {
    console.log(`[BungieAPI] Found ${charactersArr.length} characters`);
  }
  
  return charactersArr.map((char) => ({
    characterId: char.characterId,
    membershipId,
    membershipType,
    deleted: char.deleted
  }));
}

export async function fetchActivitiesForCharacter(
  membershipType: number,
  membershipId: string,
  characterId: string,
  page: number,
  mode: number,
  pageSize: number,
  apiKey: string,
  debug = false
): Promise<any[]> {
  const url = `https://www.bungie.net/Platform/Destiny2/${membershipType}/Account/${membershipId}/Character/${characterId}/Stats/Activities/?mode=${mode}&count=${pageSize}&page=${page}`;
  
  try {
    if (debug) {
      console.log(`[BungieAPI] Fetching activities: char=${characterId}, mode=${mode}, page=${page}, pageSize=${pageSize}`);
    }
    
    const response = await fetchWithRetry(url, {
      headers: { 'X-API-Key': apiKey, Accept: 'application/json' }
    }, 2, debug);
    
    if (debug) {
      console.log(`[BungieAPI] Response status: ${response.status} ${response.statusText}`);
    }
    
    const data = await response.json();
    
    if (debug) {
      console.log(`[BungieAPI] Response ErrorCode: ${data.ErrorCode}, Message: ${data.Message || 'none'}`);
      console.log(`[BungieAPI] Activities count: ${data.Response?.activities?.length || 0}`);
    }
    
    // Note: We don't throw on ErrorCode !== 1 for activities because some error codes
    // are expected and should return empty array rather than throwing:
    // - ErrorCode 1601: Character's activity history is private
    // - ErrorCode 1623: Character has no activity history  
    // - Other codes: API throttling, temporary issues, etc.
    // This is intentional behavior different from fetchCharactersForMember where
    // the character list must exist or the request is fundamentally invalid
    if (data.ErrorCode !== 1) {
      if (debug) {
        console.warn(`[BungieAPI] Bungie API error: ErrorCode=${data.ErrorCode}, Message=${data.Message}, ErrorStatus=${data.ErrorStatus}`);
      }
      // Return empty array for non-fatal errors (e.g., no activities, privacy settings)
      // This is intentional behavior different from fetchCharactersForMember
      return [];
    }
    
    return data.Response?.activities || [];
  } catch (error) {
    if (debug) {
      console.error(`[BungieAPI] Exception fetching activities for character ${characterId}:`, error);
    } else {
      console.error(`Failed to fetch activities for character ${characterId}:`, error);
    }
    return [];
  }
}

export async function fetchPGCR(instanceId: string, apiKey: string) {
  const url = `https://www.bungie.net/Platform/Destiny2/Stats/PostGameCarnageReport/${instanceId}/`;
  
  try {
    const response = await fetchWithRetry(url, {
      headers: { 'X-API-Key': apiKey, Accept: 'application/json' }
    });

    const data = await response.json();
    
    if (data.ErrorCode !== 1 || !data.Response) {
      return null;
    }
    
    return data.Response;
  } catch (error) {
    // Silent failure - let caller handle
    return null;
  }
}

export async function fetchActivityDefinition(activityHash: string, apiKey: string) {
  const url = `https://www.bungie.net/Platform/Destiny2/Manifest/DestinyActivityDefinition/${activityHash}/`;
  
  try {
    const response = await fetchWithRetry(url, {
      headers: { 'X-API-Key': apiKey, Accept: 'application/json' }
    });
    
    const data = await response.json();
    
    if (data.ErrorCode !== 1 || !data.Response) {
      return null;
    }
    
    return data.Response;
  } catch (error) {
    console.error(`Failed to fetch activity definition for ${activityHash}:`, error);
    return null;
  }
}

export async function withRateLimit<T>(
  fn: () => Promise<T>,
  maxRetries = 3
): Promise<T> {
  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      return await fn();
    } catch (err: any) {
      if (attempt === maxRetries) {
        throw err;
      }
      
      await sleep(200 * Math.pow(2, attempt));
    }
  }
  
  throw new Error('withRateLimit: should not reach here');
}