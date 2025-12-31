// ============================================================================
// FILE: src/api/bungieApi.ts
// Clean, simple Bungie API client with basic retry logic
// ============================================================================

const sleep = (ms: number) => new Promise(r => setTimeout(r, ms));

async function fetchWithRetry(
  url: string,
  options: RequestInit = {},
  retries = 2
): Promise<Response> {
  let lastError: Error | null = null;

  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      const response = await fetch(url, options);

      if (response.ok) {
        return response;
      }

      // Retry on 429 or 5xx
      if (response.status === 429) {
        const retryAfter = response.headers.get('Retry-After');
        const waitMs = retryAfter ? parseInt(retryAfter) * 1000 : 2000;
        
        if (attempt < retries) {
          await sleep(waitMs);
          continue;
        }
      }

      if (response.status >= 500 && response.status < 600) {
        lastError = new Error(`HTTP ${response.status}`);
        if (attempt < retries) {
          await sleep(1000 * (attempt + 1));
          continue;
        }
      }

      // Non-retriable error - return response
      return response;
      
    } catch (error) {
      lastError = error instanceof Error ? error : new Error(String(error));
      
      if (attempt < retries) {
        await sleep(500 * (attempt + 1));
        continue;
      }
    }
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
  apiKey: string
) {
  const url = `https://www.bungie.net/Platform/Destiny2/${membershipType}/Account/${membershipId}/Stats/`;
  const response = await fetchWithRetry(url, {
    headers: { 'X-API-Key': apiKey, Accept: 'application/json' }
  });
  
  const data = await response.json();
  
  if (data.ErrorCode !== 1) {
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
  apiKey: string
): Promise<any[]> {
  const url = `https://www.bungie.net/Platform/Destiny2/${membershipType}/Account/${membershipId}/Character/${characterId}/Stats/Activities/?mode=${mode}&count=${pageSize}&page=${page}`;
  
  try {
    const response = await fetchWithRetry(url, {
      headers: { 'X-API-Key': apiKey, Accept: 'application/json' }
    });
    
    const data = await response.json();
    return data.Response?.activities || [];
  } catch (error) {
    console.error(`Failed to fetch activities for character ${characterId}:`, error);
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

export async function fetchAggregateStatsForCharacter(
  membershipType: number,
  membershipId: string,
  characterId: string,
  apiKey: string
): Promise<any[]> {
  const url = `https://www.bungie.net/Platform/Destiny2/${membershipType}/Account/${membershipId}/Character/${characterId}/Stats/AggregateActivityStats/`;
  
  try {
    const response = await fetchWithRetry(url, {
      headers: { 'X-API-Key': apiKey, Accept: 'application/json' }
    });
    
    const data = await response.json();
    
    if (data.ErrorCode !== 1 || !data.Response) {
      return [];
    }
    
    return data.Response.activities || [];
  } catch (error) {
    console.error(`Failed to fetch aggregate stats for character ${characterId}:`, error);
    return [];
  }
}