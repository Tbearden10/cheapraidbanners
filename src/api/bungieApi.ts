// Bungie API client functions with reduced logging
// Keep console.error for failures only.

const DEFAULT_FETCH_TIMEOUT_MS = 12000;
const DEFAULT_FETCH_RETRIES = 2;

function createTimeoutSignal(timeoutMs: number) {
  const controller = new AbortController();
  const id = setTimeout(() => controller.abort(), timeoutMs);
  const signal = controller.signal;
  const cleanup = () => clearTimeout(id);
  return { signal, cleanup };
}

async function fetchWithRetry(
  url: string,
  options: RequestInit = {},
  retries = DEFAULT_FETCH_RETRIES,
  timeoutMs = DEFAULT_FETCH_TIMEOUT_MS
): Promise<Response> {
  let lastError: Error | null = null;

  for (let attempt = 0; attempt <= retries; attempt++) {
    const { signal, cleanup } = createTimeoutSignal(timeoutMs);
    try {
      const response = await fetch(url, {
        ...options,
        signal,
      });
      cleanup();

      if (response.ok) {
        return response;
      }

      if (response.status === 429 || (response.status >= 500 && response.status < 600)) {
        lastError = new Error(`HTTP ${response.status}`);
      } else {
        return response;
      }
    } catch (error) {
      cleanup();
      lastError = error instanceof Error ? error : new Error(String(error));
    }

    if (attempt < retries) {
      const waitTime = Math.pow(2, attempt) * 500 + Math.random() * 200;
      await new Promise((resolve) => setTimeout(resolve, waitTime));
    }
  }

  throw lastError || new Error('Fetch failed after retries');
}

export async function withRateLimit<T>(fn: () => Promise<T>, maxRetries = 3): Promise<T> {
  let attempt = 0;
  while (true) {
    try {
      return await fn();
    } catch (err: any) {
      attempt++;
      if (attempt > maxRetries) {
        throw err;
      }
      if (err && typeof err === 'object' && 'name' in err && (err as any).name === 'AbortError') {
        throw err;
      }
      const wait = Math.pow(2, attempt) * 200 + Math.random() * 100;
      await new Promise((r) => setTimeout(r, wait));
    }
  }
}

export async function fetchClanRoster(clanId: string, apiKey: string) {
  const url = `https://www.bungie.net/Platform/GroupV2/${clanId}/Members/`;

  try {
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
  } catch (err) {
    console.error(`fetchClanRoster failed for clan ${clanId}:`, err);
    return [];
  }
}

export async function enrichMemberWithEmblem(member: any, apiKey: string) {
  try {
    const url = `https://www.bungie.net/Platform/Destiny2/${member.membershipType}/Profile/${member.membershipId}/?components=200`;
    const response = await fetchWithRetry(url, { headers: { 'X-API-Key': apiKey, Accept: 'application/json' } });
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

export async function fetchCharactersForMember(membershipId: string, membershipType: number, apiKey: string) {
  const url = `https://www.bungie.net/Platform/Destiny2/${membershipType}/Account/${membershipId}/Stats/`;
  try {
    const response = await fetchWithRetry(url, { headers: { 'X-API-Key': apiKey, Accept: 'application/json' } });
    const data = await response.json();
    if (data.ErrorCode !== 1) {
      throw new Error(data.Message || 'Bungie API error');
    }
    let charactersArr: any[] = [];
    const characters = data.Response?.characters;
    if (Array.isArray(characters)) {
      charactersArr = characters.map((char: any) => ({ characterId: char.characterId, deleted: !!char.deleted, dateLastPlayed: char.dateLastPlayed }));
    } else if (characters && typeof characters === 'object') {
      charactersArr = Object.entries(characters).map(([characterId, char]: [string, any]) => ({
        characterId,
        deleted: !!(char as any).deleted,
        dateLastPlayed: (char as any).dateLastPlayed,
      }));
    }
    return charactersArr.map((char) => ({ characterId: char.characterId, membershipId, membershipType, deleted: char.deleted }));
  } catch (error) {
    console.error(`fetchCharactersForMember failed for ${membershipId}:`, error);
    return [];
  }
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
    const response = await fetchWithRetry(url, { headers: { 'X-API-Key': apiKey, Accept: 'application/json' } });
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
    const response = await fetchWithRetry(url, { headers: { 'X-API-Key': apiKey, Accept: 'application/json' } });
    const data = await response.json();
    if (data.ErrorCode !== 1 || !data.Response) return null;
    return data.Response;
  } catch (error) {
    console.error(`Failed to fetch PGCR for ${instanceId}:`, error);
    return null;
  }
}

export async function fetchActivityDefinition(activityHash: string, apiKey: string) {
  const url = `https://www.bungie.net/Platform/Destiny2/Manifest/DestinyActivityDefinition/${activityHash}/`;
  try {
    const response = await fetchWithRetry(url, { headers: { 'X-API-Key': apiKey, Accept: 'application/json' } });
    const data = await response.json();
    if (data.ErrorCode !== 1 || !data.Response) return null;
    return data.Response;
  } catch (error) {
    console.error(`Failed to fetch activity definition for ${activityHash}:`, error);
    return null;
  }
}