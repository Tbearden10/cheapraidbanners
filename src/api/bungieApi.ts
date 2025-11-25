// Bungie API client functions with safe timeout handling and retries.
// Ensures AbortController timers are cleaned up after each attempt.
//
// NOTE: Cloudflare Workers does not expose process.env, so we use sensible defaults here.
// If you want to override these per-deploy, pass values from Env into callers or add a small config wrapper.

const DEFAULT_FETCH_TIMEOUT_MS = 12000; // matches wrangler.toml BUNGIE_PER_REQUEST_TIMEOUT_MS
const DEFAULT_FETCH_RETRIES = 2;        // matches wrangler.toml BUNGIE_FETCH_RETRIES

function createTimeoutSignal(timeoutMs: number) {
  const controller = new AbortController();
  const id = setTimeout(() => controller.abort(), timeoutMs);
  const signal = controller.signal;
  const cleanup = () => clearTimeout(id);
  return { signal, cleanup };
}

/**
 * Fetch with retry logic (exponential backoff).
 * Uses a per-attempt AbortController which is cleaned up after each fetch attempt.
 */
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

      // cleanup timer
      cleanup();

      if (response.ok) {
        return response;
      }

      // Retry on 429 or 5xx
      if (response.status === 429 || (response.status >= 500 && response.status < 600)) {
        lastError = new Error(`HTTP ${response.status}`);
      } else {
        // Non-retriable: return the response to allow caller to handle non-OK
        return response;
      }
    } catch (error) {
      cleanup();
      lastError = error instanceof Error ? error : new Error(String(error));
    }

    // Wait before retry (exponential backoff)
    if (attempt < retries) {
      const waitTime = Math.pow(2, attempt) * 500 + Math.random() * 200;
      await new Promise((resolve) => setTimeout(resolve, waitTime));
    }
  }

  throw lastError || new Error('Fetch failed after retries');
}

/** --- API helpers below: unchanged behavior but use fetchWithRetry --- */

export async function fetchClanRoster(clanId: string, apiKey: string) {
  const url = `https://www.bungie.net/Platform/GroupV2/${clanId}/Members/`;

  const response = await fetchWithRetry(url, {
    headers: { 'X-API-Key': apiKey, Accept: 'application/json' },
  });

  const data = await response.json();
  const results = data.Response?.results || [];

  return results.map((r: any) => ({
    membershipId: r.destinyUserInfo?.membershipId,
    membershipType: r.destinyUserInfo?.membershipType,
    displayName: r.destinyUserInfo?.displayName,
    isOnline: Boolean(r.isOnline || false),
    lastOnlineStatusChange: r.lastOnlineStatusChange ? Number(r.lastOnlineStatusChange) * 1000 : null,
    joinDate: r.joinDate,
  }));
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