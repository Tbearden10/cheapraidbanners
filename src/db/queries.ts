// Database queries for clan app (TypeScript)
// Note: signatures accept nullable strings for optional DB columns to avoid "string | null" errors.

import type {
  D1Database,
  ClanMemberRow,
  MemberDungeonStatsRow,
} from '../types';

/**
 * Get list of clan members
 */
export async function getMembersList(
  db: D1Database,
  clanId: string,
  activeOnly = false
): Promise<ClanMemberRow[]> {
  const query = activeOnly
    ? `SELECT * FROM clan_members WHERE clan_id = ? AND is_active = 1 ORDER BY display_name`
    : `SELECT * FROM clan_members WHERE clan_id = ? ORDER BY display_name`;

  const result = await db.prepare(query).bind(clanId).all();
  return result.results || [];
}

/**
 * Upsert clan member
 *
 * NOTE: joinDate, emblemPath, emblemBackgroundPath accept string | null
 * to align with the possibility of missing values returned from the Bungie API.
 */
export async function upsertClanMember(db: D1Database, member: {
  clanId: string;
  membershipId: string;
  membershipType: number;
  displayName: string;
  isOnline: boolean;
  lastOnlineStatusChange?: number | null;
  joinDate?: string | null;
  emblemPath?: string | null;
  emblemBackgroundPath?: string | null;
  isActive: boolean;
}) {
  await db.prepare(`
    INSERT INTO clan_members (
      clan_id, membership_id, membership_type, display_name,
      is_online, last_online_status_change, join_date,
      emblem_path, emblem_background_path, is_active,
      created_at, updated_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(clan_id, membership_id)
    DO UPDATE SET
      display_name = excluded.display_name,
      is_online = excluded.is_online,
      last_online_status_change = excluded.last_online_status_change,
      emblem_path = excluded.emblem_path,
      emblem_background_path = excluded.emblem_background_path,
      is_active = excluded.is_active,
      updated_at = excluded.updated_at
  `).bind(
    member.clanId,
    member.membershipId,
    member.membershipType,
    member.displayName,
    member.isOnline ? 1 : 0,
    member.lastOnlineStatusChange ?? null,
    member.joinDate ?? null,
    member.emblemPath ?? null,
    member.emblemBackgroundPath ?? null,
    member.isActive ? 1 : 0,
    Date.now(),
    Date.now()
  ).run();
}

/**
 * Get member stats (all dungeons)
 */
export async function getMemberStats(
  db: D1Database,
  clanId: string,
  membershipId: string
): Promise<MemberDungeonStatsRow[]> {
  const result = await db.prepare(`
    SELECT * FROM member_dungeon_stats
    WHERE clan_id = ? AND membership_id = ?
    ORDER BY dungeon_hash
  `).bind(clanId, membershipId).all();

  return result.results || [];
}

/**
 * Upsert member dungeon stats
 * lastProcessedDate may be null
 */
// Defensive DB bindings to avoid D1_TYPE_ERROR from undefined values

/**
 * Upsert member dungeon stats (defensive)
 */
export async function upsertMemberDungeonStats(db: D1Database, stats: {
  clanId: string;
  membershipId: string;
  membershipType: number;
  dungeonHash: string;
  totalFullClears: number;
  totalPlaytimeSeconds: number;
  lastProcessedDate?: string | null;
}) {
  // Validate and coerce values to avoid binding `undefined` into D1
  const clanId = String(stats.clanId ?? '');
  const membershipId = String(stats.membershipId ?? '');
  const membershipType = Number.isNaN(Number(stats.membershipType)) ? 0 : Number(stats.membershipType);
  const dungeonHash = String(stats.dungeonHash ?? '');
  const totalFullClears = Number.isFinite(Number(stats.totalFullClears)) ? Number(stats.totalFullClears) : 0;
  const totalPlaytimeSeconds = Number.isFinite(Number(stats.totalPlaytimeSeconds)) ? Number(stats.totalPlaytimeSeconds) : 0;
  const lastProcessedDate = stats.lastProcessedDate ?? null;

  await db.prepare(`
    INSERT INTO member_dungeon_stats (
      clan_id, membership_id, membership_type, dungeon_hash,
      total_full_clears, total_playtime_seconds, last_processed_date,
      created_at, updated_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(clan_id, membership_id, dungeon_hash)
    DO UPDATE SET
      total_full_clears = excluded.total_full_clears,
      total_playtime_seconds = excluded.total_playtime_seconds,
      last_processed_date = excluded.last_processed_date,
      updated_at = excluded.updated_at
  `).bind(
    clanId,
    membershipId,
    membershipType,
    dungeonHash,
    totalFullClears,
    totalPlaytimeSeconds,
    lastProcessedDate,
    Date.now(),
    Date.now()
  ).run();
}

/**
 * Get clan aggregate stats (all dungeons + overall)
 */
export async function getClanAggregateStats(
  db: D1Database,
  clanId: string
): Promise<any[]> {
  const result = await db.prepare(`
    SELECT * FROM clan_aggregate_stats
    WHERE clan_id = ?
    ORDER BY dungeon_hash
  `).bind(clanId).all();

  return result.results || [];
}

/**
 * Recompute clan aggregate stats (sum all member stats)
 */
export async function recomputeClanAggregateStats(
  db: D1Database,
  clanId: string
) {
  // Get all dungeon hashes
  const dungeonHashesResult = await db.prepare(`
    SELECT DISTINCT dungeon_hash FROM member_dungeon_stats
    WHERE clan_id = ?
  `).bind(clanId).all();

  const dungeonHashes = (dungeonHashesResult.results || []).map((r: any) => r.dungeon_hash);

  // Compute aggregate for each dungeon
  for (const dungeonHash of dungeonHashes) {
    const result = await db.prepare(`
      SELECT 
        SUM(total_full_clears) as total_full_clears,
        SUM(total_playtime_seconds) as total_playtime_seconds,
        COUNT(DISTINCT membership_id) as active_member_count
      FROM member_dungeon_stats
      WHERE clan_id = ? AND dungeon_hash = ?
    `).bind(clanId, dungeonHash).first();

    await db.prepare(`
      INSERT INTO clan_aggregate_stats (
        clan_id, dungeon_hash, total_full_clears, 
        total_playtime_seconds, active_member_count, last_updated
      ) VALUES (?, ?, ?, ?, ?, ?)
      ON CONFLICT(clan_id, dungeon_hash)
      DO UPDATE SET
        total_full_clears = excluded.total_full_clears,
        total_playtime_seconds = excluded.total_playtime_seconds,
        active_member_count = excluded.active_member_count,
        last_updated = excluded.last_updated
    `).bind(
      clanId,
      dungeonHash,
      result?.total_full_clears || 0,
      result?.total_playtime_seconds || 0,
      result?.active_member_count || 0,
      Date.now()
    ).run();
  }

  // Compute overall aggregate (sum of all dungeons)
  const overallResult = await db.prepare(`
    SELECT 
      SUM(total_full_clears) as total_full_clears,
      SUM(total_playtime_seconds) as total_playtime_seconds,
      COUNT(DISTINCT membership_id) as active_member_count
    FROM member_dungeon_stats
    WHERE clan_id = ?
  `).bind(clanId).first();

  await db.prepare(`
    INSERT INTO clan_aggregate_stats (
      clan_id, dungeon_hash, total_full_clears, 
      total_playtime_seconds, active_member_count, last_updated
    ) VALUES (?, ?, ?, ?, ?, ?)
    ON CONFLICT(clan_id, dungeon_hash)
    DO UPDATE SET
      total_full_clears = excluded.total_full_clears,
      total_playtime_seconds = excluded.total_playtime_seconds,
      active_member_count = excluded.active_member_count,
      last_updated = excluded.last_updated
  `).bind(
    clanId,
    'all',
    overallResult?.total_full_clears || 0,
    overallResult?.total_playtime_seconds || 0,
    overallResult?.active_member_count || 0,
    Date.now()
  ).run();

  console.log(`[DB] ✅ Recomputed clan aggregates for ${dungeonHashes.length} dungeons + overall`);
}