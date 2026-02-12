// Database queries for clan members and dungeon stats

import type {
  D1Database,
  ClanMemberRow,
  MemberDungeonStatsRow,
} from '../types';
import { resolveLastOnlineStatusChangeToMs } from '../utils/lastOnlineResolver';

/**
 * Get list of clan members with activity tracking fields
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
 * Upsert clan member with activity tracking
 * NOTE: Now tracks last_online_status_change_prev and resolved timestamps to detect activity
 */
export async function upsertClanMember(db: D1Database, member: {
  clanId: string;
  membershipId: string;
  membershipType: number;
  displayName: string;
  isOnline: boolean;
  lastOnlineStatusChange?: number | string | null;
  joinDate?: string | null;
  emblemPath?: string | null;
  emblemBackgroundPath?: string | null;
  isActive: boolean;
}) {
  // compute resolved timestamp
  const rawVal = member.lastOnlineStatusChange ?? null;
  const resolved = resolveLastOnlineStatusChangeToMs(rawVal);

  await db.prepare(`
    INSERT INTO clan_members (
      clan_id, membership_id, membership_type, display_name,
      is_online, last_online_status_change, last_online_status_change_prev,
      last_online_status_change_resolved, last_online_status_change_resolved_prev,
      join_date, emblem_path, emblem_background_path, is_active,
      created_at, updated_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(clan_id, membership_id)
    DO UPDATE SET
      display_name = excluded.display_name,
      is_online = excluded.is_online,
      last_online_status_change_prev = clan_members.last_online_status_change,
      last_online_status_change = excluded.last_online_status_change,
      last_online_status_change_resolved_prev = clan_members.last_online_status_change_resolved,
      last_online_status_change_resolved = excluded.last_online_status_change_resolved,
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
    rawVal ?? null,
    null, // prev raw will be set by DO UPDATE clause from existing value
    resolved,
    null, // prev resolved will be set by DO UPDATE clause from existing value
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
 * Upsert member dungeon stats with dual clear tracking
 * IMPORTANT: Now accepts both totalClears and totalFullClears and persists last_processed_instance_id
 */
export async function upsertMemberDungeonStats(db: D1Database, stats: {
  clanId: string;
  membershipId: string;
  membershipType: number;
  dungeonHash: string;
  totalClears?: number; // optional: all completions
  totalFullClears: number; // verified full clears
  totalPlaytimeSeconds: number;
  lastProcessedDate?: string | null;
  lastProcessedInstanceId?: string | null;
}) {
  // Validate and coerce values
  const clanId = String(stats.clanId ?? '');
  const membershipId = String(stats.membershipId ?? '');
  const membershipType = Number.isNaN(Number(stats.membershipType))
    ? 0
    : Number(stats.membershipType);
  const dungeonHash = String(stats.dungeonHash ?? '');
  
  // If totalClears not provided, use totalFullClears as fallback (backward compat)
  const totalClears = Number.isFinite(Number(stats.totalClears))
    ? Number(stats.totalClears)
    : Number(stats.totalFullClears);
  
  const totalFullClears = Number.isFinite(Number(stats.totalFullClears))
    ? Number(stats.totalFullClears)
    : 0;
  
  const totalPlaytimeSeconds = Number.isFinite(Number(stats.totalPlaytimeSeconds))
    ? Number(stats.totalPlaytimeSeconds)
    : 0;
  
  const lastProcessedDate = stats.lastProcessedDate ?? null;
  const lastProcessedInstanceId = stats.lastProcessedInstanceId ?? null;

  await db.prepare(`
    INSERT INTO member_dungeon_stats (
      clan_id, membership_id, membership_type, dungeon_hash,
      total_clears, total_full_clears, total_playtime_seconds, last_processed_date, last_processed_instance_id,
      created_at, updated_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(clan_id, membership_id, dungeon_hash)
    DO UPDATE SET
      total_clears = excluded.total_clears,
      total_full_clears = excluded.total_full_clears,
      total_playtime_seconds = excluded.total_playtime_seconds,
      last_processed_date = excluded.last_processed_date,
      last_processed_instance_id = excluded.last_processed_instance_id,
      updated_at = excluded.updated_at
  `).bind(
    clanId,
    membershipId,
    membershipType,
    dungeonHash,
    totalClears,
    totalFullClears,
    totalPlaytimeSeconds,
    lastProcessedDate,
    lastProcessedInstanceId,
    Date.now(),
    Date.now()
  ).run();
}

/**
 * Get clan aggregate stats by directly summing member stats
 * Always accurate, no drift possible
 */
export async function getClanAggregateStats(
  db: D1Database,
  clanId: string
): Promise<{
  perDungeon: Array<{
    dungeonHash: string;
    totalClears: number;
    totalFullClears: number;
    totalPlaytimeSeconds: number;
    activeMemberCount: number;
  }>;
  overall: {
    totalClears: number;
    totalFullClears: number;
    totalPlaytimeSeconds: number;
    activeMemberCount: number;
  };
}> {
  // Get per-dungeon stats
  const perDungeonResult = await db.prepare(`
    SELECT 
      mds.dungeon_hash,
      COALESCE(SUM(mds.total_clears), 0) as total_clears,
      COALESCE(SUM(mds.total_full_clears), 0) as total_full_clears,
      COALESCE(SUM(mds.total_playtime_seconds), 0) as total_playtime_seconds,
      COUNT(DISTINCT mds.membership_id) as active_member_count
    FROM member_dungeon_stats mds
    INNER JOIN clan_members cm 
      ON mds.clan_id = cm.clan_id 
      AND mds.membership_id = cm.membership_id
    WHERE mds.clan_id = ? AND cm.is_active = 1
    GROUP BY mds.dungeon_hash
  `).bind(clanId).all();

  const perDungeon = (perDungeonResult.results || []).map((row: any) => ({
    dungeonHash: row.dungeon_hash,
    totalClears: Number(row.total_clears || 0),
    totalFullClears: Number(row.total_full_clears || 0),
    totalPlaytimeSeconds: Number(row.total_playtime_seconds || 0),
    activeMemberCount: Number(row.active_member_count || 0),
  }));

  // Get overall stats (across all dungeons)
  const overallResult = await db.prepare(`
    SELECT 
      COALESCE(SUM(mds.total_clears), 0) as total_clears,
      COALESCE(SUM(mds.total_full_clears), 0) as total_full_clears,
      COALESCE(SUM(mds.total_playtime_seconds), 0) as total_playtime_seconds,
      COUNT(DISTINCT mds.membership_id) as active_member_count
    FROM member_dungeon_stats mds
    INNER JOIN clan_members cm 
      ON mds.clan_id = cm.clan_id 
      AND mds.membership_id = cm.membership_id
    WHERE mds.clan_id = ? AND cm.is_active = 1
  `).bind(clanId).first();

  const overall = {
    totalClears: Number((overallResult as any)?.total_clears || 0),
    totalFullClears: Number((overallResult as any)?.total_full_clears || 0),
    totalPlaytimeSeconds: Number((overallResult as any)?.total_playtime_seconds || 0),
    activeMemberCount: Number((overallResult as any)?.active_member_count || 0),
  };

  return { perDungeon, overall };
}