// Simplified aggregate helpers with CLEAR delta logic
// Key: Apply deltas correctly, always use cumulative values

import type { D1Database } from '../types';

/**
 * Apply incremental deltas to clan aggregates
 * 
 * IMPORTANT: All delta values are INCREMENTAL (what to ADD)
 * The DB stores CUMULATIVE totals
 * 
 * @param clearsDelta - New clears to ADD (from this processing run)
 * @param fullClearsDelta - New full clears to ADD (from this processing run)
 * @param playtimeDelta - New playtime to ADD (from this processing run)
 * @param isNewRow - True if member+dungeon combo is new (increment member count)
 */
export async function applyClanAggregateDelta(
  db: D1Database,
  clanId: string,
  dungeonHash: string,
  clearsDelta: number,
  fullClearsDelta: number,
  playtimeDelta: number,
  isNewRow: boolean
): Promise<void> {
  const now = Date.now();

  if (isNewRow) {
    // New member for this dungeon - add deltas AND increment member count
    await db.prepare(`
      INSERT INTO clan_aggregate_stats (
        clan_id, dungeon_hash, 
        total_clears, total_full_clears, total_playtime_seconds,
        active_member_count, last_updated
      ) VALUES (?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(clan_id, dungeon_hash) DO UPDATE SET
        total_clears = clan_aggregate_stats.total_clears + excluded.total_clears,
        total_full_clears = clan_aggregate_stats.total_full_clears + excluded.total_full_clears,
        total_playtime_seconds = clan_aggregate_stats.total_playtime_seconds + excluded.total_playtime_seconds,
        active_member_count = clan_aggregate_stats.active_member_count + 1,
        last_updated = excluded.last_updated
    `).bind(
      clanId,
      dungeonHash,
      clearsDelta,
      fullClearsDelta,
      playtimeDelta,
      1, // Member count delta
      now
    ).run();

  } else {
    // Existing member - just add deltas
    await db.prepare(`
      INSERT INTO clan_aggregate_stats (
        clan_id, dungeon_hash,
        total_clears, total_full_clears, total_playtime_seconds,
        active_member_count, last_updated
      ) VALUES (?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(clan_id, dungeon_hash) DO UPDATE SET
        total_clears = clan_aggregate_stats.total_clears + excluded.total_clears,
        total_full_clears = clan_aggregate_stats.total_full_clears + excluded.total_full_clears,
        total_playtime_seconds = clan_aggregate_stats.total_playtime_seconds + excluded.total_playtime_seconds,
        last_updated = excluded.last_updated
    `).bind(
      clanId,
      dungeonHash,
      clearsDelta,
      fullClearsDelta,
      playtimeDelta,
      0, // Don't change member count
      now
    ).run();
  }
}

/**
 * Full recompute - sum all member stats and replace aggregates
 * Call this periodically (daily/weekly) to fix any drift
 * IMPORTANT: Only includes stats from ACTIVE clan members
 */
export async function recomputeClanAggregateStats(
  db: D1Database,
  clanId: string
): Promise<void> {
  console.log(`[Aggregate] Recomputing clan stats for ${clanId}...`);

  // Sum all member stats by dungeon, filtering by active members only
  const rows = await db.prepare(`
    SELECT 
      mds.dungeon_hash,
      COALESCE(SUM(mds.total_clears), 0) AS total_clears,
      COALESCE(SUM(mds.total_full_clears), 0) AS total_full_clears,
      COALESCE(SUM(mds.total_playtime_seconds), 0) AS total_playtime_seconds,
      COUNT(DISTINCT mds.membership_id) AS active_member_count
    FROM member_dungeon_stats mds
    INNER JOIN clan_members cm 
      ON mds.clan_id = cm.clan_id 
      AND mds.membership_id = cm.membership_id
    WHERE mds.clan_id = ? AND cm.is_active = 1
    GROUP BY mds.dungeon_hash
  `).bind(clanId).all();

  const aggregated = rows.results || [];
  const now = Date.now();

  // Replace each dungeon's aggregate
  for (const row of aggregated) {
    await db.prepare(`
      INSERT INTO clan_aggregate_stats (
        clan_id, dungeon_hash,
        total_clears, total_full_clears, total_playtime_seconds,
        active_member_count, last_updated
      ) VALUES (?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(clan_id, dungeon_hash) DO UPDATE SET
        total_clears = excluded.total_clears,
        total_full_clears = excluded.total_full_clears,
        total_playtime_seconds = excluded.total_playtime_seconds,
        active_member_count = excluded.active_member_count,
        last_updated = excluded.last_updated
    `).bind(
      clanId,
      (row as any).dungeon_hash,
      Number((row as any).total_clears || 0),
      Number((row as any).total_full_clears || 0),
      Number((row as any).total_playtime_seconds || 0),
      Number((row as any).active_member_count || 0),
      now
    ).run();
  }

  // Clean up any stale aggregates
  const dungeonHashes = aggregated.map((r: any) => r.dungeon_hash);
  if (dungeonHashes.length > 0) {
    const placeholders = dungeonHashes.map(() => '?').join(',');
    await db.prepare(`
      DELETE FROM clan_aggregate_stats
      WHERE clan_id = ? AND dungeon_hash NOT IN (${placeholders})
    `).bind(clanId, ...dungeonHashes).run();
  }

  console.log(`[Aggregate] Recomputed ${aggregated.length} dungeons for clan ${clanId}`);
}