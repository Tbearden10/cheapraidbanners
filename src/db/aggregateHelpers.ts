// Helpers for applying incremental deltas and full recompute of clan aggregates.
// - applyClanAggregateDelta: apply incremental deltas (positive or negative).
// - recomputeClanAggregateStats: recompute aggregates for a clan by summing member_dungeon_stats.

import type { D1Database } from '../types';

/**
 * Apply incremental deltas to clan_aggregate_stats.
 * fullClearsDelta and playtimeDelta may be negative to subtract when a member leaves.
 * wasNewMemberDungeonRow should be true when the member_dungeon_stats row did not exist before.
 */
export async function applyClanAggregateDelta(
  db: D1Database,
  clanId: string,
  dungeonHash: string,
  fullClearsDelta: number,
  playtimeDelta: number,
  wasNewMemberDungeonRow: boolean
): Promise<void> {
  const fcDelta = Number.isFinite(Number(fullClearsDelta)) ? Number(fullClearsDelta) : 0;
  const ptDelta = Number.isFinite(Number(playtimeDelta)) ? Number(playtimeDelta) : 0;
  const now = Date.now();

  if (wasNewMemberDungeonRow) {
    // Insert or add, and increment active_member_count by 1
    await db.prepare(`
      INSERT INTO clan_aggregate_stats (
        clan_id, dungeon_hash, total_full_clears, total_playtime_seconds, active_member_count, last_updated
      ) VALUES (?, ?, ?, ?, ?, ?)
      ON CONFLICT(clan_id, dungeon_hash) DO UPDATE SET
        total_full_clears = clan_aggregate_stats.total_full_clears + excluded.total_full_clears,
        total_playtime_seconds = clan_aggregate_stats.total_playtime_seconds + excluded.total_playtime_seconds,
        active_member_count = clan_aggregate_stats.active_member_count + excluded.active_member_count,
        last_updated = excluded.last_updated
    `).bind(clanId, dungeonHash, fcDelta, ptDelta, 1, now).run();
  } else {
    // Normal delta application (active_member_count unchanged)
    await db.prepare(`
      INSERT INTO clan_aggregate_stats (
        clan_id, dungeon_hash, total_full_clears, total_playtime_seconds, active_member_count, last_updated
      ) VALUES (?, ?, ?, ?, ?, ?)
      ON CONFLICT(clan_id, dungeon_hash) DO UPDATE SET
        total_full_clears = clan_aggregate_stats.total_full_clears + excluded.total_full_clears,
        total_playtime_seconds = clan_aggregate_stats.total_playtime_seconds + excluded.total_playtime_seconds,
        last_updated = excluded.last_updated
    `).bind(clanId, dungeonHash, fcDelta, ptDelta, 0, now).run();
  }
}

/**
 * Full recompute: sum member_dungeon_stats grouped by dungeon_hash for a clan,
 * then replace the clan_aggregate_stats rows for that clan with the recomputed values.
 *
 * This is intended to be run periodically (cron) to correct drift or after large membership changes.
 */
export async function recomputeClanAggregateStats(db: D1Database, clanId: string): Promise<void> {
  // Aggregate the member_dungeon_stats by dungeon_hash
  const rows = await db.prepare(`
    SELECT dungeon_hash,
           COALESCE(SUM(total_full_clears), 0) AS total_full_clears,
           COALESCE(SUM(total_playtime_seconds), 0) AS total_playtime_seconds,
           COUNT(DISTINCT membership_id) AS active_member_count
    FROM member_dungeon_stats
    WHERE clan_id = ?
    GROUP BY dungeon_hash
  `).bind(clanId).all();

  const aggregated = rows.results || [];

  // Upsert each aggregated row
  const now = Date.now();
  const queries = aggregated.map((r: any) =>
    db.prepare(`
      INSERT INTO clan_aggregate_stats (
        clan_id, dungeon_hash, total_full_clears, total_playtime_seconds, active_member_count, last_updated
      ) VALUES (?, ?, ?, ?, ?, ?)
      ON CONFLICT(clan_id, dungeon_hash) DO UPDATE SET
        total_full_clears = excluded.total_full_clears,
        total_playtime_seconds = excluded.total_playtime_seconds,
        active_member_count = excluded.active_member_count,
        last_updated = excluded.last_updated
    `).bind(
      clanId,
      r.dungeon_hash,
      Number(r.total_full_clears ?? 0),
      Number(r.total_playtime_seconds ?? 0),
      Number(r.active_member_count ?? 0),
      now
    )
  );

  if (queries.length > 0) {
    // D1Database doesn't provide a `batch` method; execute each prepared statement.
    await Promise.all(queries.map((q: any) => q.run()));
  }

  // Delete any aggregate rows for this clan that were not part of the recompute (cleanup)
  const dungeonHashes = aggregated.map((r: any) => r.dungeon_hash);
  if (dungeonHashes.length > 0) {
    const placeholders = dungeonHashes.map(() => '?').join(',');
    await db.prepare(`
      DELETE FROM clan_aggregate_stats
      WHERE clan_id = ? AND dungeon_hash NOT IN (${placeholders})
    `).bind(clanId, ...dungeonHashes).run();
  } else {
    // If no aggregated rows (no member_dungeon_stats), delete all aggregates for this clan
    await db.prepare(`
      DELETE FROM clan_aggregate_stats WHERE clan_id = ?
    `).bind(clanId).run();
  }
}