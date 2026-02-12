-- Single-schema file for a brand-new DB (idempotent CREATEs)
-- Run with:
--   npx wrangler d1 execute clan-stats --file=./src/schema.sql
-- Assumes the DB does not exist yet; safe to run against an empty DB.

-- Table: clan_members
CREATE TABLE IF NOT EXISTS clan_members (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  clan_id TEXT NOT NULL,
  membership_id TEXT NOT NULL,
  membership_type INTEGER NOT NULL,
  display_name TEXT NOT NULL,
  is_online INTEGER NOT NULL DEFAULT 0,
  last_online_status_change INTEGER,
  last_online_status_change_prev INTEGER,
  -- Resolved canonical timestamps (milliseconds) for robust comparison
  last_online_status_change_resolved INTEGER,
  last_online_status_change_resolved_prev INTEGER,
  join_date TEXT,
  emblem_path TEXT,
  emblem_background_path TEXT,
  is_active INTEGER NOT NULL DEFAULT 1,
  -- New column used by smart sync logic to track unchanged online count
  unchanged_online_count INTEGER NOT NULL DEFAULT 0,
  created_at INTEGER NOT NULL DEFAULT (unixepoch()),
  updated_at INTEGER NOT NULL DEFAULT (unixepoch()),
  UNIQUE(clan_id, membership_id)
);

CREATE INDEX IF NOT EXISTS idx_clan_members_clan
  ON clan_members(clan_id);

CREATE INDEX IF NOT EXISTS idx_clan_members_active
  ON clan_members(clan_id, is_active);

-- Table: member_dungeon_stats
-- Tracks both all completions (total_clears) and verified full clears (total_full_clears)
CREATE TABLE IF NOT EXISTS member_dungeon_stats (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  clan_id TEXT NOT NULL,
  membership_id TEXT NOT NULL,
  membership_type INTEGER NOT NULL,
  dungeon_hash TEXT NOT NULL,
  total_clears INTEGER NOT NULL DEFAULT 0,
  total_full_clears INTEGER NOT NULL DEFAULT 0,
  total_playtime_seconds INTEGER NOT NULL DEFAULT 0,
  last_processed_date TEXT,                -- ISO string of last processed activity
  last_processed_instance_id TEXT,         -- deterministic cutoff marker
  created_at INTEGER NOT NULL DEFAULT (unixepoch()),
  updated_at INTEGER NOT NULL DEFAULT (unixepoch()),
  UNIQUE(clan_id, membership_id, dungeon_hash)
);

CREATE INDEX IF NOT EXISTS idx_member_dungeon_stats_clan
  ON member_dungeon_stats(clan_id);

CREATE INDEX IF NOT EXISTS idx_member_dungeon_stats_member
  ON member_dungeon_stats(clan_id, membership_id);

CREATE INDEX IF NOT EXISTS idx_member_dungeon_stats_dungeon
  ON member_dungeon_stats(clan_id, dungeon_hash);
