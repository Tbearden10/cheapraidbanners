-- schema.sql - Database schema for Clan Stats App
-- Run with: wrangler d1 execute clan-stats --file=schema.sql

-- Table: clan_members
-- Tracks all clan members (active and departed)
CREATE TABLE IF NOT EXISTS clan_members (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  clan_id TEXT NOT NULL,
  membership_id TEXT NOT NULL,
  membership_type INTEGER NOT NULL,
  display_name TEXT NOT NULL,
  is_online INTEGER NOT NULL DEFAULT 0,
  last_online_status_change INTEGER,
  join_date TEXT,
  emblem_path TEXT,
  emblem_background_path TEXT,
  is_active INTEGER NOT NULL DEFAULT 1,
  created_at INTEGER NOT NULL DEFAULT (unixepoch()),
  updated_at INTEGER NOT NULL DEFAULT (unixepoch()),
  
  UNIQUE(clan_id, membership_id)
);

-- Index for fast lookups
CREATE INDEX IF NOT EXISTS idx_clan_members_clan 
ON clan_members(clan_id);

CREATE INDEX IF NOT EXISTS idx_clan_members_active 
ON clan_members(clan_id, is_active);

-- Table: member_dungeon_stats
-- Stores per-member, per-dungeon stats
-- This is the core data table (total playtime + total full clears)
CREATE TABLE IF NOT EXISTS member_dungeon_stats (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  clan_id TEXT NOT NULL,
  membership_id TEXT NOT NULL,
  membership_type INTEGER NOT NULL,
  dungeon_hash TEXT NOT NULL,
  total_full_clears INTEGER NOT NULL DEFAULT 0,
  total_playtime_seconds INTEGER NOT NULL DEFAULT 0,
  last_processed_date TEXT,
  created_at INTEGER NOT NULL DEFAULT (unixepoch()),
  updated_at INTEGER NOT NULL DEFAULT (unixepoch()),
  
  UNIQUE(clan_id, membership_id, dungeon_hash)
);

-- Index for fast lookups
CREATE INDEX IF NOT EXISTS idx_member_dungeon_stats_clan 
ON member_dungeon_stats(clan_id);

CREATE INDEX IF NOT EXISTS idx_member_dungeon_stats_member 
ON member_dungeon_stats(clan_id, membership_id);

CREATE INDEX IF NOT EXISTS idx_member_dungeon_stats_dungeon 
ON member_dungeon_stats(clan_id, dungeon_hash);

-- Table: clan_aggregate_stats
-- Stores summed stats across all members
-- One row per dungeon + one row for overall (dungeon_hash = 'all')
CREATE TABLE IF NOT EXISTS clan_aggregate_stats (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  clan_id TEXT NOT NULL,
  dungeon_hash TEXT NOT NULL, -- 'all' for overall, or specific dungeon hash
  total_full_clears INTEGER NOT NULL DEFAULT 0,
  total_playtime_seconds INTEGER NOT NULL DEFAULT 0,
  active_member_count INTEGER NOT NULL DEFAULT 0,
  last_updated INTEGER NOT NULL DEFAULT (unixepoch()),
  
  UNIQUE(clan_id, dungeon_hash)
);

-- Index for fast lookups
CREATE INDEX IF NOT EXISTS idx_clan_aggregate_stats_clan 
ON clan_aggregate_stats(clan_id);