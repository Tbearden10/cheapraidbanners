-- Full D1 schema for members + characters + aggregates (fresh setup)
-- If you can start fresh, run this SQL in the D1 SQL editor to create the correct schema.

CREATE TABLE IF NOT EXISTS members (
  id TEXT PRIMARY KEY,                         -- membershipId (string)
  display_name TEXT,
  supplemental_display_name TEXT,
  membership_type INTEGER,
  membership_type_label INTEGER,
  join_date TEXT,
  emblem_path TEXT,
  emblem_background_path TEXT,
  emblem_hash INTEGER,
  character_ids TEXT,                          -- JSON array stored as text
  fetched_at TEXT,                             -- ISO string when this member was last fetched
  is_online INTEGER DEFAULT 0,                 -- 0/1
  known_clears INTEGER DEFAULT 0,
  last_updated TEXT,
  version TEXT
);

CREATE TABLE IF NOT EXISTS characters (
  id TEXT PRIMARY KEY,
  member_id TEXT,
  last_sync_cursor TEXT,
  last_sync_at TEXT,
  FOREIGN KEY(member_id) REFERENCES members(id)
);

CREATE TABLE IF NOT EXISTS user_aggregates (
  member_id TEXT PRIMARY KEY,
  total_clears INTEGER DEFAULT 0,
  last_checked_at TEXT
);