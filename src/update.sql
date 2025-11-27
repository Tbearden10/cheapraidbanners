-- Migration: add columns required by smart member sync
-- Run with: wrangler d1 execute clan-stats --file=schema-add-columns.sql


-- Add unchanged_online_count to clan_members for smart skipping (if missing)
ALTER TABLE clan_members ADD COLUMN unchanged_online_count INTEGER NOT NULL DEFAULT 0;