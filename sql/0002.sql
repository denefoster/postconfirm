-- Migration: Consolidate static tables into main tables
-- This removes senders_static and stash_static, using source column to differentiate

-- Add source column to stash if not exists
ALTER TABLE stash ADD COLUMN IF NOT EXISTS source VARCHAR(64) DEFAULT 'postconfirm';

-- Migrate senders_static → senders
INSERT INTO senders (sender, action, ref, source, type, created, updated)
SELECT sender, action, ref, source, type, created, updated FROM senders_static
ON CONFLICT (sender) DO NOTHING;

-- Migrate stash_static → stash
INSERT INTO stash (sender, recipients, message, created, source)
SELECT sender, recipients, message, created, 'migration' FROM stash_static;

-- Drop old tables
DROP TABLE IF EXISTS senders_static;
DROP TABLE IF EXISTS stash_static;

-- Update schema version
UPDATE config SET value = '2' WHERE name = 'schema';
