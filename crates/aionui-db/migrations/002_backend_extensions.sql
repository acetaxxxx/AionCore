-- Backend-only extensions: columns that exist in aionui-backend but not in AionUi.
-- Safe to run against both fresh databases (001 just created the tables)
-- and existing AionUi databases (tables exist, columns are new).

ALTER TABLE conversations ADD COLUMN pinned    INTEGER NOT NULL DEFAULT 0;
ALTER TABLE conversations ADD COLUMN pinned_at INTEGER;
