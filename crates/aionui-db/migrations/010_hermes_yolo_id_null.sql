-- Hermes `acp` rejects the legacy startup `--yolo` flag before ACP
-- initialize completes. Existing databases must stop advertising a
-- `yolo_id` for the builtin Hermes row so startup callers do not infer
-- that incompatible flag.
UPDATE agent_metadata
SET yolo_id = NULL,
    updated_at = unixepoch('now','subsec')*1000
WHERE agent_source = 'builtin'
  AND backend = 'hermes'
  AND yolo_id IS NOT NULL;
