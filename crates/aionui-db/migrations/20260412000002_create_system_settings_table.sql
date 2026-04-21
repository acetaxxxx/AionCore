-- Create system_settings table (single-row configuration)
CREATE TABLE IF NOT EXISTS system_settings (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    language TEXT NOT NULL DEFAULT 'en-US',
    notification_enabled INTEGER NOT NULL DEFAULT 1,
    cron_notification_enabled INTEGER NOT NULL DEFAULT 0,
    command_queue_enabled INTEGER NOT NULL DEFAULT 0,
    save_upload_to_workspace INTEGER NOT NULL DEFAULT 0,
    updated_at INTEGER NOT NULL
);
