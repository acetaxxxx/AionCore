-- Create client_preferences table (generic key-value store)
CREATE TABLE IF NOT EXISTS client_preferences (
    key TEXT PRIMARY KEY NOT NULL,
    value TEXT NOT NULL,
    updated_at INTEGER NOT NULL
);
