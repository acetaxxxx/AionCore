CREATE TABLE IF NOT EXISTS remote_agents (
    id                TEXT PRIMARY KEY NOT NULL,
    name              TEXT NOT NULL,
    protocol          TEXT NOT NULL,
    url               TEXT NOT NULL,
    auth_type         TEXT NOT NULL,
    auth_token        TEXT,
    allow_insecure    INTEGER NOT NULL DEFAULT 0,
    avatar            TEXT,
    description       TEXT,
    device_id         TEXT,
    device_public_key TEXT,
    device_private_key TEXT,
    device_token      TEXT,
    status            TEXT NOT NULL DEFAULT 'unknown',
    last_connected_at INTEGER,
    created_at        INTEGER NOT NULL,
    updated_at        INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_remote_agents_status ON remote_agents(status);
