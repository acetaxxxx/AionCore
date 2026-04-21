-- Create channel integration tables (assistant_plugins, assistant_users,
-- assistant_sessions, assistant_pairing_codes)

CREATE TABLE IF NOT EXISTS assistant_plugins (
    id              TEXT PRIMARY KEY NOT NULL,
    type            TEXT NOT NULL CHECK (type IN ('telegram', 'slack', 'discord', 'lark', 'dingtalk', 'weixin')),
    name            TEXT NOT NULL,
    enabled         INTEGER NOT NULL DEFAULT 0,
    config          TEXT NOT NULL,
    status          TEXT,
    last_connected  INTEGER,
    created_at      INTEGER NOT NULL,
    updated_at      INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS assistant_users (
    id                TEXT PRIMARY KEY NOT NULL,
    platform_user_id  TEXT NOT NULL,
    platform_type     TEXT NOT NULL,
    display_name      TEXT,
    authorized_at     INTEGER NOT NULL,
    last_active       INTEGER,
    session_id        TEXT,
    UNIQUE (platform_user_id, platform_type)
);

CREATE TABLE IF NOT EXISTS assistant_sessions (
    id                TEXT PRIMARY KEY NOT NULL,
    user_id           TEXT NOT NULL REFERENCES assistant_users(id) ON DELETE CASCADE,
    agent_type        TEXT NOT NULL CHECK (agent_type IN ('gemini', 'acp', 'codex', 'openclaw-gateway')),
    conversation_id   TEXT REFERENCES conversations(id) ON DELETE SET NULL,
    workspace         TEXT,
    chat_id           TEXT,
    created_at        INTEGER NOT NULL,
    last_activity     INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_assistant_sessions_user_id ON assistant_sessions(user_id);
CREATE INDEX IF NOT EXISTS idx_assistant_sessions_user_chat ON assistant_sessions(user_id, chat_id);

CREATE TABLE IF NOT EXISTS assistant_pairing_codes (
    code              TEXT PRIMARY KEY NOT NULL,
    platform_user_id  TEXT NOT NULL,
    platform_type     TEXT NOT NULL,
    display_name      TEXT,
    requested_at      INTEGER NOT NULL,
    expires_at        INTEGER NOT NULL,
    status            TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'approved', 'rejected', 'expired'))
);

CREATE INDEX IF NOT EXISTS idx_pairing_codes_status ON assistant_pairing_codes(status);
