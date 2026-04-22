-- Initial schema: matches AionUi v26 database layout.
-- All statements use IF NOT EXISTS so this migration is safe to run
-- against an existing AionUi database (every CREATE is a no-op).

-- ── Users ───────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS users (
    id            TEXT PRIMARY KEY NOT NULL,
    username      TEXT NOT NULL UNIQUE,
    email         TEXT UNIQUE,
    password_hash TEXT NOT NULL,
    avatar_path   TEXT,
    jwt_secret    TEXT,
    created_at    INTEGER NOT NULL,
    updated_at    INTEGER NOT NULL,
    last_login    INTEGER
);

CREATE INDEX IF NOT EXISTS idx_users_username ON users(username);
CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);

-- ── System Settings ─────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS system_settings (
    id                        INTEGER PRIMARY KEY CHECK (id = 1),
    language                  TEXT    NOT NULL DEFAULT 'en-US',
    notification_enabled      INTEGER NOT NULL DEFAULT 1,
    cron_notification_enabled INTEGER NOT NULL DEFAULT 0,
    command_queue_enabled     INTEGER NOT NULL DEFAULT 0,
    save_upload_to_workspace  INTEGER NOT NULL DEFAULT 0,
    updated_at                INTEGER NOT NULL
);

-- ── Client Preferences ──────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS client_preferences (
    key        TEXT PRIMARY KEY NOT NULL,
    value      TEXT    NOT NULL,
    updated_at INTEGER NOT NULL
);

-- ── Providers ───────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS providers (
    id                TEXT PRIMARY KEY NOT NULL,
    platform          TEXT    NOT NULL,
    name              TEXT    NOT NULL,
    base_url          TEXT    NOT NULL,
    api_key_encrypted TEXT    NOT NULL,
    models            TEXT    NOT NULL DEFAULT '[]',
    enabled           INTEGER NOT NULL DEFAULT 1,
    capabilities      TEXT    NOT NULL DEFAULT '[]',
    context_limit     INTEGER,
    model_protocols   TEXT,
    model_enabled     TEXT,
    model_health      TEXT,
    bedrock_config    TEXT,
    created_at        INTEGER NOT NULL,
    updated_at        INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_providers_platform ON providers(platform);

-- ── Conversations ───────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS conversations (
    id              TEXT    PRIMARY KEY NOT NULL,
    user_id         TEXT    NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    name            TEXT    NOT NULL,
    type            TEXT    NOT NULL,
    extra           TEXT    NOT NULL DEFAULT '{}',
    model           TEXT,
    status          TEXT    NOT NULL DEFAULT 'pending'
                            CHECK(status IN ('pending', 'running', 'finished')),
    source          TEXT,
    channel_chat_id TEXT,
    created_at      INTEGER NOT NULL,
    updated_at      INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_conversations_user_id
    ON conversations(user_id);
CREATE INDEX IF NOT EXISTS idx_conversations_updated_at
    ON conversations(updated_at);
CREATE INDEX IF NOT EXISTS idx_conversations_type
    ON conversations(type);
CREATE INDEX IF NOT EXISTS idx_conversations_user_updated
    ON conversations(user_id, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_conversations_source
    ON conversations(source);
CREATE INDEX IF NOT EXISTS idx_conversations_source_updated
    ON conversations(source, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_conversations_source_chat
    ON conversations(source, channel_chat_id, updated_at DESC);

-- ── Messages ────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS messages (
    id              TEXT    PRIMARY KEY NOT NULL,
    conversation_id TEXT    NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
    msg_id          TEXT,
    type            TEXT    NOT NULL,
    content         TEXT    NOT NULL DEFAULT '{}',
    position        TEXT    CHECK(position IN ('left', 'right', 'center', 'pop')),
    status          TEXT    CHECK(status IN ('finish', 'pending', 'error', 'work')),
    hidden          INTEGER NOT NULL DEFAULT 0,
    created_at      INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_messages_conversation_id
    ON messages(conversation_id);
CREATE INDEX IF NOT EXISTS idx_messages_created_at
    ON messages(created_at);
CREATE INDEX IF NOT EXISTS idx_messages_type
    ON messages(type);
CREATE INDEX IF NOT EXISTS idx_messages_msg_id
    ON messages(msg_id);
CREATE INDEX IF NOT EXISTS idx_messages_conv_created
    ON messages(conversation_id, created_at);
CREATE INDEX IF NOT EXISTS idx_messages_conversation_created
    ON messages(conversation_id, created_at);
CREATE INDEX IF NOT EXISTS idx_messages_conv_created_desc
    ON messages(conversation_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_messages_type_created
    ON messages(type, created_at DESC);

-- ── Remote Agents ───────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS remote_agents (
    id                 TEXT PRIMARY KEY NOT NULL,
    name               TEXT    NOT NULL,
    protocol           TEXT    NOT NULL,
    url                TEXT    NOT NULL,
    auth_type          TEXT    NOT NULL,
    auth_token         TEXT,
    allow_insecure     INTEGER NOT NULL DEFAULT 0,
    avatar             TEXT,
    description        TEXT,
    device_id          TEXT,
    device_public_key  TEXT,
    device_private_key TEXT,
    device_token       TEXT,
    status             TEXT    NOT NULL DEFAULT 'unknown',
    last_connected_at  INTEGER,
    created_at         INTEGER NOT NULL,
    updated_at         INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_remote_agents_status ON remote_agents(status);

-- ── MCP Servers ─────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS mcp_servers (
    id               TEXT PRIMARY KEY NOT NULL,
    name             TEXT    NOT NULL UNIQUE,
    description      TEXT,
    enabled          INTEGER NOT NULL DEFAULT 0,
    transport_type   TEXT    NOT NULL,
    transport_config TEXT    NOT NULL,
    tools            TEXT,
    status           TEXT    NOT NULL DEFAULT 'disconnected',
    last_connected   INTEGER,
    original_json    TEXT,
    builtin          INTEGER NOT NULL DEFAULT 0,
    created_at       INTEGER NOT NULL,
    updated_at       INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_mcp_servers_name ON mcp_servers(name);
CREATE INDEX IF NOT EXISTS idx_mcp_servers_enabled ON mcp_servers(enabled);

-- ── OAuth Tokens ────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS oauth_tokens (
    server_url    TEXT PRIMARY KEY NOT NULL,
    access_token  TEXT    NOT NULL,
    refresh_token TEXT,
    token_type    TEXT    NOT NULL DEFAULT 'bearer',
    expires_at    INTEGER,
    created_at    INTEGER NOT NULL,
    updated_at    INTEGER NOT NULL
);

-- ── Channel Integration ─────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS assistant_plugins (
    id             TEXT PRIMARY KEY NOT NULL,
    type           TEXT    NOT NULL,
    name           TEXT    NOT NULL,
    enabled        INTEGER NOT NULL DEFAULT 0,
    config         TEXT    NOT NULL,
    status         TEXT,
    last_connected INTEGER,
    created_at     INTEGER NOT NULL,
    updated_at     INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS assistant_users (
    id               TEXT PRIMARY KEY NOT NULL,
    platform_user_id TEXT    NOT NULL,
    platform_type    TEXT    NOT NULL,
    display_name     TEXT,
    authorized_at    INTEGER NOT NULL,
    last_active      INTEGER,
    session_id       TEXT,
    UNIQUE (platform_user_id, platform_type)
);

CREATE TABLE IF NOT EXISTS assistant_sessions (
    id              TEXT PRIMARY KEY NOT NULL,
    user_id         TEXT    NOT NULL REFERENCES assistant_users(id) ON DELETE CASCADE,
    agent_type      TEXT    NOT NULL,
    conversation_id TEXT    REFERENCES conversations(id) ON DELETE SET NULL,
    workspace       TEXT,
    chat_id         TEXT,
    created_at      INTEGER NOT NULL,
    last_activity   INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_assistant_sessions_user_id
    ON assistant_sessions(user_id);
CREATE INDEX IF NOT EXISTS idx_assistant_sessions_user_chat
    ON assistant_sessions(user_id, chat_id);

CREATE TABLE IF NOT EXISTS assistant_pairing_codes (
    code             TEXT PRIMARY KEY NOT NULL,
    platform_user_id TEXT    NOT NULL,
    platform_type    TEXT    NOT NULL,
    display_name     TEXT,
    requested_at     INTEGER NOT NULL,
    expires_at       INTEGER NOT NULL,
    status           TEXT    NOT NULL DEFAULT 'pending'
                             CHECK (status IN ('pending', 'approved', 'rejected', 'expired'))
);

CREATE INDEX IF NOT EXISTS idx_pairing_codes_status
    ON assistant_pairing_codes(status);

-- ── Teams ───────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS teams (
    id             TEXT PRIMARY KEY NOT NULL,
    user_id        TEXT    NOT NULL DEFAULT 'system_default_user',
    name           TEXT    NOT NULL,
    workspace      TEXT    NOT NULL DEFAULT '',
    workspace_mode TEXT    NOT NULL DEFAULT 'shared',
    agents         TEXT    NOT NULL DEFAULT '[]',
    lead_agent_id  TEXT,
    session_mode   TEXT,
    created_at     INTEGER NOT NULL,
    updated_at     INTEGER NOT NULL
);

-- ── Mailbox ─────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS mailbox (
    id            TEXT    PRIMARY KEY NOT NULL,
    team_id       TEXT    NOT NULL,
    to_agent_id   TEXT    NOT NULL,
    from_agent_id TEXT    NOT NULL,
    type          TEXT    NOT NULL CHECK (type IN ('message', 'idle_notification', 'shutdown_request')),
    content       TEXT    NOT NULL,
    summary       TEXT,
    files         TEXT,
    read          INTEGER NOT NULL DEFAULT 0,
    created_at    INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_mailbox_team_to_read
    ON mailbox(team_id, to_agent_id, read);
CREATE INDEX IF NOT EXISTS idx_mailbox_team_id
    ON mailbox(team_id);

-- ── Team Tasks ──────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS team_tasks (
    id          TEXT    PRIMARY KEY NOT NULL,
    team_id     TEXT    NOT NULL,
    subject     TEXT    NOT NULL,
    description TEXT,
    status      TEXT    NOT NULL DEFAULT 'pending'
                        CHECK (status IN ('pending', 'in_progress', 'completed', 'deleted')),
    owner       TEXT,
    blocked_by  TEXT    NOT NULL DEFAULT '[]',
    blocks      TEXT    NOT NULL DEFAULT '[]',
    metadata    TEXT,
    created_at  INTEGER NOT NULL,
    updated_at  INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_team_tasks_team_id
    ON team_tasks(team_id);

-- ── ACP Session ─────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS acp_session (
    conversation_id TEXT PRIMARY KEY,
    agent_backend   TEXT    NOT NULL,
    agent_source    TEXT    NOT NULL,
    agent_id        TEXT    NOT NULL,
    session_id      TEXT,
    session_status  TEXT    NOT NULL DEFAULT 'idle',
    session_config  TEXT    NOT NULL DEFAULT '{}',
    last_active_at  INTEGER,
    suspended_at    INTEGER
);

CREATE INDEX IF NOT EXISTS idx_acp_session_status
    ON acp_session(session_status);
CREATE INDEX IF NOT EXISTS idx_acp_session_suspended
    ON acp_session(session_status, suspended_at)
    WHERE session_status = 'suspended';
CREATE INDEX IF NOT EXISTS idx_acp_session_agent_id
    ON acp_session(agent_id);

-- ── Cron Jobs ───────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS cron_jobs (
    id                   TEXT    PRIMARY KEY NOT NULL,
    name                 TEXT    NOT NULL,
    enabled              INTEGER NOT NULL DEFAULT 1,
    schedule_kind        TEXT    NOT NULL CHECK(schedule_kind IN ('at', 'every', 'cron')),
    schedule_value       TEXT    NOT NULL,
    schedule_tz          TEXT,
    schedule_description TEXT,
    payload_message      TEXT    NOT NULL,
    execution_mode       TEXT    NOT NULL DEFAULT 'existing'
                                 CHECK(execution_mode IN ('existing', 'new_conversation')),
    agent_config         TEXT,
    conversation_id      TEXT    NOT NULL,
    conversation_title   TEXT,
    agent_type           TEXT    NOT NULL,
    created_by           TEXT    NOT NULL CHECK(created_by IN ('user', 'agent')),
    skill_content        TEXT,
    description          TEXT,
    created_at           INTEGER NOT NULL,
    updated_at           INTEGER NOT NULL,
    next_run_at          INTEGER,
    last_run_at          INTEGER,
    last_status          TEXT    CHECK(last_status IN ('ok', 'error', 'skipped', 'missed')),
    last_error           TEXT,
    run_count            INTEGER NOT NULL DEFAULT 0,
    retry_count          INTEGER NOT NULL DEFAULT 0,
    max_retries          INTEGER NOT NULL DEFAULT 3
);

CREATE INDEX IF NOT EXISTS idx_cron_jobs_conversation
    ON cron_jobs(conversation_id);
CREATE INDEX IF NOT EXISTS idx_cron_jobs_next_run
    ON cron_jobs(next_run_at) WHERE enabled = 1;
CREATE INDEX IF NOT EXISTS idx_cron_jobs_agent_type
    ON cron_jobs(agent_type);
CREATE INDEX IF NOT EXISTS idx_conversations_cron_job_id
    ON conversations(json_extract(extra, '$.cronJobId'));
