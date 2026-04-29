-- Remove the CHECK(agent_type IN ('gemini','acp','codex')) constraint from
-- assistant_sessions. This constraint was created by a legacy Electron
-- frontend and prevents newer agent types (aionrs, openclaw-gateway, nanobot,
-- remote) from being stored.
--
-- SQLite does not support ALTER TABLE ... DROP CONSTRAINT, so we must
-- rebuild the table.

CREATE TABLE IF NOT EXISTS assistant_sessions_new (
    id              TEXT PRIMARY KEY NOT NULL,
    user_id         TEXT    NOT NULL REFERENCES assistant_users(id) ON DELETE CASCADE,
    agent_type      TEXT    NOT NULL,
    conversation_id TEXT    REFERENCES conversations(id) ON DELETE SET NULL,
    workspace       TEXT,
    chat_id         TEXT,
    created_at      INTEGER NOT NULL,
    last_activity   INTEGER NOT NULL
);

INSERT OR IGNORE INTO assistant_sessions_new
    (id, user_id, agent_type, conversation_id, workspace, chat_id, created_at, last_activity)
SELECT id, user_id, agent_type, conversation_id, workspace, chat_id, created_at, last_activity
FROM assistant_sessions;

DROP TABLE IF EXISTS assistant_sessions;

ALTER TABLE assistant_sessions_new RENAME TO assistant_sessions;

CREATE INDEX IF NOT EXISTS idx_assistant_sessions_user_id
    ON assistant_sessions(user_id);
CREATE INDEX IF NOT EXISTS idx_assistant_sessions_user_chat
    ON assistant_sessions(user_id, chat_id);
