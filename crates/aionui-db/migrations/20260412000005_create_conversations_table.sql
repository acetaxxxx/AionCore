-- Conversations table: stores all AI chat sessions.
-- CASCADE DELETE: deleting a user cascades to all their conversations.
--
-- Note: pinned/pinned_at fields originate from 05-conversation.md (TChatConversation)
-- rather than the base table definition in 02-database.md.

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
    pinned          INTEGER NOT NULL DEFAULT 0,
    pinned_at       INTEGER,
    created_at      INTEGER NOT NULL,
    updated_at      INTEGER NOT NULL
);

-- Query pattern indexes (from 02-database.md):
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
