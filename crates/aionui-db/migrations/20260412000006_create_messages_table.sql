-- Messages table: stores all chat messages within conversations.
-- CASCADE DELETE: deleting a conversation cascades to all its messages.

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

-- Query pattern indexes (from 02-database.md):
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
