CREATE TABLE IF NOT EXISTS conversation_artifacts (
    id              TEXT PRIMARY KEY NOT NULL,
    conversation_id TEXT    NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
    cron_job_id     TEXT,
    kind            TEXT    NOT NULL
                            CHECK(kind IN ('cron_trigger', 'skill_suggest')),
    status          TEXT    NOT NULL DEFAULT 'active'
                            CHECK(status IN ('active', 'pending', 'dismissed', 'saved')),
    payload         TEXT    NOT NULL DEFAULT '{}',
    created_at      INTEGER NOT NULL,
    updated_at      INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_conversation_artifacts_conversation_id
    ON conversation_artifacts(conversation_id);
CREATE INDEX IF NOT EXISTS idx_conversation_artifacts_created_at
    ON conversation_artifacts(created_at);
CREATE INDEX IF NOT EXISTS idx_conversation_artifacts_conversation_created
    ON conversation_artifacts(conversation_id, created_at);
CREATE INDEX IF NOT EXISTS idx_conversation_artifacts_cron_job
    ON conversation_artifacts(cron_job_id);
CREATE INDEX IF NOT EXISTS idx_conversation_artifacts_kind_status
    ON conversation_artifacts(kind, status);
