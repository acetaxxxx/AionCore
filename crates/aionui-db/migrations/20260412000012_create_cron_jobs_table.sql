-- Cron jobs table: stores scheduled task definitions and execution state.
-- CASCADE DELETE: deleting a conversation does NOT cascade here;
-- cleanup is handled by the application layer (ICronRepository::delete_by_conversation).

CREATE TABLE IF NOT EXISTS cron_jobs (
    id                  TEXT    PRIMARY KEY NOT NULL,
    name                TEXT    NOT NULL,
    enabled             INTEGER NOT NULL DEFAULT 1,
    schedule_kind       TEXT    NOT NULL CHECK(schedule_kind IN ('at', 'every', 'cron')),
    schedule_value      TEXT    NOT NULL,
    schedule_tz         TEXT,
    schedule_description TEXT,
    payload_message     TEXT    NOT NULL,
    execution_mode      TEXT    NOT NULL DEFAULT 'existing'
                                CHECK(execution_mode IN ('existing', 'new_conversation')),
    agent_config        TEXT,
    conversation_id     TEXT    NOT NULL,
    conversation_title  TEXT,
    agent_type          TEXT    NOT NULL,
    created_by          TEXT    NOT NULL CHECK(created_by IN ('user', 'agent')),
    skill_content       TEXT,
    created_at          INTEGER NOT NULL,
    updated_at          INTEGER NOT NULL,
    next_run_at         INTEGER,
    last_run_at         INTEGER,
    last_status         TEXT    CHECK(last_status IN ('ok', 'error', 'skipped', 'missed')),
    last_error          TEXT,
    run_count           INTEGER NOT NULL DEFAULT 0,
    retry_count         INTEGER NOT NULL DEFAULT 0,
    max_retries         INTEGER NOT NULL DEFAULT 3
);

CREATE INDEX IF NOT EXISTS idx_cron_jobs_conversation
    ON cron_jobs(conversation_id);

CREATE INDEX IF NOT EXISTS idx_cron_jobs_next_run
    ON cron_jobs(next_run_at) WHERE enabled = 1;

CREATE INDEX IF NOT EXISTS idx_cron_jobs_agent_type
    ON cron_jobs(agent_type);

-- M-108: conversations table index for cronJobId lookups
CREATE INDEX IF NOT EXISTS idx_conversations_cron_job_id
    ON conversations(json_extract(extra, '$.cronJobId'));
