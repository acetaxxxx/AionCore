-- Create team collaboration tables (teams, mailbox, team_tasks)

CREATE TABLE IF NOT EXISTS teams (
    id              TEXT PRIMARY KEY NOT NULL,
    name            TEXT NOT NULL,
    agents          TEXT NOT NULL DEFAULT '[]',
    lead_agent_id   TEXT,
    created_at      INTEGER NOT NULL,
    updated_at      INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS mailbox (
    id              TEXT PRIMARY KEY NOT NULL,
    team_id         TEXT NOT NULL,
    to_agent_id     TEXT NOT NULL,
    from_agent_id   TEXT NOT NULL,
    type            TEXT NOT NULL CHECK (type IN ('message', 'idle_notification', 'shutdown_request')),
    content         TEXT NOT NULL,
    summary         TEXT,
    read            INTEGER NOT NULL DEFAULT 0,
    created_at      INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_mailbox_team_to_read ON mailbox(team_id, to_agent_id, read);
CREATE INDEX IF NOT EXISTS idx_mailbox_team_id ON mailbox(team_id);

CREATE TABLE IF NOT EXISTS team_tasks (
    id              TEXT PRIMARY KEY NOT NULL,
    team_id         TEXT NOT NULL,
    subject         TEXT NOT NULL,
    description     TEXT,
    status          TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'in_progress', 'completed', 'deleted')),
    owner           TEXT,
    blocked_by      TEXT NOT NULL DEFAULT '[]',
    blocks          TEXT NOT NULL DEFAULT '[]',
    metadata        TEXT,
    created_at      INTEGER NOT NULL,
    updated_at      INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_team_tasks_team_id ON team_tasks(team_id);
