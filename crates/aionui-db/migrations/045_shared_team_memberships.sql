-- Shared Team is opt-in. Existing Teams remain private after this migration.
ALTER TABLE teams ADD COLUMN sharing_mode TEXT NOT NULL DEFAULT 'private'
    CHECK (sharing_mode IN ('private', 'shared'));

CREATE TABLE team_memberships (
    membership_ref TEXT PRIMARY KEY NOT NULL,
    team_id TEXT NOT NULL REFERENCES teams(id) ON DELETE CASCADE,
    user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    created_at INTEGER NOT NULL,
    UNIQUE (team_id, user_id)
);

CREATE INDEX idx_team_memberships_user_team ON team_memberships(user_id, team_id);
