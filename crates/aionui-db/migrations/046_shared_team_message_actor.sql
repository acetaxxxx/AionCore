-- Records the authenticated human who initiated a Team message separately
-- from the owner identity used to execute and persist Team conversations.
ALTER TABLE mailbox ADD COLUMN actor_user_id TEXT;
