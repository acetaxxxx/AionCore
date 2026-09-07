-- Browser Web Push subscriptions are user-scoped durable delivery targets.
-- Browser endpoint and key material are encrypted by the repository; the
-- endpoint hash exists only to enforce endpoint uniqueness and replacement.
CREATE TABLE IF NOT EXISTS push_subscriptions (
    id                  TEXT PRIMARY KEY NOT NULL,
    user_id             TEXT NOT NULL,
    endpoint_hash       TEXT NOT NULL UNIQUE,
    endpoint_encrypted  TEXT NOT NULL,
    p256dh_encrypted    TEXT NOT NULL,
    auth_encrypted      TEXT NOT NULL,
    created_at          INTEGER NOT NULL,
    updated_at          INTEGER NOT NULL,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_push_subscriptions_user_id
    ON push_subscriptions(user_id, created_at, id);
