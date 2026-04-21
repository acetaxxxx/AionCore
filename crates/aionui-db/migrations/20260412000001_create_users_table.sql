-- Create users table
CREATE TABLE IF NOT EXISTS users (
    id TEXT PRIMARY KEY NOT NULL,
    username TEXT NOT NULL UNIQUE,
    email TEXT UNIQUE,
    password_hash TEXT NOT NULL,
    avatar_path TEXT,
    jwt_secret TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    last_login INTEGER
);

CREATE INDEX IF NOT EXISTS idx_users_username ON users(username);
CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);
