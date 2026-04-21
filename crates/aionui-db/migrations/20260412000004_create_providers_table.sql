-- Create providers table (model provider configuration)
CREATE TABLE IF NOT EXISTS providers (
    id TEXT PRIMARY KEY NOT NULL,
    platform TEXT NOT NULL,
    name TEXT NOT NULL,
    base_url TEXT NOT NULL,
    api_key_encrypted TEXT NOT NULL,
    models TEXT NOT NULL DEFAULT '[]',
    enabled INTEGER NOT NULL DEFAULT 1,
    capabilities TEXT NOT NULL DEFAULT '[]',
    context_limit INTEGER,
    model_protocols TEXT,
    model_enabled TEXT,
    model_health TEXT,
    bedrock_config TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_providers_platform ON providers(platform);
