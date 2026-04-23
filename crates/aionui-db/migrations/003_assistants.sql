-- User-authored assistants only. Built-ins and extension-contributed are
-- resolved in memory and not stored here.
CREATE TABLE assistants (
    id                        TEXT PRIMARY KEY,
    name                      TEXT NOT NULL,
    description               TEXT,
    avatar                    TEXT,
    preset_agent_type         TEXT NOT NULL DEFAULT 'gemini',
    enabled_skills            TEXT,  -- JSON: string[]
    custom_skill_names        TEXT,  -- JSON: string[]
    disabled_builtin_skills   TEXT,  -- JSON: string[]
    prompts                   TEXT,  -- JSON: string[]
    models                    TEXT,  -- JSON: string[]
    name_i18n                 TEXT,  -- JSON: {locale: string}
    description_i18n          TEXT,  -- JSON: {locale: string}
    prompts_i18n              TEXT,  -- JSON: {locale: string[]}
    created_at                INTEGER NOT NULL,
    updated_at                INTEGER NOT NULL
);

CREATE INDEX idx_assistants_updated_at ON assistants (updated_at DESC);

-- Per-assistant user state. Rows may reference built-in or user ids; never
-- extension ids (extension assistants are read-only). No FK because the
-- referent may live in memory (built-in) rather than a table.
CREATE TABLE assistant_overrides (
    assistant_id   TEXT PRIMARY KEY,
    enabled        INTEGER NOT NULL DEFAULT 1,
    sort_order     INTEGER NOT NULL DEFAULT 0,
    last_used_at   INTEGER,
    updated_at     INTEGER NOT NULL
);
