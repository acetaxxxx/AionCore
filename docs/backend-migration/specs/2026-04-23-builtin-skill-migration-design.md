# Built-in Skill Migration — Backend Design Spec

**Date:** 2026-04-23
**Scope:** Backend-side design for embedding built-in skill resources into
the `aionui-backend` binary via `include_dir!`, introducing two new
endpoints for gemini CLI materialization, and refactoring
`aionui-extension::skill_service` to read from embedded assets rather than
the on-disk sibling directory.

**Companion spec (frontend-side migration + team plan):**
[`AionUi/docs/backend-migration/specs/2026-04-23-builtin-skill-migration-design.md`](../../../../AionUi/docs/backend-migration/specs/2026-04-23-builtin-skill-migration-design.md)

**Reference — prior pilot pattern:**
[`2026-04-23-assistant-user-data-migration-design.md`](./2026-04-23-assistant-user-data-migration-design.md) — H2 (include_dir) is the direct template.

---

## 1. Context

The `aionui-extension` crate already serves `/api/skills/*` endpoints today:

- `GET /api/skills` — merged list (builtin + custom + extension)
- `GET /api/skills/builtin-auto` — scans `{builtin_skills_dir}/_builtin/`
- `POST /api/skills/builtin-skill` — reads `{builtin_skills_dir}/{fileName}`
- ...

The implementation reads from `app_resource_dir` = `{current_exe}/..`, the
same fragile assumption that bit the assistant pilot. When the backend is
launched via the AionUi Electron-packaged binary (which never ships a
`builtin-skills/` sibling), the endpoint returns empty.

The frontend has its own separate copy of the same skill corpus at
`AionUi/src/process/resources/skills/`, syncs it to `{cacheDir}/builtin-skills/`
at startup, and reads from that. Two sources, constantly drifting.

**This spec eliminates the on-disk dependency entirely** — backend embeds
the corpus into the binary, backend is the sole source, frontend reads
through HTTP.

## 2. Goals

1. Built-in skills are embedded into the backend binary (`include_dir!`);
   runtime path resolution goes away.
2. `_builtin/` subdirectory renamed to `auto-inject/` in both the corpus
   and the code.
3. `GET /api/skills/builtin-auto` response includes a `location` field on
   each entry so the frontend can pass that back into `readBuiltinSkill`
   without string-concatenating paths.
4. Two new endpoints (`POST` / `DELETE /api/skills/materialize-for-agent`)
   let the frontend hand off skill-file materialization for gemini CLI to
   the backend, which writes into a `data_dir`-scoped temp folder. The
   frontend never touches skill files.
5. An `AIONUI_BUILTIN_SKILLS_PATH` env var keeps a disk-based read path
   available for E2E and rapid iteration.

## 3. Non-Goals

- No changes to `GET /api/skills` on the custom / extension source sides.
- No changes to assistant-skill or assistant-rule dispatch (already done
  in the assistant pilot).
- No conversion of `builtin_skills_dir` ingestion to a different
  representation (the embedded `Dir<'static>` is what it is — mirror the
  assistant pilot's approach).

## 4. Architecture

```
aionui-extension (existing crate)
    │
    ├── constants.rs
    │     BUILTIN_AUTO_SKILLS_SUBDIR: &str = "auto-inject"  (was "_builtin")
    │
    ├── skill_service.rs
    │     static BUILTIN_SKILLS: Dir<'static> = include_dir!(
    │         "$CARGO_MANIFEST_DIR/../aionui-app/assets/builtin-skills"
    │     );
    │
    │     fn read_builtin_skill(paths, file_name) -> String
    │         // 1. If AIONUI_BUILTIN_SKILLS_PATH set → read from disk
    │         // 2. else → BUILTIN_SKILLS.get_file(file_name).contents_utf8()
    │
    │     fn list_builtin_auto_skills(paths) -> Vec<AutoSkill>
    │         // Iterate BUILTIN_SKILLS/auto-inject/{name}/SKILL.md,
    │         // parse frontmatter, emit { name, description, location }
    │
    │     fn list_skills(paths) -> Vec<SkillInfo>
    │         // For source=builtin, location stays absolute-style for
    │         // backward compat (consumers like SkillsHubSettings export
    │         // use it); a NEW relative_location field carries the
    │         // relative path that the frontend passes back into
    │         // read_builtin_skill.
    │
    │     fn materialize_skills_for_agent(params) -> PathBuf
    │         // Write embedded skill contents into
    │         // {data_dir}/agent-skills/{conversationId}/
    │         // - all auto-inject skills
    │         // - opt-in skills whose name is in enabledSkills
    │         // Also copies user + extension skills (from their disk paths)
    │         // Returns the absolute dir path.
    │
    │     fn cleanup_agent_skills(conversation_id) -> ()
    │         // fs::remove_dir_all({data_dir}/agent-skills/{conversationId}/)
    │
    └── skill_routes.rs
          + route POST /api/skills/materialize-for-agent → materialize handler
          + route DELETE /api/skills/materialize-for-agent/{conversationId} → cleanup handler
```

**Cargo changes:**

```toml
# crates/aionui-extension/Cargo.toml
[dependencies]
include_dir = "0.7"   # new
```

`aionui-app` gains the dependency indirectly. No new crate, no new
migration file (no DB changes).

## 5. Data Structures

### 5.1 `SkillPaths` adjustment

```rust
pub struct SkillPaths {
    // was: pub builtin_skills_dir: PathBuf
    // now: None when using embedded (production); Some when env override set.
    pub builtin_skills_dir: Option<PathBuf>,
    pub skills_dir: PathBuf,           // user skills, unchanged
    pub builtin_rules_dir: PathBuf,    // unchanged
    pub assistant_rules_dir: PathBuf,  // unchanged
    pub assistant_skills_dir: PathBuf, // unchanged
    pub data_dir: PathBuf,             // NEW: root for {data_dir}/agent-skills
}

pub fn resolve_skill_paths(app_resource_dir: &Path, data_dir: &Path) -> SkillPaths {
    let builtin_skills_dir = std::env::var("AIONUI_BUILTIN_SKILLS_PATH")
        .ok()
        .map(PathBuf::from);

    SkillPaths {
        builtin_skills_dir,
        skills_dir: data_dir.join(SKILLS_DIR_NAME),
        builtin_rules_dir: app_resource_dir.join(BUILTIN_RULES_DIR_NAME),
        assistant_rules_dir: data_dir.join(ASSISTANT_RULES_DIR_NAME),
        assistant_skills_dir: data_dir.join(ASSISTANT_SKILLS_DIR_NAME),
        data_dir: data_dir.to_path_buf(),
    }
}
```

Add `data_dir` to the struct so `materialize` has somewhere to write.

### 5.2 Response types (api-types)

```rust
// crates/aionui-api-types/src/extension.rs (or skill.rs if separated)

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BuiltinAutoSkill {
    pub name: String,
    pub description: String,
    pub location: String,  // NEW — e.g. "auto-inject/cron/SKILL.md"
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SkillInfo {
    pub name: String,
    pub description: String,
    /// Filesystem-usable path. For `source=custom`, the absolute path of
    /// the user's skill dir. For `source=builtin`, a backend-synthesized
    /// absolute path under `{data_dir}/builtin-skills-view/{name}/SKILL.md`
    /// that the export-symlink flow can resolve. For `source=extension`,
    /// the path the extension declares.
    pub location: String,
    /// Present only for `source=builtin`. The relative path the frontend
    /// passes to `readBuiltinSkill` (e.g. `"auto-inject/cron/SKILL.md"`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub relative_location: Option<String>,
    pub is_custom: bool,
    pub source: SkillSource,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MaterializeSkillsRequest {
    pub conversation_id: String,
    pub enabled_skills: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct MaterializeSkillsResponse {
    pub dir_path: String,  // absolute path
}
```

`BuiltinAutoSkill` adds `location` without removing fields. `SkillInfo.location`
changes semantics for `source=builtin` only (relative path) — frontend already
in the habit of receiving path strings, no type change.

## 6. API Contract

### 6.1 Existing endpoints — implementation changes

**`GET /api/skills/builtin-auto`**

- Request: unchanged
- Response: each entry now carries a `location` field.
  ```json
  [
    { "name": "cron", "description": "...", "location": "auto-inject/cron/SKILL.md" },
    { "name": "office-cli", "description": "...", "location": "auto-inject/office-cli/SKILL.md" }
  ]
  ```
- Implementation change: reads embedded `BUILTIN_SKILLS.get_dir("auto-inject")`
  unless `AIONUI_BUILTIN_SKILLS_PATH` is set.

**`POST /api/skills/builtin-skill`**

- Request: `{ "fileName": "auto-inject/cron/SKILL.md" }` — relative path
  under the builtin-skills root, now accepting the `auto-inject/` prefix.
- Response: md content string (unchanged).
- Implementation: `BUILTIN_SKILLS.get_file(&file_name).and_then(|f| f.contents_utf8())`.
  Still validated against `../` traversal (`validate_filename`). Returns empty
  string if missing (preserves existing behavior).

**`GET /api/skills`**

- Request: unchanged
- Response: shape adds an optional `relativeLocation` field for
  `source=builtin` rows. `location` stays absolute for all sources to
  preserve the `SkillsHubSettings` export flow (which passes `location`
  into `exportSkillWithSymlink` as an absolute path).
- For `source=builtin`: backend synthesizes `location` as
  `{data_dir}/builtin-skills-view/{name}/SKILL.md`. The backend lazily
  materializes that view on first request (writing embedded contents to
  disk) so that the symlink export flow has a real source path. This
  view is separate from `agent-skills/` (gemini CLI use) — simpler
  layout, one subdir per skill. `relativeLocation` carries the
  relative-to-embedded path that `readBuiltinSkill` expects.
- For `source=custom` / `source=extension`: unchanged.

### 6.2 New endpoints

**`POST /api/skills/materialize-for-agent`**

```
Request body:
  {
    "conversationId": "conv-abc-123",
    "enabledSkills": ["mermaid", "pdf"]
  }

Response (200):
  {
    "dirPath": "/Users/zhoukai/.aionui-dev/agent-skills/conv-abc-123"
  }

Response (400): validation error (empty conversationId, path traversal in name, ...)
Response (500): filesystem error
```

Behavior:

1. Create `{data_dir}/agent-skills/{conversationId}/`. If exists, delete
   and recreate (ensures fresh state between retries).
2. Write every auto-inject skill into the target dir at the **flat**
   layout: `{target}/{name}/SKILL.md` (plus optional `references/`,
   `scripts/` subtrees). The auto-inject origin is preserved in the
   embedded corpus but disappears at the flattened output — gemini CLI
   only cares about one directory per skill.
3. For each name in `enabledSkills`:
   - Look up in built-in (embedded) — if found, write to `{target}/{name}/`
   - Else in custom (`{skills_dir}/{name}/`) — if found, copy the directory
   - Else in extension skills — if found, copy
   - Else ignore (log a warning)
4. **Collision rule:** auto-inject skills are written first. If a name
   in `enabledSkills` collides with an auto-inject name (should not
   happen in practice), the opt-in overwrites the auto-inject version.
   Log a warning.
5. Return the absolute `dirPath`.

**`DELETE /api/skills/materialize-for-agent/{conversationId}`**

```
Response (200): { "success": true }
Response (400): empty / malformed conversationId
Idempotent: directory not existing → 200 (not 404)
```

Behavior: `fs::remove_dir_all({data_dir}/agent-skills/{conversationId}/)`, swallow
"not found" errors. Returns 200 in both cases.

### 6.3 Existing endpoints NOT modified

| Endpoint | Why |
|---|---|
| `GET /api/skills/paths` | Still returns `{userSkillsDir, builtinSkillsDir}`. The `builtinSkillsDir` string is now "emulated" — e.g., `"embedded://builtin-skills"` or the env-override path. Consumer (`SkillsHubSettings`) only uses it for display. |
| `POST /api/skills/import`, `/api/skills/scan`, etc. | All operate on user skills directory. Not affected. |
| `POST /api/skills/assistant-rule/*`, `/api/skills/assistant-skill/*` | Assistant-side dispatch, unaffected. |

## 7. Implementation Layout

```
crates/aionui-extension/
├── Cargo.toml                (+ include_dir = "0.7")
├── src/
│   ├── constants.rs          (BUILTIN_AUTO_SKILLS_SUBDIR renamed)
│   ├── skill_service.rs      (add BUILTIN_SKILLS static,
│   │                          rewrite read_builtin_skill,
│   │                          list_builtin_auto_skills,
│   │                          list_skills;
│   │                          add materialize_skills_for_agent,
│   │                          cleanup_agent_skills)
│   ├── skill_routes.rs       (+ 2 new routes)
│   └── state.rs              (SkillRouterState unchanged in shape,
│                              just gets data_dir threaded in)
└── tests/
    └── skill_builtin_embed.rs  (NEW — integration tests for embedded read)

crates/aionui-app/
├── assets/builtin-skills/    NEW DIR (copy from AionUi src/process/resources/skills/,
│                              rename _builtin → auto-inject)
└── src/lib.rs                (pass data_dir into resolve_skill_paths call)

crates/aionui-app/tests/
└── skills_builtin_e2e.rs     NEW — full HTTP surface, materialize + cleanup round-trip
```

## 8. `aionui-app` Wiring

The only integration change is passing `data_dir` into
`resolve_skill_paths()` so the skill service knows where to materialize:

```rust
// crates/aionui-app/src/lib.rs — around the existing skill_paths block
let app_resource_dir = std::env::current_exe()
    .ok()
    .and_then(|p| p.canonicalize().ok())   // stay symlink-safe (assistant H1 lesson)
    .and_then(|p| p.parent().map(|pp| pp.to_path_buf()))
    .unwrap_or_else(|| std::path::PathBuf::from("."));

let skill_paths = aionui_extension::resolve_skill_paths(
    &app_resource_dir,
    data_dir,    // NEW — passed into SkillPaths for materialize target
);
```

On startup, also invoke a one-time orphan cleanup. **Cross-crate rule:**
`aionui-extension` MUST NOT import `aionui-conversation`. The cleanup
helper takes a predicate closure; the composition layer (`aionui-app`)
wires the conversation-repo check:

```rust
// In aionui-extension::skill_service — no conversation-repo dependency:
pub async fn cleanup_orphan_agent_skills<F>(
    paths: &SkillPaths,
    is_live_conversation: F,
) -> Result<(), std::io::Error>
where
    F: Fn(&str) -> bool,
{
    // Scan {data_dir}/agent-skills/*; for each subdir named {convId},
    // remove if !is_live_conversation(convId).
}

// In aionui-app composition:
let conv_repo = services.conversation_repo.clone();
skill_service::cleanup_orphan_agent_skills(&skill_paths, |id| {
    // Blocking check is fine — startup, small count. Use a blocking
    // tokio handle or the async repo with a tokio::runtime::Handle.
    conv_repo.exists_blocking(id)
}).await?;
```

This keeps `aionui-extension` dependency-free of domain crates, only
`aionui-db` traits if needed.

## 9. Testing

### 9.1 Rust unit tests — `crates/aionui-extension/src/skill_service.rs`

| Test | Assertion |
|---|---|
| `embedded_lists_auto_inject_from_corpus` | 4 entries returned (cron, office-cli, aionui-skills, skill-creator per current corpus); each has non-empty `location` starting with `auto-inject/` |
| `embedded_reads_builtin_skill_content` | `read_builtin_skill("auto-inject/cron/SKILL.md")` returns non-empty string starting with `---` (frontmatter) |
| `embedded_rejects_path_traversal` | `read_builtin_skill("../etc/passwd")` → `ExtensionError::InvalidFilename` |
| `embedded_handles_missing_file` | `read_builtin_skill("nonexistent/SKILL.md")` → empty string (preserves existing contract) |
| `env_override_reads_from_disk` | Seeds `AIONUI_BUILTIN_SKILLS_PATH` to a temp dir; `list_builtin_auto_skills` reflects the seeded content, not embedded |
| `list_skills_builtin_has_relative_location` | `source=builtin` entries have relative `location`; `source=custom` entries still absolute |
| `materialize_creates_fresh_dir` | Call materialize twice with same `convId`; second call starts fresh |
| `materialize_includes_auto_inject` | Without `enabledSkills`, returned dir has all auto-inject subtrees |
| `materialize_includes_opt_in` | With `enabledSkills=["mermaid"]`, dir includes mermaid but not moltbook |
| `materialize_handles_nonexistent_skill_name` | Unknown name is silently skipped; log emitted |
| `cleanup_is_idempotent` | Call cleanup twice; both return 200 |
| `orphan_cleanup_removes_stale` | Seed `agent-skills/` with a dir not in conversations table; orphan cleanup removes it |
| `orphan_cleanup_preserves_live` | Seed a dir whose id is in conversations table; orphan cleanup keeps it |

### 9.2 Rust integration (HTTP) — `crates/aionui-app/tests/skills_builtin_e2e.rs`

Standard tower::oneshot pattern matching `assistants_e2e.rs`:

| Endpoint | Scenarios |
|---|---|
| `GET /api/skills/builtin-auto` | Happy: ≥ 4 entries, each with location. Env override: disk entries returned. |
| `POST /api/skills/builtin-skill` | With `auto-inject/cron/SKILL.md` → 200 + content. With `mermaid/SKILL.md` → 200. With `../passwd` → 400. |
| `GET /api/skills` | `source=builtin` rows have relative `location`. Merged builtin+custom+extension without duplicates. |
| `POST /api/skills/materialize-for-agent` | Happy: returns `dirPath` + dir exists + auto-inject subtrees present. With `enabledSkills=["mermaid"]`: mermaid present. With bogus name: skipped silently. Re-call: fresh dir. |
| `DELETE /api/skills/materialize-for-agent/{id}` | Happy: returns 200, dir gone. Call twice: idempotent. Unknown id: 200 anyway. |

### 9.3 Gates

Before the backend feature branch is merged:

```bash
cargo fmt --all -- --check      # clean
cargo clippy --workspace -- -D warnings  # clean
cargo test --workspace          # all green (includes existing skill_service tests
                                #   updated for _builtin → auto-inject)
cargo test --test skills_builtin_e2e
cargo test --test assistants_e2e  # assistant pilot regression — include_dir
                                  #   additions here should not interact
```

## 10. Risks

1. **Binary size growth.** `include_dir` embedding ~30 skill SKILL.md files
   (plus `references/` dirs for some) estimated at ~1-3 MB uncompressed.
   Acceptable given assistant's +2.8MB precedent.
2. **Large skill files in the corpus.** Some opt-in skills (morph-ppt,
   pptx, xlsx) have heavy reference docs. Compile time grows. Mitigation:
   monitor `cargo build` time; if ≥ 2× baseline on clean builds, switch
   heavy skills to runtime-fetched-from-disk mode with an override.
3. **Orphan cleanup depends on conversations table access.** Circular
   worry: if backend's DB init fails, orphan cleanup fails. Mitigation:
   wrap cleanup in `if db.is_ok() { ... }`; not critical to startup.
4. **gemini CLI filesystem expectations.** gemini's `--extensions` path
   semantics may require specific subdir structure (e.g. one extension
   per subdir with `gemini.config.json`). Verify the current wrapper in
   `gemini/cli/config.ts` matches the `{target}/auto-inject-{name}/SKILL.md`
   layout the materialize endpoint produces. Adjust if needed during
   T1 or T4.
5. **Coexistence with assistant pilot branch.** The backend branch is
   based on `feat/assistant-user-data`, not the archive. If that pilot's
   changes are reverted or rebased, this branch needs replay. Low
   probability; mitigated by branching off a known-good commit.

## 11. Definition of Done

- [ ] `crates/aionui-app/assets/builtin-skills/` contains the full corpus
      with `auto-inject/` as the subdir name
- [ ] `include_dir = "0.7"` added to `crates/aionui-extension/Cargo.toml`
- [ ] `BUILTIN_AUTO_SKILLS_SUBDIR` constant renamed
- [ ] `SkillPaths.builtin_skills_dir` becomes `Option<PathBuf>`;
      `data_dir` field added
- [ ] `read_builtin_skill` / `list_builtin_auto_skills` / `list_skills`
      read from embedded unless env override set
- [ ] `BuiltinAutoSkill` response includes `location` field
- [ ] `POST /api/skills/materialize-for-agent` + `DELETE` endpoints
      registered and working
- [ ] Orphan cleanup runs on startup
- [ ] All tests from §9 green
- [ ] `cargo fmt` / `cargo clippy` clean
- [ ] Cross-repo verification: frontend spec's DoD items can be checked
      against this backend's APIs
