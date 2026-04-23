# backend-tester Handoff — Assistant User Data Migration — 2026-04-23

**Branch:** `feat/assistant-user-data`
**Base SHA:** T1b `f909a3e` (as reported by coordinator)
**Deliverable:** `crates/aionui-app/tests/assistants_e2e.rs`

## Status

T2 (Backend HTTP integration tests) complete. All endpoints in backend spec §6
probed via `tower::ServiceExt::oneshot` against a full
`aionui_app::create_router_with_states` instance. 44/44 tests green.

## What landed

New file: `crates/aionui-app/tests/assistants_e2e.rs` (44 test functions +
one shared `fixture()` helper). Uses the existing `tests/common/mod.rs`
helpers (`get_with_token`, `json_with_token`, `delete_with_token`,
`setup_and_login`, `body_json`) — no changes to `common` needed.

## Fixture design

Because the production `build_module_states` resolves
`~/.aionui/extension-states.json` and the real built-in-assistants asset
directory (neither appropriate for a hermetic test), the fixture
replaces the `extension`, `hub`, `skill`, and `assistant` module states
after calling `build_module_states`. This gives:

- `BuiltinAssistantRegistry::load_from_dir(<tempdir>/assets)` seeded with
  `builtin-office` (has rule/skill/avatar files on disk) and
  `builtin-bare` (no referenced assets — the 404-path fixture).
- `AssistantService::new(...).with_user_data_dir(<tempdir>)` so user rule /
  skill / avatar CRUD runs inside a throwaway directory.
- A fresh `ExtensionRegistry` initialized via `initialize_with_scan_paths`
  against a `<tempdir>/extensions/fixture-ext/aion-extension.json` manifest
  that contributes an assistant with id `ext-helper` — exercises the real
  extension-classification path without mocks.
- `SkillRouterState` wired to the above `AssistantService` as its
  `AssistantRuleDispatcher`, so `/api/skills/assistant-rule/*` and
  `/api/skills/assistant-skill/*` dispatch through real classify+route
  logic end-to-end.

Auth / CSRF: each fixture logs in via `setup_and_login` and threads the
resulting `(token, csrf)` into every authenticated request (same pattern
as `system_version_e2e.rs`).

## Per-endpoint coverage

Every endpoint from backend spec §6 receives ≥ one happy + one error
test. Total 44 tests; mapping to endpoints below. Error shape = HTTP
status; body shape is verified on happy paths via `body_json(...)`.

### `GET /api/assistants`

| Test | Scenario | Expect |
| --- | --- | --- |
| `list_populated_returns_builtins_and_extension` | Empty DB + 2 builtins + 1 extension | 200, list length 3, sources cover builtin+extension |
| `list_requires_auth` | No Bearer token | 403 (auth_middleware returns Forbidden for missing auth) |

### `POST /api/assistants`

| Test | Scenario | Expect |
| --- | --- | --- |
| `create_happy_path_returns_201` | Valid user row | 201, `source == "user"` |
| `create_rejects_empty_name_with_400` | Whitespace-only `name` | 400 |
| `create_rejects_builtin_id_collision_with_400` | `id = "builtin-office"` | 400 |
| `create_rejects_extension_id_collision_with_400` | `id = "ext-helper"` | 400 |
| `create_rejects_duplicate_user_id_with_409` | Same id twice | 1st 201, 2nd 409 |

### `PUT /api/assistants/{id}`

| Test | Scenario | Expect |
| --- | --- | --- |
| `update_happy_path_returns_200` | Rename existing user row | 200, new name reflected |
| `update_missing_user_returns_404` | Unknown id | 404 |
| `update_builtin_is_forbidden` | `id = "builtin-office"` | 403 |
| `update_extension_is_forbidden` | `id = "ext-helper"` | 403 |

### `DELETE /api/assistants/{id}`

| Test | Scenario | Expect |
| --- | --- | --- |
| `delete_happy_path_removes_row_and_user_assets` | User row + on-disk rule/skill/avatar files | 200, row gone from list, all three files deleted |
| `delete_builtin_is_forbidden` | `id = "builtin-office"` | 403 |
| `delete_extension_is_forbidden` | `id = "ext-helper"` | 403 |

### `PATCH /api/assistants/{id}/state`

| Test | Scenario | Expect |
| --- | --- | --- |
| `set_state_inserts_override_for_builtin` | First-time override on builtin | 200, enabled=false, sortOrder=9, source=builtin |
| `set_state_updates_existing_override_for_user` | Two successive patches | 2nd call preserves `enabled` when omitted |
| `set_state_extension_is_400` | `id = "ext-helper"` | 400 (extension is read-only) |
| `set_state_unknown_user_returns_404` | User id not present | 404 |

### `POST /api/assistants/import`

| Test | Scenario | Expect |
| --- | --- | --- |
| `import_happy_path_inserts_new_rows` | 2 fresh rows | 200, imported=2 |
| `import_skips_builtin_collision` | `id = "builtin-office"` | 200, skipped=1 |
| `import_skips_extension_collision` | `id = "ext-helper"` | 200, skipped=1 |
| `import_skips_already_imported_user_row` | User row pre-created with name "A"; re-import with "A-updated" | 200, skipped=1, row retains "A" |
| `import_retry_is_idempotent` | Same payload twice | 1st imported=1, 2nd imported=0 skipped=1 |

### `GET /api/assistants/{id}/avatar`

| Test | Scenario | Expect |
| --- | --- | --- |
| `avatar_builtin_returns_bytes_with_content_type` | builtin-office → `office.png` on disk | 200, Content-Type `image/png`, raw bytes match |
| `avatar_user_returns_bytes_after_file_planted` | User row + `u1.svg` planted | 200, Content-Type `image/svg+xml` |
| `avatar_missing_returns_404` | builtin-bare (no avatar in manifest) | 404 |

### `POST /api/skills/assistant-rule/read|write` + `DELETE /api/skills/assistant-rule/{id}`

| Test | Scenario | Expect |
| --- | --- | --- |
| `read_rule_builtin_returns_manifest_file_contents` | `assistantId=builtin-office, locale=en-US` | 200, data = `"office rule body"` |
| `read_rule_extension_returns_empty_string` | `assistantId=ext-helper` | 200, data = `""` (spec §6.4) |
| `read_rule_user_round_trip_through_write` | write then read | 200, data matches written content |
| `write_rule_user_happy_path` | user id + content | 200, file present on disk |
| `write_rule_builtin_returns_400` | builtin id | 400 |
| `write_rule_extension_returns_400` | extension id | 400 |
| `delete_rule_user_removes_file` | file planted, then DELETE | 200, file gone |
| `delete_rule_builtin_returns_400` | builtin id | 400 |
| `delete_rule_extension_returns_400` | extension id | 400 |

### `POST /api/skills/assistant-skill/read|write` + `DELETE /api/skills/assistant-skill/{id}`

Parallel structure to the rule endpoints.

| Test | Scenario | Expect |
| --- | --- | --- |
| `read_skill_builtin_returns_manifest_file_contents` | builtin-office en-US | 200, `"office skill body"` |
| `read_skill_extension_returns_empty_string` | ext-helper | 200, `""` |
| `read_skill_user_round_trip_through_write` | write zh-CN then read zh-CN | 200, content matches |
| `write_skill_user_happy_path` | user id | 200, file present |
| `write_skill_builtin_returns_400` | builtin | 400 |
| `write_skill_extension_returns_400` | extension | 400 |
| `delete_skill_user_removes_file` | file planted, then DELETE | 200, file gone |
| `delete_skill_builtin_returns_400` | builtin | 400 |
| `delete_skill_extension_returns_400` | extension | 400 |

## Probe transcript

```bash
$ cd /Users/zhoukai/Documents/github/aionui-backend
$ git rev-parse HEAD
20d9f69…  # ahead of T1b f909a3e by one docs commit (development-workflow.md)

$ cargo test --workspace --no-run
  Finished `test` profile [unoptimized + debuginfo]
  (all test binaries linked, no errors)

$ cargo test --test assistants_e2e -- --nocapture
  running 44 tests
  test result: FAILED. 0 passed; 44 failed (first run — stale
    ~/.aionui/extension-states.json poisoned the extension registry)

# Root cause: `build_module_states` resolves
# `~/.aionui/extension-states.json`, which on this dev box contains a
# legacy `{version, extensions}` shape while the current loader expects
# a `Vec<ExtensionState>`. The parser returns
# `JsonParse("invalid type: map, expected a sequence")`, which
# `initialize_with_scan_paths` bubbles up. In production this does not
# happen because the file is written freshly by the current version
# before any read — but a test that triggers `initialize…` against the
# real home dir trips on the stale file.
#
# Fix in the test (not in production code): rebuild `extension`, `hub`,
# `skill` router states with a pristine `ExtensionStateStore` rooted at
# a temp dir. Result:

$ cargo test --test assistants_e2e -- --nocapture
  running 44 tests
  test result: ok. 44 passed; 0 failed; 0 ignored; finished in 7.85s

$ cargo fmt --all -- --check
  (clean — no diffs)

$ cargo clippy -p aionui-app --tests --test assistants_e2e -- -D warnings
  (no warnings originating in assistants_e2e.rs; pre-existing workspace
   lints in aionui-office/snapshot.rs and
   aionui-channel/plugins/weixin/login.rs are out of scope per T2 gate)
```

## Cross-platform validation (spec §12 DoD)

### macOS (this workstation) — PASS

```bash
$ cargo build --release
  Finished `release` profile [optimized]

$ /Users/zhoukai/Documents/github/aionui-backend/target/release/aionui-backend \
    --local --port 25900 --data-dir /tmp/aionui-probe
  INFO aionui_backend: Server listening on 127.0.0.1:25900

$ curl -s http://127.0.0.1:25900/api/assistants | jq '.data | length'
  20

$ curl -s http://127.0.0.1:25900/api/assistants | jq '[.data[].source] | unique'
  ["builtin"]
```

Built-in assistants load from the packaged `assets/builtin-assistants/`
correctly (20 entries vs. the ≥ 2 gate in plan §2.4).

### Linux / Windows — DEFERRED

No CI runner access from this workstation. Per plan §2.4 fallback,
notifying coordinator so they can either schedule an L/W probe or scope
it as a follow-up. Same probe command + curl will be enough — no test
changes anticipated since the test uses `TempDir` and
`BuiltinAssistantRegistry::load_from_dir`, both of which are
cross-platform.

## Known gaps / open questions

1. **No upload-avatar coverage.** Backend spec §6.1 lists
   `POST /api/assistants/{id}/avatar` but `assistant_routes()` currently
   mounts only `GET` for that path. Plan §2.2 reflects this — no upload
   test requested. Flagging so that if upload lands in a follow-up, a
   new test is added alongside it.
2. **Extension dispatch for read-rule / read-skill returns empty.** The
   service impl in `aionui-assistant/src/service.rs:449` explicitly
   returns `Ok(String::new())` for `AssistantSource::Extension`, with
   the comment *"ResolvedAssistant doesn't expose rule content directly
   in the current backend; return empty until extension schema gains
   this field"*. The tests assert the current empty-string behaviour
   (`read_rule_extension_returns_empty_string`,
   `read_skill_extension_returns_empty_string`). When the extension
   schema eventually surfaces rule/skill content, these assertions
   will need to be tightened alongside the service change.
3. **The fixture drifts from the production wiring path.** Because
   `build_module_states` is not test-friendly (it hardcodes
   `~/.aionui/` and resolves the system built-in-assistants directory),
   the fixture in `assistants_e2e.rs` substitutes four states after the
   fact. This is a test-only concern, but if someone adds a new
   domain crate whose default state depends on `~/.aionui/` they
   should expect similar contortions. Suggest tracking a cleanup task
   to make `build_module_states` accept a `data_dir` override so tests
   can go through it directly.

## Definition of Done — checklist

- [x] All endpoint probes green (44/44)
- [x] Handoff committed with probe transcript + per-endpoint pass/fail
      summary
- [x] macOS runtime probe recorded
- [x] SHA will be reported to coordinator via SendMessage after commit
- [ ] L/W probe — deferred to coordinator (no CI runner access)

## For the e2e-tester (T5)

- Use the same `create_router_with_states` override pattern if you need
  to short-circuit `~/.aionui/` reads during Playwright E2E.
- The integration tests here exercise the HTTP layer only (fake
  auth/CSRF via `tower::oneshot`). Full E2E still needs to verify the
  Electron main process migration hook + renderer IPC calls arrive at
  these endpoints with correct shapes — what T5 owns.
- Known shape check: `AssistantResponse` uses camelCase for
  `presetAgentType`, `sortOrder`, `lastUsedAt`, `nameI18n`, etc. (see
  `aionui-api-types/src/assistant.rs`). Frontend types in T3a already
  mirror this.
