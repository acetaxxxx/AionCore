# Snake-Case Wire Realignment — Design Spec

**Date:** 2026-04-24
**Scope:** Undo the directional mistake of H1 (on `feat/builtin-skills`
branch) and realign the whole builtin-skill pilot surface with the
project-wide snake_case wire convention established on `origin/main` by
commit `dae96f8` ("refactor: remove camelCase serde rename from all
aionui-api-types structs").

**Companion spec (frontend migration + team plan):**
[`AionUi/docs/backend-migration/specs/2026-04-24-snake-case-wire-realignment-design.md`](../../../../AionUi/docs/backend-migration/specs/2026-04-24-snake-case-wire-realignment-design.md)

---

## 1. Root cause

The T3 e2e run on `feat/builtin-skills` initially failed because the
frontend (post-T2) was sending camelCase field names while the backend
default serde behavior emits snake_case. I mis-diagnosed the fix direction
and landed H1 (`04f1537`), which added `#[serde(rename_all = "camelCase")]`
to 16 additional structs in `aionui-api-types/src/skill.rs` (bringing the
total to 21). H1's T3 run-2 went green against a frontend that was also
camelCase.

Meanwhile **`origin/main` had already chosen snake_case as the
project-wide wire convention** in `dae96f8`: "Remove rename_all='camelCase'
from all struct definitions across 18 files… Update all 152+ test
assertions to expect snake_case JSON keys." The main branch was later
merged into `feat/builtin-skills` (`10cd7b0`). The merge commit resolved 4
text conflicts but did not roll back H1's 21 camelCase attributes. So
builtin-skills HEAD carries a camelCase island inside an otherwise
snake_case project.

Worse, the frontend was ALSO merged with `feat/backend-migration` which
followed the snake_case convention (`259505156`). That merge's commit
message literally says "Adopt snake_case field names from origin where the
coordinator branch hadn't touched them." The result: some frontend fields
got correctly snake-cased (`file_name`), others kept camelCase (pilot-new
fields: `conversationId`, `enabledSkills`, `dirPath`, `relativeLocation`,
`isCustom`). The frontend and backend now disagree on exactly the fields
the builtin-skill pilot introduced.

Evidence (live probe against `feat/builtin-skills` HEAD):

| Probe | Result |
|---|---|
| `POST /api/skills/builtin-skill` body `{"file_name":...}` | **400** (backend wants camelCase via H1) |
| `POST /api/skills/builtin-skill` body `{"fileName":...}` | 200 |
| `POST /api/skills/materialize-for-agent` body `{"conversation_id":...}` | **400** |
| `POST /api/skills/materialize-for-agent` body `{"conversationId":...}` | 200 |
| `GET /api/skills` response row keys | `isCustom`, `relativeLocation` (camelCase) |

`dae96f8` on main says this all should be snake_case. Therefore H1 and all
pilot-new fields must be realigned.

## 2. Goals

1. Remove every `#[serde(rename_all = "camelCase")]` from `skill.rs`
   (back to 0, matching `dae96f8` intent).
2. Update the 18 tests that H1 added / flipped (they currently assert
   camelCase; must assert snake_case).
3. Update the 2 types H1 did not originally touch but the pilot introduced
   with camelCase expectations — `MaterializeSkillsRequest`,
   `MaterializeSkillsResponse` (and add `SkillListItemResponse`'s new
   `relative_location` field is serialized snake_case).
4. Realign the frontend: every field introduced by the builtin-skill
   pilot switches to snake_case — `materializeSkillsForAgent`,
   `cleanupSkillsForAgent`, `listBuiltinAutoSkills` response, and
   `listAvailableSkills` response's `relativeLocation` / `isCustom`.
5. Update frontend tests that exercise these fields.
6. Integration verify: `skills_builtin_e2e` + `assistants_e2e` +
   Playwright builtin-skill suite all green.

## 3. Non-Goals

- No changes to routes, handlers, business logic, or database schema.
- No changes to endpoints outside the skill surface.
- No changes to types `BuiltinAutoSkillResponse`'s wire field order or
  behavior beyond snake-casing — the shape is identical, only field
  serialization changes.
- No changes to URL path parameters (`:conversation_id` stays as-is; URL
  path naming is orthogonal to body serde).

## 4. Backend Changes

### 4.1 `crates/aionui-api-types/src/skill.rs`

Remove all 21 occurrences of `#[serde(rename_all = "camelCase")]`.
After removal: `grep -c 'rename_all = "camelCase"' skill.rs` returns 0.

**Resulting wire shapes (same as what `dae96f8` established for all other
structs):**

| Struct | Fields on wire |
|---|---|
| `SkillListItemResponse` | `name, description, location, relative_location, is_custom, source` |
| `ReadSkillInfoRequest` | `skill_path` |
| `ReadSkillInfoResponse` | `name, description` |
| `ImportSkillRequest` | `skill_path` |
| `ImportSkillResponse` | `skill_name` |
| `ExportSkillRequest` | `skill_path, target_dir` |
| `DeleteSkillRequest` | `skill_name` |
| `ScanForSkillsRequest` | `search_dirs, ...` |
| `ScanForSkillsResponse` | `skills, ...` |
| `ScannedSkillResponse` | `name, description, path, source` |
| `ExternalSkillSourceResponse` | `name, path, ...` |
| `NamedPathResponse` | `name, path` |
| `SkillPathsResponse` | `user_skills_dir, builtin_skills_dir` |
| `ReadAssistantRuleRequest` | `assistant_id, locale` |
| `WriteAssistantRuleRequest` | `assistant_id, content, locale` |
| `ReadBuiltinResourceRequest` | `file_name` |
| `AddExternalPathRequest` | `name, path` |
| `RemoveExternalPathRequest` | `name` |
| `BuiltinAutoSkillResponse` | `name, description, location` *(location already snake lowercase)* |
| `MaterializeSkillsRequest` | `conversation_id, enabled_skills` |
| `MaterializeSkillsResponse` | `dir_path` |

### 4.2 Tests

**Flip all 18 H1 tests inside `skill.rs`'s `#[cfg(test)]` module** — currently
they assert camelCase keys. Must assert snake_case. Also flip the regression
guards that reject legacy snake_case (they should instead reject legacy
camelCase now).

Tests to update:

- `test_skill_list_item_serde`
- `test_skill_list_item_deserializes_camel_case` → rename to `..._snake_case`, flip assertions
- `test_skill_list_item_builtin_with_relative_location`
- `test_read_skill_info_request`
- `test_read_skill_info_response_roundtrip` (if present)
- `test_import_skill_request`, `test_import_skill_response`
- `test_read_builtin_resource_request` — flip: now `file_name` is accepted, `fileName` rejected
- `test_read_assistant_rule_request_with_locale`
- `test_read_assistant_rule_request_without_locale`
- `test_write_assistant_rule_request`
- `test_scan_for_skills_request`
- `test_external_skill_source_response_roundtrip`
- `test_skill_paths_response`
- `test_materialize_request_roundtrip`
- `test_materialize_request_default_enabled`
- `test_materialize_response_serializes_camel` → rename to `..._serializes_snake`, flip assertion (`dirPath` → `dir_path`)

### 4.3 Handlers and routes

Handler code does not use serde field names directly — it accesses Rust
fields. No change needed in `skill_routes.rs` or `skill_service.rs`.

### 4.4 Regression tests at the HTTP layer

`crates/aionui-app/tests/skills_builtin_e2e.rs` has 14 tests that use curl-
equivalent JSON payloads. If any of them hard-coded camelCase keys, they
must flip to snake_case. Audit required; backend-dev fixes whatever they
find.

### 4.5 Regression — assistant pilot must stay green

`crates/aionui-app/tests/assistants_e2e.rs` (44 tests) must stay green.
The assistant pilot's own api-types structs (`AssistantResponse` etc.) are
in `assistant.rs`, which already has no `rename_all = "camelCase"` (it
follows the project convention). The only risk is anything dispatched via
skill routes that a assistant test uses; the shape is unchanged.

## 5. Frontend Changes

### 5.1 `src/common/adapter/ipcBridge.ts`

Switch camelCase field names in **pilot-new** skill-surface signatures to
snake_case. Merge-introduced `file_name` on `readBuiltinRule` and
`readBuiltinSkill` is ALREADY snake_case — leave it. Fields to flip:

| Method | Field | was | becomes |
|---|---|---|---|
| `listBuiltinAutoSkills` response | — (response shape) | `{name, description, location}` — location already snake_case, stays |
| `listAvailableSkills` response | `relativeLocation` → `relative_location`, `isCustom` → `is_custom` | camelCase | snake_case |
| `materializeSkillsForAgent` request | `conversationId` → `conversation_id`, `enabledSkills` → `enabled_skills` | camelCase | snake_case |
| `materializeSkillsForAgent` response | `dirPath` → `dir_path` | camelCase | snake_case |
| `cleanupSkillsForAgent` request | `conversationId` stays in TypeScript function-param because it's a URL-path param, NOT a body field. URL path: `/api/skills/materialize-for-agent/:conversation_id` — backend route param naming is backend-internal; frontend just builds the path. But the TS parameter name remains `conversationId` on the Electron side for readability. |

### 5.2 `src/process/task/AcpSkillManager.ts`

Uses `skill.location` and `skill.relativeLocation` / `skill.relative_location`
depending on post-fix shape. Flip usages accordingly. The invoke at
`AcpSkillManager.ts:341` already sends `file_name` — leave it.

### 5.3 `src/process/utils/initAgent.ts` + callers of `materializeSkillsForAgent`

Update the argument shape from `{ conversationId, enabledSkills }` to
`{ conversation_id, enabled_skills }`. Update the response access from
`dirPath` to `dir_path`.

### 5.4 Vitest

- `tests/unit/acpSkillManager.test.ts` — mock `ipcBridge.fs.readBuiltinSkill`
  returns are fine (string); the `listAvailableSkills` mock response must
  now return `relative_location` / `is_custom` (snake_case) for assertions
  to still exercise the code path.
- `tests/unit/initAgent.materialize.test.ts` — mock response `{ dir_path }`;
  mock request body assertions `{ conversation_id, enabled_skills }`.

### 5.5 E2E

`tests/e2e/features/builtin-skill-migration/builtin-skill-migration.e2e.ts`
— audit its payload assertions. Any probe that was written to target
camelCase needs flipping.

## 6. Rollout

Single coordinated commit pair (backend first, frontend second). No
intermediate broken state is exposed to users because no user ships from
these branches directly.

1. backend-dev lands backend changes on `feat/builtin-skills`. Must pass:
   - `cargo test -p aionui-api-types` (the 18 flipped tests)
   - `cargo test --test skills_builtin_e2e`
   - `cargo test --test assistants_e2e`
   - `cargo clippy --workspace -- -D warnings` (no new warnings)
   - `cargo build --release` — refresh `~/.cargo/bin/aionui-backend`
2. frontend-dev lands frontend changes on
   `feat/backend-migration-builtin-skills`. Must pass:
   - `bun run test --run` (baseline unchanged)
   - `bunx tsc --noEmit` clean
   - `bun run lint --quiet` — no new warnings
3. e2e-tester reruns the Playwright suite against the new pair. Must
   report 8/8 green.
4. coordinator updates the coordinator branch merge, writes handoff.

## 7. Risk

- **`AcpSkillManager.discoverSkills` silently falls back to absolute
  path on missing `relativeLocation`.** Post-fix the field name changes;
  if the TypeScript access site isn't updated, skills silently drop. E2E
  catches this via scenario 2 (auto-inject) and 3 (opt-in materialize).
- **The "merge" between 5.1/5.2/5.3 changes requires coordination.** If
  frontend lands before backend, the frontend sends `conversation_id`
  but backend still requires `conversationId` → every materialize call
  fails. Coordinator enforces order via task dependencies.
- **Pre-existing clippy / `cp1_get_external_paths_empty` remain red.**
  Out of scope; same baseline as before.

## 8. Definition of Done

- [ ] `grep -c 'rename_all = "camelCase"' crates/aionui-api-types/src/skill.rs` returns 0
- [ ] All 18+ flipped `#[cfg(test)]` tests in `skill.rs` pass
- [ ] `crates/aionui-app/tests/skills_builtin_e2e.rs` 14/14 green
- [ ] `crates/aionui-app/tests/assistants_e2e.rs` 44/44 green
- [ ] Frontend `grep -E "conversationId|enabledSkills|dirPath|relativeLocation|isCustom" src/common/adapter/ipcBridge.ts` returns 0 hits on skill-related signatures
- [ ] Frontend Vitest baseline unchanged
- [ ] Playwright 8/8 green
- [ ] Coordinator handoff committed
