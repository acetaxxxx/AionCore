# backend-dev Handoff — Skill-Library — 2026-04-22

**Branch:** `feat/extension-skill-library`
**Last commit:** `686e855` (spec alignment). E1–E5 implementation/test commits
listed in "Done".

## Done

Tasks 1 and 2 of the pilot plan are complete. Every endpoint E1–E5 is
wired, tested at the HTTP level, and documented.

| Step  | Commit    | Subject                                                             |
| ----- | --------- | ------------------------------------------------------------------- |
| 1.3   | `b2e3c9f` | docs(extension): draft Skill Library API spec for pilot migration   |
| 2.2   | `75ab3f1` | feat(extension/skills): add source field to GET /api/skills         |
| 2.3   | `95ab84c` | feat(extension/skills): implement GET /api/skills/builtin-auto      |
| 2.4   | `5da1b87` | test(extension/skills): HTTP tests for POST /api/skills/builtin-rule|
| 2.5   | `358c364` | test(extension/skills): HTTP tests for POST /api/skills/builtin-skill|
| 2.6   | `ac1d2dc` | test(extension/skills): HTTP tests for POST /api/skills/info        |
| 2.7   | `686e855` | docs(extension): align Skill Library spec with implementation       |

Green at the last pass:

- `cargo test -p aionui-api-types --lib` → 405 passed
- `cargo test -p aionui-extension --lib` → 325 passed (+4 new: source
  field assertions, 2× builtin-auto)
- `cargo test -p aionui-app --test extension_e2e` → 39 passed (+11 new:
  `sl1–sl3`, `ba1–ba3`, `rm2–rm3`, `sk2–sk3`, `si1–si3`)

### Scope breakdown — new vs. adapted (for successor calibration)

Back-of-the-envelope for the next module's backend-dev: roughly 1/5
endpoints was net-new code, 1/5 required a shape change, 3/5 needed
only test supplementation. Full wall-clock for the pilot (once the
toolchain was installed) was dominated by test authorship and HTTP
integration plumbing, not by business logic.

| ID | Category          | What was already there                                        | What Task 2 added                                                                              |
| -- | ----------------- | ------------------------------------------------------------- | ---------------------------------------------------------------------------------------------- |
| E1 | **Adapted**       | `list_skills` route + `list_available_skills` service, minus `source` | `SkillSource` enum + `SkillSourceResponse` DTO + field population + 3 HTTP tests + 1 extended unit test |
| E2 | **Net-new**       | nothing — route, service, DTO, constant all absent             | Full vertical slice: constant `BUILTIN_AUTO_SKILLS_SUBDIR`, `BuiltinAutoSkillItem`, `list_builtin_auto_skills`, `BuiltinAutoSkillResponse`, route mount, 2 unit tests, 3 HTTP tests |
| E3 | **Adapted (test only)** | Route + service + unit tests all green                  | 2 HTTP tests (`rm2` happy path, `rm3` traversal) — existing `rm1` covered file-not-found       |
| E4 | **Adapted (test only)** | Route + service + unit tests all green                  | 3 HTTP tests (`sk1`–`sk3`); confirmed via renderer grep that `validate_filename` strictness is correct (no preset sends nested paths) |
| E5 | **Adapted (test only)** | Route + service + unit tests all green                  | 3 HTTP tests (`si1` happy path, `si2` empty-`name` → dir basename fallback, `si3` 404 missing path) |

Notes for strict TDD bookkeeping:

- **E1 and E2 followed a real red→green loop** (wrote failing unit test
  asserting missing `source` field and `list_builtin_auto_skills`
  respectively → saw FAIL → implemented → saw PASS).
- **E3/E4/E5 did not have a red stage** because the implementations were
  already correct. I still wrote net-new HTTP tests that exercise the
  wire contract end-to-end (auth → handler → service → response), but
  calling this "strict TDD" would be inaccurate — it is better
  characterised as contract-locking test supplementation. Flagging
  this as a deviation from Step 2.2–2.6's wording for transparency.

### Contract highlights (for frontend-dev / e2e-tester)

- `GET /api/skills` → `ApiResponse<SkillListItem[]>` where
  `SkillListItem.source: 'builtin' | 'custom' | 'extension'`. Pilot only
  emits `'builtin'` / `'custom'`; `'extension'` is reserved for a future
  milestone when `ExtensionRegistry` contribution resolution lands on the
  Rust side.
- `GET /api/skills/builtin-auto` → `ApiResponse<BuiltinAutoSkill[]>`
  (`{ name, description }`). Scans `<builtin_skills_dir>/_builtin/`. Empty
  array when `_builtin/` is missing.
- `POST /api/skills/builtin-rule` and `POST /api/skills/builtin-skill` →
  `ApiResponse<string>`. Missing file = empty string (graceful); path
  separators or empty fileName = 400.
- `POST /api/skills/info` → `ApiResponse<{ name, description }>`. Empty
  `name` in frontmatter falls back to directory basename. Missing path = 404.

All routes sit behind auth middleware; unauthenticated requests get 403
(matches other `/api/*` routes).

### Files changed

- `crates/aionui-api-types/src/skill.rs` — added `SkillSourceResponse`,
  `BuiltinAutoSkillResponse`; extended `SkillListItemResponse`.
- `crates/aionui-api-types/src/lib.rs` — re-exported the new types.
- `crates/aionui-extension/src/constants.rs` — added
  `BUILTIN_AUTO_SKILLS_SUBDIR = "_builtin"`.
- `crates/aionui-extension/src/skill_service.rs` — added `SkillSource`
  enum, `source` field on `SkillListItem`, `BuiltinAutoSkillItem` struct,
  `list_builtin_auto_skills` function, 3 new unit tests.
- `crates/aionui-extension/src/skill_routes.rs` — mounted `/api/skills/builtin-auto`,
  wired source-field mapping in `list_skills`.
- `crates/aionui-extension/src/lib.rs` — re-exported new symbols.
- `crates/aionui-app/tests/common/mod.rs` — added `build_app_with_skill_paths`
  helper that isolates skill I/O to a TempDir.
- `crates/aionui-app/tests/extension_e2e.rs` — added SL / BA / SK / SI
  test sections; supplemented RM with happy-path and traversal cases.
- `docs/api-spec/13-extension.md` — added `## Skill Library` section with
  the E1–E5 contract, source-of-truth table, error matrix, and
  delta-resolution log.

### Key TS baseline findings

Coordinator already has these via prior SendMessage; listing here for
posterity.

- HTTP handlers no longer live in `src/process/bridge/` — that migration
  was already done. Baseline semantics are now read from the
  `ipcBridge.ts` type signatures and from
  `src/process/extensions/resolvers/*` + `src/process/utils/initStorage.ts`.
- Built-in skills live under `src/process/resources/skills/`; the
  `_builtin/` subdirectory is auto-injected into every assistant (see
  `initStorage.ts::getBuiltinAutoSkillsDir`). Current contents: `cron/`,
  `aionui-skills/`, `office-cli/`, `skill-creator/`.
- Preset `ruleFile`/`skillFile` values (`src/common/config/presets/assistantPresets.ts`)
  are always flat filenames, never nested paths — confirmed by grep.

## In flight

None. All five endpoints green; docs updated; handoff committed.

## Known issues / open questions

- **Extension-contributed skills not yet in `GET /api/skills`.** The Rust
  `ExtensionRegistry` (in `aionui-extension/src/registry.rs`) exists but
  is not yet wired into `skill_service::list_available_skills`. Once it is,
  those entries should emit `source: 'extension'`. Out of pilot scope per
  the plan.
- **Non-hermetic `build_app()`-based tests (`rm1`, `sk1`, `rm3`, `sk3`,
  `ba3`) read the developer's real `$HOME/.aionui/` layout** because they
  use the default `SkillRouterState`. They are deliberately wired this way
  so they keep working against production paths; each asserts only
  branch-independent invariants (auth, traversal rejection, file-not-found
  fallback). Tests that need seeded skill fixtures use
  `build_app_with_skill_paths` against a `TempDir` instead.
- **No Rust toolchain on coordinator's machine at start.** I installed
  `rustup` via Homebrew (stable 1.95.0), so subsequent Rust-side agents
  should already have cargo on `PATH` via `/opt/homebrew/opt/rustup/bin`.
  If `cargo` is missing in their shell, they may need to `export
  PATH="/opt/homebrew/opt/rustup/bin:$PATH"` first.

## Next steps for a successor

If another backend-dev takes over before the pilot closes:

1. **Frontend-dev may report regressions** via incident files under
   `docs/backend-migration/incidents/` or via SendMessage. Follow the
   plan's loop-handling (Step 4.6): one atomic fix per commit, tests
   first, message the reporter when green.
2. **If E2 `/_builtin/` scanning is ever expanded** to descend into
   nested subdirectories, update `scan_skill_dirs` instead of the route
   — the scan helper is shared with E1 and extension discovery.
3. **When `ExtensionRegistry` contribution resolution is ported**, merge
   its resolved skills into `list_available_skills` with
   `source: SkillSource::Extension`. Extend unit test
   `list_skills_builtin_and_custom` with an `Extension` case.
4. **Do NOT merge `feat/extension-skill-library` into `main`** — per the
   pilot plan, base-branch integration is explicitly deferred until
   after the pilot closes and the coordinator schedules a separate
   user-approved integration step.

---

## Rerun fix — 2026-04-22 (post e2e rerun)

### The gap

During e2e-tester-2's full-suite rerun (17 pass / 12 fail), one failure
was a real backend contract gap rather than a test-authoring issue:

`ExternalSkillSourceResponse` in
`crates/aionui-api-types/src/skill.rs` lacked a machine-readable
`source` field. The renderer at
`src/renderer/pages/settings/SkillsHubSettings.tsx:289` consumes
`source.source` as both a React key and a `data-testid` suffix
(`external-source-tab-${source}`). With the field missing, every tab
rendered with `data-testid="external-source-tab-undefined"`, Playwright
strict-mode found two matches, and **TC-S-09, TC-S-10, TC-S-12,
TC-S-14, and TC-S-16** all failed at the first tab interaction.

### The fix

Single atomic commit — **`3a86d58`** on `feat/extension-skill-library`:

| Layer                                            | Change                                                                                                                    |
| ------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------- |
| `crates/aionui-api-types/src/skill.rs`           | Added `pub source: String` to `ExternalSkillSourceResponse` (camelCase on the wire) + 3 unit tests (shape, custom, roundtrip). |
| `crates/aionui-extension/src/constants.rs`       | Extended `COMMON_SKILL_DIRS` from `(name, rel_path)` to `(name, rel_path, source_slug)`: `claude`, `gemini`, `agents`.   |
| `crates/aionui-extension/src/skill_service.rs`   | Added `source: String` to `ExternalSkillSource` + rewrote `detect_and_count_external_skills` to iterate `COMMON_SKILL_DIRS` directly (so the slug survives). Custom paths now emit `format!("custom-{path}")`. |
| `crates/aionui-extension/src/skill_routes.rs`    | Mapped `source` through to the HTTP DTO.                                                                                 |
| `crates/aionui-extension/tests/skill_integration_test.rs` | Extended `detect_external_skills_from_custom_paths` + new `detect_external_skills_custom_sources_are_unique`.        |
| `crates/aionui-app/tests/extension_e2e.rs`       | Added HTTP e2e tests `de1_detect_external_populates_custom_source_slug` and `de2_detect_external_source_slugs_are_unique` driving the full stack through `/api/skills/external-paths` + `/api/skills/detect-external`. |

Slug convention matches the pre-migration TS handler (reconstructed from
commit `0a00e937e:src/process/bridge/fsBridge.ts`) and satisfies the
`external-source-tab-custom-` prefix assertion in
`tests/e2e/features/settings/skills/edge-cases.e2e.ts:74`.

### Verification

- `cargo test -p aionui-api-types -p aionui-extension` → 911 passed (17 suites).
- `cargo test -p aionui-app --test extension_e2e -- de1 de2 sl1 sl2 sl3` → 5 passed.
- `cargo fmt --all -- --check` → clean.
- `cargo clippy` has pre-existing errors in `snapshot.rs`,
  `conversation.rs`, `lifecycle.rs`, and `handler_integration.rs`; none
  in files touched by this fix. Verified these same errors exist on
  the stashed (clean) tree.
- Release binary rebuilt and reinstalled at
  `~/.cargo/bin/aionui-backend` via `cargo install --path crates/aionui-app --locked`
  so the next renderer e2e run hits the fixed contract.

Commit SHA: **`3a86d58`**. Pushed to `origin/feat/extension-skill-library`.
