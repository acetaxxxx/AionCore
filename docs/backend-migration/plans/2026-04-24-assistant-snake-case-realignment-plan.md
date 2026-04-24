# Assistant Snake-Case Realignment — Implementation Plan

> **Team mode** — coordinator + parallel teammates. Three scope-slices: assistant bulk rename (heavy) + 2 hotfixes (light, ACP + fs-temp).
>
> **Companion specs:**
> - [`aionui-backend/docs/backend-migration/specs/2026-04-24-assistant-snake-case-realignment-design.md`](../specs/2026-04-24-assistant-snake-case-realignment-design.md) — authoritative contract
> - [`AionUi/docs/backend-migration/specs/2026-04-24-assistant-snake-case-realignment-design.md`](../../../../AionUi/docs/backend-migration/specs/2026-04-24-assistant-snake-case-realignment-design.md) — frontend companion

**Goal:** Scrub the last camelCase residues from the wire surface —
7 `rename_all = "camelCase"` on `aionui-api-types/src/assistant.rs`,
1 on `aionui-assistant/src/builtin.rs`, 20 entries × ~10 keys each in
`assets/builtin-assistants/assistants.json`, ~209 frontend access
sites across 43 files in the `Assistant` type, plus 2 drive-by
frontend hotfixes (ACP `setModel`/`setConfigOption`,
FS `createTempFile`/`createUploadFile`).

**Team size:** 1 coordinator + 3 role-teammates
(backend-dev, frontend-dev, e2e-tester).

**Tech Stack:** Rust + axum + serde + sqlx (backend); TypeScript +
Electron + Vitest + Playwright (frontend); `jq` for JSON rewriting;
`ts-morph` for TS codemod.

---

## Branches

| Branch | Repo | Base | Owner(s) |
| --- | --- | --- | --- |
| `feat/backend-migration-coordinator-assistant-camel` | aionui-backend | `origin/feat/builtin-skills` @ `8414318` | coordinator |
| `feat/backend-migration-coordinator-assistant-camel` | AionUi | `origin/feat/backend-migration-coordinator` @ `460259c9d` | coordinator |
| `feat/assistant-snake-case` | aionui-backend | coord branch above | backend-dev |
| `feat/assistant-snake-case` | AionUi | coord branch above | frontend-dev |
| `fix/acp-camelcase-hotfix` | AionUi | coord branch above | frontend-dev |
| `fix/fs-temp-camelcase-hotfix` | AionUi | coord branch above | frontend-dev |

The two coordinator branches (one per repo) already exist and hold
the spec commits (`8414318` backend, `460259c9d` AionUi). Teammates
branch off them. At T4, coordinator merges three AionUi feature
branches back into `feat/backend-migration-coordinator-assistant-camel`
and finally cherry-picks or fast-forwards back to
`feat/backend-migration-coordinator` on AionUi main-line.

### Worktree locations (coordinator)

- Backend: `/Users/zhoukai/Documents/worktrees/aionui-backend-assistant-camel`
- AionUi: `/Users/zhoukai/Documents/worktrees/aionui-assistant-camel`

Teammates work from these worktrees directly (they `cd` in); they do
not create their own worktrees.

---

## Task graph

```
T0 (coordinator setup — already done: worktrees + spec commits)
 │
 ├────────────────────────┬──────────────────────┐
 ▼                        ▼                      ▼
T1 backend-dev            T2b frontend-dev       T2c frontend-dev
(feat/assistant-          (fix/acp-camel-        (fix/fs-temp-
 snake-case on             case-hotfix)           camelcase-hotfix)
 aionui-backend)
 │
 ▼
T2a frontend-dev
(feat/assistant-snake-case on AionUi, depends on T1)
 │
 ▼
T3 e2e-tester (Playwright, assistants_e2e, skills_builtin_e2e —
               depends on T1 + T2a + T2b + T2c)
 │
 ▼
T4 coordinator closure (merge 3 AionUi branches into coord branch;
                         packaging smoke; handoff)
```

**Critical path**: T0 → T1 → T2a → T3 → T4. T2b + T2c fit inside the
T1/T2a window without extending the critical path.

---

## T0: Coordinator setup (DONE)

**Files:**
- Create: `aionui-backend/docs/backend-migration/specs/2026-04-24-assistant-snake-case-realignment-design.md` ✅ `8414318`
- Create: `AionUi/docs/backend-migration/specs/2026-04-24-assistant-snake-case-realignment-design.md` ✅ `460259c9d`
- Create worktree `aionui-backend-assistant-camel` on backend ✅
- Create worktree `aionui-assistant-camel` on AionUi ✅

Already done by the time this plan was written.

---

## T1: Backend changes — `feat/assistant-snake-case` on aionui-backend

**Owner:** backend-dev
**Worktree:** `/Users/zhoukai/Documents/worktrees/aionui-backend-assistant-camel`
**Branch to create:** `feat/assistant-snake-case` off current HEAD (`8414318`)

**Files:**
- Modify: `crates/aionui-api-types/src/assistant.rs` (remove 7 rename_all; flip 6 inline tests; add 1 regression test)
- Modify: `crates/aionui-assistant/src/builtin.rs:34` (remove 1 rename_all)
- Modify: `crates/aionui-app/assets/builtin-assistants/assistants.json` (jq walk rewrite)
- Modify: `crates/aionui-app/tests/assistants_e2e.rs` (flip 9 JSON keys at lines 97, 105, 517, 525, 537, 547, 555)

### Steps

- [ ] **Step T1.1: Create branch + verify baseline**

Run:
```bash
cd /Users/zhoukai/Documents/worktrees/aionui-backend-assistant-camel
git checkout -b feat/assistant-snake-case
git log --oneline -1
# expect: 8414318 docs(backend-migration): add assistant snake-case realignment spec

# Baseline sanity — tests should be green before our changes
cargo test -p aionui-api-types --lib 2>&1 | tail -5
cargo test --test assistants_e2e 2>&1 | tail -5
cargo test --test skills_builtin_e2e 2>&1 | tail -5
```
Expected: all green. Record baseline test counts.

- [ ] **Step T1.2: Remove 7 rename_all from `assistant.rs`**

File: `crates/aionui-api-types/src/assistant.rs`

Delete line-by-line these 7 attribute lines:
```rust
#[serde(rename_all = "camelCase")]  // line 26 (before AssistantResponse)
#[serde(rename_all = "camelCase")]  // line 68 (before CreateAssistantRequest)
#[serde(rename_all = "camelCase")]  // line 99 (before UpdateAssistantRequest)
#[serde(rename_all = "camelCase")]  // line 129 (before SetAssistantStateRequest)
#[serde(rename_all = "camelCase")]  // line 142 (before ImportAssistantsRequest)
#[serde(rename_all = "camelCase")]  // line 149 (before ImportAssistantsResult)
#[serde(rename_all = "camelCase")]  // line 160 (before ImportError)
```

**Keep** line 16's `#[serde(rename_all = "lowercase")]` — that's on the
`AssistantSource` enum for lowercase variant names, unrelated.

Verify:
```bash
grep -c 'rename_all = "camelCase"' crates/aionui-api-types/src/assistant.rs
# expect: 0
grep -c 'rename_all = "lowercase"' crates/aionui-api-types/src/assistant.rs
# expect: 1
```

- [ ] **Step T1.3: Flip inline unit tests**

File: `crates/aionui-api-types/src/assistant.rs` → `#[cfg(test)] mod tests`

Rename tests + flip assertions:

```rust
// BEFORE — line 171
#[test]
fn assistant_source_camel_case_serializes_lowercase() {
    // ... (no camelCase in body, just misleading name)
}

// AFTER
#[test]
fn assistant_source_serializes_lowercase() {
    let json = serde_json::to_string(&AssistantSource::Builtin).unwrap();
    assert_eq!(json, "\"builtin\"");
    let json = serde_json::to_string(&AssistantSource::User).unwrap();
    assert_eq!(json, "\"user\"");
    let json = serde_json::to_string(&AssistantSource::Extension).unwrap();
    assert_eq!(json, "\"extension\"");
}
```

```rust
// BEFORE — line 181
#[test]
fn assistant_response_round_trip_camel_case() {
    let resp = AssistantResponse { /* ... */ };
    let json = serde_json::to_value(&resp).unwrap();
    assert_eq!(json["presetAgentType"], "gemini");
    assert_eq!(json["sortOrder"], 5);
    assert_eq!(json["lastUsedAt"], 1234);
}

// AFTER
#[test]
fn assistant_response_round_trip_snake_case() {
    let resp = AssistantResponse {
        id: "a1".into(),
        source: AssistantSource::User,
        name: "Name".into(),
        name_i18n: HashMap::new(),
        description: None,
        description_i18n: HashMap::new(),
        avatar: None,
        enabled: true,
        sort_order: 5,
        preset_agent_type: "gemini".into(),
        enabled_skills: vec![],
        custom_skill_names: vec![],
        disabled_builtin_skills: vec![],
        context: None,
        context_i18n: HashMap::new(),
        prompts: vec![],
        prompts_i18n: HashMap::new(),
        models: vec![],
        last_used_at: Some(1_234),
    };

    let json = serde_json::to_value(&resp).unwrap();
    assert_eq!(json["preset_agent_type"], "gemini");
    assert_eq!(json["sort_order"], 5);
    assert_eq!(json["last_used_at"], 1234);
}
```

The remaining 4 tests (`create_assistant_request_accepts_minimal_body`,
`update_assistant_request_supports_partial`, `set_state_request_all_optional`,
`import_result_default_is_zeroes`) contain no camelCase keys — leave untouched.

- [ ] **Step T1.4: Add regression test**

Append at the bottom of `#[cfg(test)] mod tests`:

```rust
#[test]
fn assistant_response_rejects_camel_case() {
    // Regression: after T1 this pilot removes rename_all = "camelCase".
    // Ensure a camelCase body does NOT get aliased into snake fields.
    let json = serde_json::json!({
        "id": "a1",
        "source": "user",
        "name": "X",
        "presetAgentType": "gemini",  // ← legacy camelCase — should be ignored
        "enabled": true,
        "sortOrder": 99,              // ← legacy camelCase — should be ignored
    });
    let resp: AssistantResponse = serde_json::from_value(json).unwrap();
    assert_eq!(resp.preset_agent_type, "", "presetAgentType must NOT alias into preset_agent_type");
    assert_eq!(resp.sort_order, 0, "sortOrder must NOT alias into sort_order");
}
```

- [ ] **Step T1.5: Run api-types tests, verify all green**

Run:
```bash
cargo test -p aionui-api-types --lib 2>&1 | tail -10
```
Expected: all green, including `assistant_response_round_trip_snake_case`
and `assistant_response_rejects_camel_case`.

If any other api-types tests (in other files e.g. `conversation.rs`)
break: do NOT silently modify; investigate — likely means assistant
types were imported elsewhere with camelCase expectation.

- [ ] **Step T1.6: Remove rename_all from `builtin.rs`**

File: `crates/aionui-assistant/src/builtin.rs` line 34

Delete:
```rust
#[serde(rename_all = "camelCase")]
pub struct BuiltinAssistant {
```

Become:
```rust
pub struct BuiltinAssistant {
```

Verify:
```bash
grep -c 'rename_all = "camelCase"' crates/aionui-assistant/src/builtin.rs
# expect: 0
```

At this point `cargo test -p aionui-assistant` will fail because
`assistants.json` still has camelCase keys. That's the next step.

- [ ] **Step T1.7: Rewrite `assistants.json` with jq**

File: `crates/aionui-app/assets/builtin-assistants/assistants.json`

Run (from repo root, which for backend-dev is the worktree):
```bash
jq '
walk(
  if type == "object" then
    with_entries(
      .key |= (
        if . == "nameI18n" then "name_i18n"
        elif . == "descriptionI18n" then "description_i18n"
        elif . == "presetAgentType" then "preset_agent_type"
        elif . == "enabledSkills" then "enabled_skills"
        elif . == "customSkillNames" then "custom_skill_names"
        elif . == "disabledBuiltinSkills" then "disabled_builtin_skills"
        elif . == "ruleFile" then "rule_file"
        elif . == "skillFile" then "skill_file"
        elif . == "promptsI18n" then "prompts_i18n"
        else . end
      )
    )
  else . end
)
' crates/aionui-app/assets/builtin-assistants/assistants.json > /tmp/assistants.json.new \
  && mv /tmp/assistants.json.new crates/aionui-app/assets/builtin-assistants/assistants.json
```

Note: `walk` is a standard jq builtin in jq 1.6+; if your jq is older,
install via `brew install jq`.

Verify:
```bash
grep -cE '"(presetAgentType|nameI18n|descriptionI18n|enabledSkills|customSkillNames|disabledBuiltinSkills|promptsI18n|ruleFile|skillFile)"' \
  crates/aionui-app/assets/builtin-assistants/assistants.json
# expect: 0

grep -c '"preset_agent_type"' crates/aionui-app/assets/builtin-assistants/assistants.json
# expect: 20 (one per assistant)

jq '.assistants | length' crates/aionui-app/assets/builtin-assistants/assistants.json
# expect: 20 (must not lose any entries)

jq '.' crates/aionui-app/assets/builtin-assistants/assistants.json > /dev/null
# expect: exit 0 (must remain valid JSON)
```

- [ ] **Step T1.8: Run aionui-assistant tests**

Run:
```bash
cargo test -p aionui-assistant 2>&1 | tail -10
```
Expected: all green (builtin loader parses the snake_case JSON).

- [ ] **Step T1.9: Flip hardcoded JSON keys in `assistants_e2e.rs`**

File: `crates/aionui-app/tests/assistants_e2e.rs`

Specific edits (line numbers approximate — follow the pattern):

1. Lines ~97 and ~105 — `serde_json::json!({...})` request bodies:
   - `"presetAgentType": "gemini"` → `"preset_agent_type": "gemini"`
2. Lines ~517, 525, 537, 547, 555 — `SetAssistantStateRequest` bodies
   and response key assertions:
   - `"sortOrder": 9` → `"sort_order": 9`
   - `json["data"]["sortOrder"]` → `json["data"]["sort_order"]`
   - `"sortOrder": 3` → `"sort_order": 3`
   - `"sortOrder": 7` → `"sort_order": 7`
   - `json["data"]["sortOrder"]` → `json["data"]["sort_order"]`

Full regex sweep to catch any missed:
```bash
grep -nE '"(presetAgentType|sortOrder|nameI18n|descriptionI18n|enabledSkills|customSkillNames|disabledBuiltinSkills|promptsI18n|lastUsedAt)"' \
  crates/aionui-app/tests/assistants_e2e.rs
```
After flipping all: this grep returns 0 lines.

- [ ] **Step T1.10: Run assistants_e2e, verify 44/44 green**

Run:
```bash
cargo test --test assistants_e2e 2>&1 | tail -10
```
Expected: all 44 tests green.

If a test fails with "missing field" or "unknown variant": the body
you flipped may have a key you didn't catch. Grep for the remaining
camelCase token in that test and fix.

- [ ] **Step T1.11: Run skills_builtin_e2e as regression**

Run:
```bash
cargo test --test skills_builtin_e2e 2>&1 | tail -10
```
Expected: all 14 tests green (skill pilot untouched).

- [ ] **Step T1.12: Run full workspace tests + clippy + fmt**

Run:
```bash
cargo test --workspace 2>&1 | tail -20
cargo clippy --workspace -- -D warnings 2>&1 | tail -5
cargo fmt --all -- --check
```
Expected: all green, no new warnings, fmt passes.

- [ ] **Step T1.13: Release build + symlink refresh**

Run:
```bash
cargo build --release 2>&1 | tail -3
ls -la ~/.cargo/bin/aionui-backend
# If it's a symlink, readlink to confirm it points at this worktree's target/release/aionui-backend
readlink ~/.cargo/bin/aionui-backend

# If needed:
ln -sf "$(pwd)/target/release/aionui-backend" ~/.cargo/bin/aionui-backend
stat -Lf "%Sm %N" ~/.cargo/bin/aionui-backend
# expect: fresh mtime matching this build
```

Use `stat -L` (follows symlink) — `stat -f` reads the link's own mtime
and is a known footgun on macOS (playbook-documented).

- [ ] **Step T1.14: Commit and push**

Run:
```bash
git add crates/aionui-api-types/src/assistant.rs \
        crates/aionui-assistant/src/builtin.rs \
        crates/aionui-app/assets/builtin-assistants/assistants.json \
        crates/aionui-app/tests/assistants_e2e.rs
git commit -m "refactor(assistant): remove rename_all camelCase from api-types and builtin manifest

- Remove 7 rename_all=camelCase from aionui-api-types/src/assistant.rs
- Remove 1 rename_all=camelCase from aionui-assistant/src/builtin.rs
- Rewrite assets/builtin-assistants/assistants.json keys to snake_case via jq walk
- Flip 9 hardcoded JSON keys in assistants_e2e.rs
- Add assistant_response_rejects_camel_case regression test
"
git push -u origin feat/assistant-snake-case 2>&1 | tail -5
git log origin/feat/assistant-snake-case --oneline -1
# expect: the SHA of the commit you just pushed
```

**Task complete =** branch pushed AND `git log origin/feat/assistant-snake-case --oneline -1` shows your SHA AND all tests documented in this task are green.

---

## T2b: ACP hotfix — `fix/acp-camelcase-hotfix` on AionUi

**Owner:** frontend-dev
**Worktree:** `/Users/zhoukai/Documents/worktrees/aionui-assistant-camel`
**Branch:** `fix/acp-camelcase-hotfix` off HEAD of the worktree (`460259c9d`).

**Files:**
- Modify: `src/common/adapter/ipcBridge.ts` lines ~584-605 (setModel + setConfigOption body keys)
- Create: `tests/unit/ipcBridge.acpHotfix.test.ts` (2 regression tests)

### Steps

- [ ] **Step T2b.1: Create branch + verify baseline**

Run:
```bash
cd /Users/zhoukai/Documents/worktrees/aionui-assistant-camel
git checkout -b fix/acp-camelcase-hotfix
bun install 2>&1 | tail -3
bun run test --run 2>&1 | tail -5
bunx tsc --noEmit 2>&1 | tail -3
```
Expected: tests green, tsc clean.

- [ ] **Step T2b.2: Write failing regression test**

Create: `tests/unit/ipcBridge.acpHotfix.test.ts`

```typescript
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { acpConversations } from '../../src/common/adapter/ipcBridge';

describe('ipcBridge.acpConversations — wire body uses snake_case', () => {
  beforeEach(() => {
    vi.stubGlobal('fetch', vi.fn(async (_url: string, init?: RequestInit) => {
      return { ok: true, status: 200, json: async () => ({ data: null }) } as Response;
    }));
  });

  it('setModel sends {model_id} not {modelId}', async () => {
    await acpConversations.setModel.invoke({ conversationId: 'c1', modelId: 'claude-sonnet-4' });
    const fetchMock = globalThis.fetch as ReturnType<typeof vi.fn>;
    const [, init] = fetchMock.mock.calls[0];
    const body = JSON.parse(init!.body as string);
    expect(body).toEqual({ model_id: 'claude-sonnet-4' });
    expect(body).not.toHaveProperty('modelId');
  });

  it('setConfigOption sends snake_case body keys', async () => {
    await acpConversations.setConfigOption.invoke({
      conversationId: 'c1',
      configId: 'temperature',
      value: '0.5',
    });
    const fetchMock = globalThis.fetch as ReturnType<typeof vi.fn>;
    const [, init] = fetchMock.mock.calls[0];
    const body = JSON.parse(init!.body as string);
    expect(body).toEqual({ value: '0.5' });
    // configId is in URL path, not body — this asserts it's NOT in body
    expect(body).not.toHaveProperty('configId');
    expect(body).not.toHaveProperty('config_id');
  });
});
```

Note: `acpConversations` is the export name in ipcBridge.ts; verify
the actual export name with `grep '^export const' src/common/adapter/ipcBridge.ts`
and adjust the import if different. The first test's shape adapts to
that name.

- [ ] **Step T2b.3: Run test to verify it fails**

Run:
```bash
bun run test --run tests/unit/ipcBridge.acpHotfix.test.ts 2>&1 | tail -10
```
Expected: both tests FAIL (body contains `modelId` not `model_id`).

- [ ] **Step T2b.4: Fix `setModel` body**

File: `src/common/adapter/ipcBridge.ts` around line 584.

Change:
```typescript
// BEFORE
setModel: httpPut<
  void,
  { conversationId: string; modelId: string }
>(
  (p) => `/api/conversations/${p.conversationId}/acp/model`,
  (p) => ({ modelId: p.modelId }),
),

// AFTER
setModel: httpPut<
  void,
  { conversationId: string; modelId: string }
>(
  (p) => `/api/conversations/${p.conversationId}/acp/model`,
  (p) => ({ model_id: p.modelId }),
),
```

The TS type signature `{modelId}` is the **frontend-facing** input
shape — keep camelCase for TS idiom. Only the wire body flips.

- [ ] **Step T2b.5: Fix `setConfigOption` body**

File: `src/common/adapter/ipcBridge.ts` around lines 595-600.

Inspect first:
```bash
grep -A 6 'setConfigOption:' src/common/adapter/ipcBridge.ts
```

Verify there's no `configId` in the body transformer — it should only
appear in the URL path. If the body transformer already only has
`{value: p.value}`, no edit needed (just keep consistency). The
regression test asserts this.

- [ ] **Step T2b.6: Run tests to verify pass**

Run:
```bash
bun run test --run tests/unit/ipcBridge.acpHotfix.test.ts 2>&1 | tail -10
bun run test --run 2>&1 | tail -5
bunx tsc --noEmit 2>&1 | tail -3
```
Expected: the 2 new tests pass, existing tests still green, tsc clean.

- [ ] **Step T2b.7: Commit and push**

Run:
```bash
git add src/common/adapter/ipcBridge.ts tests/unit/ipcBridge.acpHotfix.test.ts
git commit -m "fix(adapter): ACP setModel body uses snake_case model_id

Backend aionui-api-types/src/acp.rs has always expected snake_case
(model_id). Frontend was sending modelId, silently broken since
before the skill snake-case realignment pilot (followup item).
This fixes by aligning body keys while keeping TS-facing camelCase
parameter names.
"
git push -u origin fix/acp-camelcase-hotfix 2>&1 | tail -5
git log origin/fix/acp-camelcase-hotfix --oneline -1
```

**Task complete =** pushed AND `git log origin/...` shows the SHA AND
Vitest fully green AND tsc clean.

---

## T2c: FS hotfix — `fix/fs-temp-camelcase-hotfix` on AionUi

**Owner:** frontend-dev
**Worktree:** `/Users/zhoukai/Documents/worktrees/aionui-assistant-camel`
**Branch:** `fix/fs-temp-camelcase-hotfix` off worktree HEAD (`460259c9d`)
— **independent of T2b**; base is coordinator branch, not T2b.

**Files:**
- Modify: `src/common/adapter/ipcBridge.ts` lines ~305-306
- Modify: all call sites of `createTempFile` and `createUploadFile`
- Create: `tests/unit/ipcBridge.fsHotfix.test.ts` (2 regression tests)

### Steps

- [ ] **Step T2c.1: Create branch + find all callers**

Run:
```bash
cd /Users/zhoukai/Documents/worktrees/aionui-assistant-camel
git checkout feat/backend-migration-coordinator-assistant-camel
git checkout -b fix/fs-temp-camelcase-hotfix

grep -rn 'createTempFile\|createUploadFile' src/ tests/ 2>&1 | tee /tmp/fs-callers.txt
wc -l /tmp/fs-callers.txt
```
Record the call site count. Every call site passing `{fileName: ...}`
will need to become `{file_name: ...}`.

- [ ] **Step T2c.2: Write failing regression test**

Create: `tests/unit/ipcBridge.fsHotfix.test.ts`

```typescript
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { fs } from '../../src/common/adapter/ipcBridge';

describe('ipcBridge.fs — createTempFile/createUploadFile use snake_case body', () => {
  beforeEach(() => {
    vi.stubGlobal('fetch', vi.fn(async () => ({ ok: true, status: 200, json: async () => ({ data: '/tmp/x' }) } as Response)));
  });

  it('createTempFile sends {file_name}', async () => {
    await fs.createTempFile.invoke({ file_name: 'x.txt' });
    const fetchMock = globalThis.fetch as ReturnType<typeof vi.fn>;
    const [, init] = fetchMock.mock.calls[0];
    const body = JSON.parse(init!.body as string);
    expect(body).toEqual({ file_name: 'x.txt' });
    expect(body).not.toHaveProperty('fileName');
  });

  it('createUploadFile sends {file_name, conversation_id}', async () => {
    await fs.createUploadFile.invoke({ file_name: 'y.zip', conversation_id: 'c1' });
    const fetchMock = globalThis.fetch as ReturnType<typeof vi.fn>;
    const [, init] = fetchMock.mock.calls[0];
    const body = JSON.parse(init!.body as string);
    expect(body).toEqual({ file_name: 'y.zip', conversation_id: 'c1' });
    expect(body).not.toHaveProperty('fileName');
    expect(body).not.toHaveProperty('conversationId');
  });
});
```

Grep for the actual `fs` export name first:
```bash
grep '^export const fs' src/common/adapter/ipcBridge.ts
```
If named differently (e.g. `fsAdapter`), adjust the import.

- [ ] **Step T2c.3: Run test to verify it fails**

Run:
```bash
bun run test --run tests/unit/ipcBridge.fsHotfix.test.ts 2>&1 | tail -10
```
Expected: 2 tests FAIL (tsc may also fail on the new type signature
before step T2c.4 because the input type still says `fileName`).

If tsc blocks the test from running, that's OK — proceed to T2c.4;
after the type flip, the tests will run and still fail on the body
shape, then pass after T2c.5.

- [ ] **Step T2c.4: Flip type signatures in `ipcBridge.ts`**

File: `src/common/adapter/ipcBridge.ts` lines ~305-306.

Change:
```typescript
// BEFORE
createTempFile: httpPost<string, { fileName: string }>('/api/fs/temp'),
createUploadFile: httpPost<string, { fileName: string; conversationId?: string }>('/api/fs/temp'),

// AFTER
createTempFile: httpPost<string, { file_name: string }>('/api/fs/temp'),
createUploadFile: httpPost<string, { file_name: string; conversation_id?: string }>('/api/fs/temp'),
```

Now tsc will error everywhere the callers pass `{fileName: ...}`.

- [ ] **Step T2c.5: Fix all callers**

Run:
```bash
bunx tsc --noEmit 2>&1 | grep -E 'createTempFile|createUploadFile|fileName'
```

For each error location, change the caller's argument object:
- `{fileName: x}` → `{file_name: x}`
- `{fileName: x, conversationId: c}` → `{file_name: x, conversation_id: c}`

Keep local variable names and intermediate variables camelCase —
only the object key at the invoke boundary flips. Example:

```typescript
// BEFORE
await ipcBridge.fs.createTempFile.invoke({ fileName: baseName });

// AFTER
await ipcBridge.fs.createTempFile.invoke({ file_name: baseName });
```

Iterate until `bunx tsc --noEmit` is clean.

- [ ] **Step T2c.6: Run tests to verify pass**

Run:
```bash
bun run test --run tests/unit/ipcBridge.fsHotfix.test.ts 2>&1 | tail -10
bun run test --run 2>&1 | tail -5
bunx tsc --noEmit 2>&1 | tail -3
bun run lint --quiet 2>&1 | tail -3
```
Expected: 2 new tests pass, all other tests still pass, tsc clean,
lint no new warnings.

- [ ] **Step T2c.7: Commit and push**

Run:
```bash
git add -A
# Review
git status -s
git diff --cached --stat

git commit -m "fix(adapter): fs createTempFile/createUploadFile wire bodies use snake_case

Backend aionui-api-types/src/file.rs has always expected file_name
and conversation_id. Frontend was sending fileName/conversationId,
silently broken (followup item from skill snake-case realignment).
Flip type signatures of both methods plus all call sites (tsc-enforced).
"
git push -u origin fix/fs-temp-camelcase-hotfix 2>&1 | tail -5
git log origin/fix/fs-temp-camelcase-hotfix --oneline -1
```

**Task complete =** pushed AND SHA visible on origin AND all tests +
tsc + lint green.

---

## T2a: Assistant bulk rename — `feat/assistant-snake-case` on AionUi

**Owner:** frontend-dev
**Worktree:** `/Users/zhoukai/Documents/worktrees/aionui-assistant-camel`
**Branch:** `feat/assistant-snake-case` off worktree HEAD (`460259c9d`)
**Depends on:** T1 (backend must have flipped wire first, so that Vitest
mocks and live probes reflect the new shape).

**Files — types:**
- Modify: `src/common/types/assistantTypes.ts` (central type file flip)

**Files — codemod driver:**
- Create: `scripts/codemods/assistantSnakeCase.ts` (one-shot ts-morph script)

**Files — manual inspection after codemod:**
- 43 files total (listed in spec §5.1.2). Codemod handles ~90%; audit remaining.

**Files — Electron process specifics:**
- `src/process/utils/migrateAssistants.ts` (split out `legacyAssistantToCreateRequest` helper)
- `src/process/utils/initAgent.ts`
- `src/process/extensions/resolvers/AssistantResolver.ts`

**Files — tests:**
- `tests/unit/assistant/*.test.ts` — fixtures flipped
- `tests/unit/migrateAssistants.test.ts` — output assertions flipped
- `tests/e2e/features/assistant-*` — payload assertions flipped

### Steps

- [ ] **Step T2a.1: Create branch + verify T1 landed**

Run:
```bash
cd /Users/zhoukai/Documents/worktrees/aionui-assistant-camel
git checkout feat/backend-migration-coordinator-assistant-camel
git fetch origin feat/assistant-snake-case 2>&1 | tail -2
git log origin/feat/assistant-snake-case --oneline -1
# expect: backend-dev's commit exists

git checkout -b feat/assistant-snake-case
bun install 2>&1 | tail -3
bun run test --run 2>&1 | tail -5
```
Baseline: Vitest green (many will break after the flip, that's expected).

- [ ] **Step T2a.2: Flip central types**

File: `src/common/types/assistantTypes.ts`

Apply the field map to ALL interfaces in the file (`Assistant`,
`CreateAssistantRequest`, `UpdateAssistantRequest`,
`SetAssistantStateRequest`, `ImportAssistantsRequest`,
`ImportAssistantsResult`, `ImportError`):

| camelCase | snake_case |
|---|---|
| `nameI18n` | `name_i18n` |
| `descriptionI18n` | `description_i18n` |
| `sortOrder` | `sort_order` |
| `presetAgentType` | `preset_agent_type` |
| `enabledSkills` | `enabled_skills` |
| `customSkillNames` | `custom_skill_names` |
| `disabledBuiltinSkills` | `disabled_builtin_skills` |
| `contextI18n` | `context_i18n` |
| `promptsI18n` | `prompts_i18n` |
| `lastUsedAt` | `last_used_at` |

Example — before / after for `Assistant`:
```typescript
// BEFORE
export interface Assistant {
  id: string;
  source: AssistantSource;
  name: string;
  nameI18n: Record<string, string>;
  description?: string;
  descriptionI18n: Record<string, string>;
  avatar?: string;
  enabled: boolean;
  sortOrder: number;
  presetAgentType: string;
  enabledSkills: string[];
  customSkillNames: string[];
  disabledBuiltinSkills: string[];
  context?: string;
  contextI18n: Record<string, string>;
  prompts: string[];
  promptsI18n: Record<string, string[]>;
  models: string[];
  lastUsedAt?: number;
}

// AFTER
export interface Assistant {
  id: string;
  source: AssistantSource;
  name: string;
  name_i18n: Record<string, string>;
  description?: string;
  description_i18n: Record<string, string>;
  avatar?: string;
  enabled: boolean;
  sort_order: number;
  preset_agent_type: string;
  enabled_skills: string[];
  custom_skill_names: string[];
  disabled_builtin_skills: string[];
  context?: string;
  context_i18n: Record<string, string>;
  prompts: string[];
  prompts_i18n: Record<string, string[]>;
  models: string[];
  last_used_at?: number;
}
```

Do NOT rename `Assistant` itself or the non-mapped fields.

After this single file edit, tsc will error in ~209 places across
43 files.

- [ ] **Step T2a.3: Sanity — tsc errors bounded**

Run:
```bash
bunx tsc --noEmit 2>&1 | grep -cE 'nameI18n|descriptionI18n|sortOrder|presetAgentType|enabledSkills|customSkillNames|disabledBuiltinSkills|contextI18n|promptsI18n|lastUsedAt'
```
Record the error count. Expected: in the range 150-250 (many-to-one mapping of 209 access sites → tsc error lines may differ slightly).

If the count is 0, types didn't flip — go back to T2a.2.
If dramatically more than 300, something other than `Assistant` is also affected — investigate before writing codemod.

- [ ] **Step T2a.4: Write the ts-morph codemod**

Create: `scripts/codemods/assistantSnakeCase.ts`

```typescript
import { Project, SyntaxKind } from 'ts-morph';

const FIELD_MAP: Record<string, string> = {
  nameI18n: 'name_i18n',
  descriptionI18n: 'description_i18n',
  sortOrder: 'sort_order',
  presetAgentType: 'preset_agent_type',
  enabledSkills: 'enabled_skills',
  customSkillNames: 'custom_skill_names',
  disabledBuiltinSkills: 'disabled_builtin_skills',
  contextI18n: 'context_i18n',
  promptsI18n: 'prompts_i18n',
  lastUsedAt: 'last_used_at',
};

const ASSISTANT_TYPES = new Set([
  'Assistant',
  'CreateAssistantRequest',
  'UpdateAssistantRequest',
  'SetAssistantStateRequest',
  'ImportAssistantsRequest',
  'ImportAssistantsResult',
  'ImportError',
]);

function targetIsAssistantShape(type: import('ts-morph').Type): boolean {
  // Walk union + intersection + array element + promise unwrap
  const flat = type.isArray() ? [type.getArrayElementType()!] : [type];
  for (const t of flat) {
    const sym = t.getSymbol() ?? t.getAliasSymbol();
    if (sym && ASSISTANT_TYPES.has(sym.getName())) return true;
    // Also handle Assistant[] passed into functions
    for (const sub of t.getUnionTypes()) {
      const subSym = sub.getSymbol() ?? sub.getAliasSymbol();
      if (subSym && ASSISTANT_TYPES.has(subSym.getName())) return true;
    }
  }
  return false;
}

const project = new Project({
  tsConfigFilePath: 'tsconfig.json',
  skipAddingFilesFromTsConfig: false,
});

let propAccessFlipped = 0;
let objectLiteralFlipped = 0;
let destructureFlipped = 0;

for (const sf of project.getSourceFiles()) {
  if (sf.getFilePath().includes('node_modules')) continue;
  if (sf.getFilePath().endsWith('/assistantTypes.ts')) continue; // already done by hand

  // (a) Property access: x.nameI18n where x has Assistant shape
  sf.forEachDescendant((node) => {
    if (node.getKind() === SyntaxKind.PropertyAccessExpression) {
      const pae = node.asKindOrThrow(SyntaxKind.PropertyAccessExpression);
      const propName = pae.getName();
      if (!FIELD_MAP[propName]) return;
      const recvType = pae.getExpression().getType();
      if (targetIsAssistantShape(recvType)) {
        pae.getNameNode().replaceWithText(FIELD_MAP[propName]);
        propAccessFlipped++;
      }
    }
  });

  // (b) Object literal property assignment: { sortOrder: 5 } when contextual type is Assistant
  sf.forEachDescendant((node) => {
    if (node.getKind() !== SyntaxKind.PropertyAssignment) return;
    const pa = node.asKindOrThrow(SyntaxKind.PropertyAssignment);
    const name = pa.getName();
    if (!FIELD_MAP[name]) return;
    const parentType = pa.getParentIfKind(SyntaxKind.ObjectLiteralExpression)?.getContextualType();
    if (parentType && targetIsAssistantShape(parentType)) {
      pa.getNameNode().replaceWithText(FIELD_MAP[name]);
      objectLiteralFlipped++;
    }
  });

  // (c) Destructuring: const { sortOrder } = assistant  →  const { sort_order: sortOrder } = assistant
  sf.forEachDescendant((node) => {
    if (node.getKind() !== SyntaxKind.BindingElement) return;
    const be = node.asKindOrThrow(SyntaxKind.BindingElement);
    if (be.getPropertyNameNode()) return; // already aliased
    const nameNode = be.getNameNode();
    if (nameNode.getKind() !== SyntaxKind.Identifier) return;
    const name = nameNode.getText();
    if (!FIELD_MAP[name]) return;
    // The source of the destructure — walk up until we find VariableDeclaration
    const vd = be.getFirstAncestorByKind(SyntaxKind.VariableDeclaration);
    if (!vd) return;
    const initType = vd.getInitializer()?.getType();
    if (initType && targetIsAssistantShape(initType)) {
      be.setPropertyName(FIELD_MAP[name]);
      destructureFlipped++;
    }
  });
}

project.saveSync();

console.log(`Flipped property accesses: ${propAccessFlipped}`);
console.log(`Flipped object literals: ${objectLiteralFlipped}`);
console.log(`Flipped destructurings: ${destructureFlipped}`);
```

- [ ] **Step T2a.5: Run the codemod**

Run:
```bash
bun install ts-morph --dev 2>&1 | tail -2
bunx tsx scripts/codemods/assistantSnakeCase.ts 2>&1 | tail -5
```
Expected output: summary with 3 counts summing to ~200ish. If all 3
are 0, `targetIsAssistantShape` is not matching — check tsconfig path.

- [ ] **Step T2a.6: Re-check tsc errors**

Run:
```bash
bunx tsc --noEmit 2>&1 | grep -cE 'nameI18n|descriptionI18n|sortOrder|presetAgentType|enabledSkills|customSkillNames|disabledBuiltinSkills|contextI18n|promptsI18n|lastUsedAt'
```
Expected: dropped significantly from T2a.3's count. Remainder (residual
~10-30) is the "Wave 2 manual" — codemod can't handle cases like
dynamic access (`obj[key]` where `key` is a string variable), JSON
literal payloads, some complex destructure patterns, or
object-literal-without-contextual-type.

- [ ] **Step T2a.7: Manual Wave 2 — iterate each remaining tsc error**

Run until clean:
```bash
bunx tsc --noEmit 2>&1 | head -20
```

For each error:
1. Read the line.
2. Determine: is this an `Assistant` shape? If yes, flip the camelCase key by hand (same destructure alias pattern or direct rename as appropriate).
3. If no (it's an unrelated local type with an unfortunately similar name like `nameI18n` on a completely different type), add an inline comment explaining why it stays camelCase, or just leave it.

Iterate until `bunx tsc --noEmit` returns 0 errors.

- [ ] **Step T2a.8: Refactor `migrateAssistants.ts` to split out the mapper**

File: `src/process/utils/migrateAssistants.ts`

Identify the function that receives legacy ConfigStorage shape
(camelCase, untouched) and builds a `CreateAssistantRequest`
(now snake_case). Extract the mapping into a separate pure function
`legacyAssistantToCreateRequest`.

Example starting point (adapt to actual code shape):

```typescript
// New helper (near top of file or at bottom)
import type { CreateAssistantRequest } from '../../common/types/assistantTypes';

interface LegacyAssistant {
  id: string;
  name: string;
  nameI18n?: Record<string, string>;
  description?: string;
  descriptionI18n?: Record<string, string>;
  avatar?: string;
  presetAgentType?: string;
  enabledSkills?: string[];
  customSkillNames?: string[];
  disabledBuiltinSkills?: string[];
  prompts?: string[];
  promptsI18n?: Record<string, string[]>;
  models?: string[];
}

export function legacyAssistantToCreateRequest(legacy: LegacyAssistant): CreateAssistantRequest {
  return {
    id: legacy.id,
    name: legacy.name,
    description: legacy.description,
    avatar: legacy.avatar,
    preset_agent_type: legacy.presetAgentType,
    enabled_skills: legacy.enabledSkills,
    custom_skill_names: legacy.customSkillNames,
    disabled_builtin_skills: legacy.disabledBuiltinSkills,
    prompts: legacy.prompts,
    models: legacy.models,
    name_i18n: legacy.nameI18n,
    description_i18n: legacy.descriptionI18n,
    prompts_i18n: legacy.promptsI18n,
  };
}
```

Then replace the inline mapping in the existing migrate function with a
call to `legacyAssistantToCreateRequest(rawLegacyAssistant)`.

- [ ] **Step T2a.9: Unit test for `legacyAssistantToCreateRequest`**

File: `tests/unit/migrateAssistants.test.ts`

If it doesn't exist, create. Otherwise add test case. Use a representative
fixture — ideally take a couple real entries from a user's old
`aionui-config.txt` format (any existing migration-pilot fixture works).

```typescript
import { describe, it, expect } from 'vitest';
import { legacyAssistantToCreateRequest } from '../../src/process/utils/migrateAssistants';

describe('legacyAssistantToCreateRequest', () => {
  it('maps camelCase legacy fields to snake_case CreateAssistantRequest', () => {
    const legacy = {
      id: 'a1',
      name: 'X',
      nameI18n: { 'en-US': 'X-en' },
      description: 'desc',
      descriptionI18n: { 'en-US': 'desc-en' },
      avatar: '🦊',
      presetAgentType: 'gemini',
      enabledSkills: ['s1'],
      customSkillNames: [],
      disabledBuiltinSkills: ['b1'],
      prompts: ['p'],
      promptsI18n: { 'en-US': ['p-en'] },
      models: ['gemini-pro'],
    };
    const out = legacyAssistantToCreateRequest(legacy);
    expect(out).toEqual({
      id: 'a1',
      name: 'X',
      name_i18n: { 'en-US': 'X-en' },
      description: 'desc',
      description_i18n: { 'en-US': 'desc-en' },
      avatar: '🦊',
      preset_agent_type: 'gemini',
      enabled_skills: ['s1'],
      custom_skill_names: [],
      disabled_builtin_skills: ['b1'],
      prompts: ['p'],
      prompts_i18n: { 'en-US': ['p-en'] },
      models: ['gemini-pro'],
    });
  });

  it('handles missing optional fields without crashing', () => {
    const legacy = { id: 'min', name: 'M' };
    const out = legacyAssistantToCreateRequest(legacy);
    expect(out.id).toBe('min');
    expect(out.name).toBe('M');
    expect(out.preset_agent_type).toBeUndefined();
    expect(out.enabled_skills).toBeUndefined();
  });
});
```

- [ ] **Step T2a.10: Flip Vitest fixtures**

Grep for `Assistant`-shaped mock objects in `tests/unit/**/*.test.ts`:

```bash
grep -rE "(nameI18n|descriptionI18n|sortOrder|presetAgentType|enabledSkills|customSkillNames|disabledBuiltinSkills|contextI18n|promptsI18n|lastUsedAt)" tests/unit/ 2>&1 | head -40
```

For each fixture that represents an `Assistant`, flip the field names
(not the variable names the test uses). Save. Repeat until the above
grep returns 0 hits in assistant-test fixtures.

Exception: fixtures in `tests/unit/migrateAssistants.test.ts` that
represent **input legacy shape** — these stay camelCase. That's the
whole point of §T2a.9.

- [ ] **Step T2a.11: Run all Vitest, verify green**

Run:
```bash
bun run test --run 2>&1 | tail -10
```

Iterate any failing tests. Common causes:
- Stale mock still has camelCase → flip it.
- Assertion literals (`.toHaveProperty('sortOrder')`) → flip to `.toHaveProperty('sort_order')`.

- [ ] **Step T2a.12: Flip Playwright fixtures**

```bash
grep -rE "(nameI18n|descriptionI18n|sortOrder|presetAgentType|enabledSkills|customSkillNames|disabledBuiltinSkills|contextI18n|promptsI18n|lastUsedAt)" tests/e2e/ 2>&1 | head -40
```

Flip any JSON body literals / payload assertions that target Assistant
endpoints. Leave alone anything that's intentionally legacy-camelCase
input (unlikely in e2e).

- [ ] **Step T2a.13: Final sweep grep — DoD-level checks**

Run:
```bash
# Property access sites (should all be flipped now, except legacy-input code in migrateAssistants.ts — but that's typed against LegacyAssistant, not Assistant, so the type system has already gated it)
grep -rE "\.(nameI18n|descriptionI18n|sortOrder|presetAgentType|enabledSkills|customSkillNames|disabledBuiltinSkills|contextI18n|promptsI18n|lastUsedAt)\b" src/ 2>&1 | grep -v 'migrateAssistants\.ts\|LegacyAssistant' | wc -l
# expect: 0

# String-form field names (may have some in test fixtures for legacy shape — only fail if found in production src/)
grep -rE "['\"](nameI18n|descriptionI18n|sortOrder|presetAgentType|enabledSkills|customSkillNames|disabledBuiltinSkills|contextI18n|promptsI18n|lastUsedAt)['\"]" src/ 2>&1 | grep -v 'migrateAssistants\|LegacyAssistant' | wc -l
# expect: 0
```

If non-zero, grep by specific field, read each, decide: flip or leave
as legacy-input.

- [ ] **Step T2a.14: tsc + lint + test full sweep**

Run:
```bash
bunx tsc --noEmit 2>&1 | tail -3
bun run lint --quiet 2>&1 | tail -3
bun run test --run 2>&1 | tail -10
```
Expected: all green, zero new warnings.

- [ ] **Step T2a.15: Commit and push**

Run:
```bash
git add -A
git status -s | head
git diff --cached --stat | tail -20

git commit -m "refactor(assistant): flip frontend Assistant type + 209 access sites to snake_case

- src/common/types/assistantTypes.ts field flip
- ts-morph codemod at scripts/codemods/assistantSnakeCase.ts (kept for reproducibility)
- Destructuring preserves local variable names via { snake_name: camelName } pattern
- migrateAssistants.ts: split out legacyAssistantToCreateRequest mapper (legacy camel stays, wire snake flips)
- Vitest + Playwright fixtures realigned
- All access sites flipped; grep DoD returns 0 hits in src/

Pairs with backend feat/assistant-snake-case that removed 7 rename_all
from aionui-api-types/assistant.rs, 1 from aionui-assistant/builtin.rs,
and rewrote assets/builtin-assistants/assistants.json via jq walk.
"
git push -u origin feat/assistant-snake-case 2>&1 | tail -5
git log origin/feat/assistant-snake-case --oneline -1
```

**Task complete =** pushed AND upstream SHA verified AND tsc + lint +
Vitest all green AND DoD greps return 0.

---

## T3: E2E — Playwright + regression reruns

**Owner:** e2e-tester
**Worktree:** `/Users/zhoukai/Documents/worktrees/aionui-assistant-camel` (Playwright binds AionUi)
**Branch:** `feat/assistant-snake-case` (on AionUi) — run with all three frontend branches merged locally, OR run against each branch separately if merge conflicts.

**Depends on:** T1 + T2a + T2b + T2c all pushed.

**Files:**
- Read only: `tests/e2e/features/**/*.e2e.ts`
- Create: `docs/backend-migration/runs/2026-04-24-assistant-snake-case-e2e.md` (test report)

### Steps

- [ ] **Step T3.1: Pull latest + set up test base**

Run:
```bash
cd /Users/zhoukai/Documents/worktrees/aionui-assistant-camel
git fetch origin
# Create a local integration branch that merges all three frontend branches
git checkout -b test/assistant-snake-case-integration feat/backend-migration-coordinator-assistant-camel
git merge origin/feat/assistant-snake-case --no-edit
git merge origin/fix/acp-camelcase-hotfix --no-edit
git merge origin/fix/fs-temp-camelcase-hotfix --no-edit
```

If any merge has conflicts: attempt `git checkout --theirs` on ipcBridge.ts
and review carefully. If unclear, abort and SendMessage coordinator.

- [ ] **Step T3.2: Rebuild Electron + verify backend is fresh**

Run:
```bash
bun install 2>&1 | tail -3
# Backend symlink should be pointing at the aionui-backend worktree
readlink ~/.cargo/bin/aionui-backend
stat -Lf "%Sm %N" ~/.cargo/bin/aionui-backend
# expect: recent mtime (within this pilot's window) and path pointing at
# /Users/zhoukai/Documents/worktrees/aionui-backend-assistant-camel/target/release/aionui-backend

# If not, SendMessage backend-dev to refresh. Don't silently proceed.
```

- [ ] **Step T3.3: Run Vitest as pre-check**

Run:
```bash
bun run test --run 2>&1 | tail -10
```
Expected: all green. If not, integration branch has a regression that
didn't show in the individual branches — do NOT proceed; report back.

- [ ] **Step T3.4: Run Playwright assistant scenarios**

Run:
```bash
bun run test:e2e -- tests/e2e/features/assistant 2>&1 | tail -40
```
Expected: all scenarios green.

- [ ] **Step T3.5: Run Playwright skill scenarios as regression**

Run:
```bash
bun run test:e2e -- tests/e2e/features/builtin-skill 2>&1 | tail -20
```
Expected: 8/8 green (from skill pilot's baseline).

- [ ] **Step T3.6: Run Playwright ACP + fs-temp scenarios (regression for hotfixes)**

Run:
```bash
# Identify scenarios relevant to setModel / setConfigOption / createTempFile / createUploadFile
grep -rln 'setModel\|setConfigOption\|createTempFile\|createUploadFile' tests/e2e/ 2>&1 | head
```

If relevant e2e suites exist, run them:
```bash
bun run test:e2e -- tests/e2e/features/<relevant-path> 2>&1 | tail -20
```
If no e2e coverage for these: skip, note in report ("no Playwright
coverage for ACP setModel / fs createTempFile; regression gated by
Vitest only").

- [ ] **Step T3.7: Write run report**

Create: `docs/backend-migration/runs/2026-04-24-assistant-snake-case-e2e.md`

```markdown
# E2E Test Run Report — Assistant Snake-Case Realignment

**Date:** 2026-04-24
**Owner:** e2e-tester
**Branches tested:** feat/assistant-snake-case (backend + frontend), fix/acp-camelcase-hotfix, fix/fs-temp-camelcase-hotfix (merged into test/assistant-snake-case-integration)

## Results

- Vitest: <N>/<N> green
- Playwright assistant suite: <N>/<N> green
- Playwright skill suite (regression): 8/8 green
- Playwright ACP/fs suite: <N>/<N> green (or: no direct coverage, Vitest-gated)

## Commits under test

- aionui-backend feat/assistant-snake-case: <SHA>
- AionUi feat/assistant-snake-case: <SHA>
- AionUi fix/acp-camelcase-hotfix: <SHA>
- AionUi fix/fs-temp-camelcase-hotfix: <SHA>

## Issues found

<none / detail>

## Conclusion

<ready for T4 merge / needs rework>
```

- [ ] **Step T3.8: Commit report + push**

Run:
```bash
git add docs/backend-migration/runs/2026-04-24-assistant-snake-case-e2e.md
git commit -m "docs(backend-migration): e2e run report for assistant snake-case realignment"
git push origin test/assistant-snake-case-integration 2>&1 | tail -5
git log origin/test/assistant-snake-case-integration --oneline -1
```

Also SendMessage coordinator with:
- all 4 SHAs tested
- pass/fail summary
- any issue details

**Task complete =** report pushed AND coordinator acknowledges AND all scenario results green.

---

## T4: Coordinator closure — merge + packaging smoke + handoff

**Owner:** coordinator
**Worktrees:**
- Backend: `/Users/zhoukai/Documents/worktrees/aionui-backend-assistant-camel`
- AionUi: `/Users/zhoukai/Documents/worktrees/aionui-assistant-camel`

**Depends on:** T3 green.

**Files:**
- Merge into AionUi `feat/backend-migration-coordinator-assistant-camel`:
  - `feat/assistant-snake-case`
  - `fix/acp-camelcase-hotfix`
  - `fix/fs-temp-camelcase-hotfix`
- Merge into backend `feat/backend-migration-coordinator-assistant-camel`:
  - `feat/assistant-snake-case`
- Finally merge both coordinator-assistant-camel branches back into the originating branches:
  - Backend: back to `feat/builtin-skills`
  - AionUi: back to `feat/backend-migration-coordinator`
- Create: `AionUi/docs/backend-migration/handoffs/coordinator-assistant-snake-case-2026-04-24.md`
- Update: `AionUi/docs/backend-migration/notes/team-operations-playbook.md` (new lessons)
- Update: `AionUi/docs/backend-migration/modules/skill-library.md` or equivalent (module log)

### Steps

- [ ] **Step T4.1: Merge feature branches into coord branches**

Backend:
```bash
cd /Users/zhoukai/Documents/worktrees/aionui-backend-assistant-camel
git fetch origin
git checkout feat/backend-migration-coordinator-assistant-camel
git merge origin/feat/assistant-snake-case --no-edit
# Expect: clean merge (no conflicts — coord branch and feat branch diverged only by the feat commits)
git push origin feat/backend-migration-coordinator-assistant-camel 2>&1 | tail -3
```

AionUi (merge order: small → large to minimize `ipcBridge.ts` conflicts):
```bash
cd /Users/zhoukai/Documents/worktrees/aionui-assistant-camel
git fetch origin
git checkout feat/backend-migration-coordinator-assistant-camel

# Merge order per spec §6.7: small first, large last
git merge origin/fix/acp-camelcase-hotfix --no-edit
# expect: clean merge (changes to lines 584-599 of ipcBridge.ts only)

git merge origin/fix/fs-temp-camelcase-hotfix --no-edit
# expect: clean merge (changes to lines 305-306 of ipcBridge.ts only; non-overlapping with acp block)

git merge origin/feat/assistant-snake-case --no-edit
# expect: clean merge (assistant bulk rename doesn't touch ipcBridge.ts lines the hotfixes flipped)
# if conflicts: `git checkout --theirs` on ipcBridge.ts (pilot-side wins per skill-pilot precedent), hand-verify

git push origin feat/backend-migration-coordinator-assistant-camel 2>&1 | tail -3
```

- [ ] **Step T4.2: Packaging smoke (release binary)**

Run:
```bash
cd /Users/zhoukai/Documents/worktrees/aionui-backend-assistant-camel
cargo build --release 2>&1 | tail -3

TMPDIR=$(mktemp -d)
echo "Smoke tmpdir: $TMPDIR"
cp target/release/aionui-backend "$TMPDIR/"
# Deliberately NO `cp -r assets/ $TMPDIR/` — we want to prove the binary is self-contained

"$TMPDIR/aionui-backend" --local --port 25911 --data-dir "$TMPDIR/data" &
BACKEND_PID=$!
sleep 3

# Probe 1: GET /api/assistants row keys
curl -s http://127.0.0.1:25911/api/assistants | jq '.data[0] | keys' 2>&1
# expect: keys are all snake_case; no camelCase key

# Probe 2: POST /api/assistants with snake body
curl -s -X POST http://127.0.0.1:25911/api/assistants \
  -H 'Content-Type: application/json' \
  -d '{"name":"X","preset_agent_type":"gemini","enabled_skills":["s1"]}'
# expect: 201 response data has preset_agent_type=gemini, enabled_skills=["s1"]

# Probe 3: POST with camel body — should NOT populate snake field
curl -s -X POST http://127.0.0.1:25911/api/assistants \
  -H 'Content-Type: application/json' \
  -d '{"name":"Y","presetAgentType":"gemini","enabledSkills":["s1"]}' | jq '.data.preset_agent_type'
# expect: "" (empty string — camelCase was silently ignored, NOT aliased)

# Probe 4: builtin assistants loaded
curl -s http://127.0.0.1:25911/api/assistants | jq '.data | map(select(.source == "builtin")) | length'
# expect: 20 (or whatever the current builtin count is — matches assistants.json entry count)

kill $BACKEND_PID
rm -rf "$TMPDIR"
```

Record probe results.

- [ ] **Step T4.3: Write handoff**

Create: `AionUi/docs/backend-migration/handoffs/coordinator-assistant-snake-case-2026-04-24.md`

Structure (follow skill-realignment handoff as model):
```markdown
# Coordinator Handoff — Assistant Snake-Case Realignment — 2026-04-24

**Coordinator branches:**
- AionUi: feat/backend-migration-coordinator-assistant-camel @ <SHA after T4.1>
- aionui-backend: feat/backend-migration-coordinator-assistant-camel @ <SHA>

**Feature branches:**
- AionUi feat/assistant-snake-case @ <SHA>
- AionUi fix/acp-camelcase-hotfix @ <SHA>
- AionUi fix/fs-temp-camelcase-hotfix @ <SHA>
- aionui-backend feat/assistant-snake-case @ <SHA>

**PRs:** None, per user convention.

## What shipped

Removed all camelCase wire residues from the assistant pilot +
2 drive-by hotfixes from the skill realignment pilot's followup list.

- Backend: 7 rename_all removed from api-types/assistant.rs,
  1 from aionui-assistant/builtin.rs, 20-entry assistants.json
  rewritten via jq walk (~200 key substitutions), 9 hardcoded
  assistants_e2e.rs JSON keys flipped, 1 new regression test
  (assistant_response_rejects_camel_case).
- Frontend (heavy): assistantTypes.ts central type flip, ts-morph
  codemod handled ~90% of 209 access sites across 43 files,
  manual Wave 2 covered residual. migrateAssistants.ts split
  legacyAssistantToCreateRequest mapper (legacy stays camel at
  type level, wire flips to snake).
- Frontend (hotfix): ipcBridge.ts setModel now sends model_id;
  setConfigOption body verified clean; createTempFile +
  createUploadFile type signatures flipped, all call sites
  updated (tsc-enforced).

Test matrix:
- backend cargo test --workspace: all green
- frontend Vitest: all green
- Playwright assistant: <N>/<N> green
- Playwright skill (regression): 8/8 green
- packaging smoke (T4.2): all 4 probes passed

## Why

§1 of the spec — the assistant pilot's scaffold landed 5 hours after
dae96f8 established snake_case as project-wide wire convention, but
the author didn't read main's history. skill realignment pilot's
followup list flagged ACP and fs-temp as "may or may not be broken";
both were confirmed broken.

## Role deliverables

| Role | Final SHA | Deliverable |
|---|---|---|
| coordinator | <merge commit SHA> | spec, plan, merge, packaging smoke, this handoff |
| backend-dev | <feat/assistant-snake-case backend SHA> | T1 |
| frontend-dev | <feat/assistant-snake-case + acp + fs SHAs> | T2a, T2b, T2c |
| e2e-tester | <integration branch SHA> | T3 + run report |

## Merge conflicts during T4.1

<record any; if clean, "none">

## Lessons captured

<add to playbook — see T4.4>

## Followups (non-blocking)

- Other endpoints not audited — e.g. `getMode`, `getModelInfo`,
  `getConfigOptions` — may or may not have frontend/backend
  divergence. User chose Q6(b) to defer.
- Project-wide lint on aionui-api-types for rename_all remains
  unlanded (mentioned in skill-realignment followups still valid).
- channel/plugins/{weixin,dingtalk} camelCase stays — external
  webhook protocols, not project convention.
```

- [ ] **Step T4.4: Append lessons to playbook**

File: `AionUi/docs/backend-migration/notes/team-operations-playbook.md`

Append (newest on top, per file convention):

Candidate lessons (pick based on what actually happened during the run):
- "ts-morph codemod design — contextual-type gating for wide-shape renames" (generic lesson from T2a's approach)
- "jq walk is the key-only rewrite" (lesson for T1.7's assistants.json)
- "Integration branch for multi-branch e2e" (T3.1's pattern)
- Any zombie / backlog / merge-conflict incidents that surfaced

Only write what actually happened. If the run was clean, just
append one "clean team run — reference execution for future
multi-branch pilots" note.

- [ ] **Step T4.5: Update module log**

File: look for most recent module-log entry in
`AionUi/docs/backend-migration/modules/skill-library.md` or create
`assistant.md` sibling if this pilot wants its own module log.

Append a short entry:

```markdown
## 2026-04-24 — Assistant snake-case realignment

Pilot: 3 branches (feat/assistant-snake-case + 2 hotfixes).
Backend removed 7 rename_all from assistant.rs + 1 from builtin.rs,
rewrote assistants.json; frontend flipped 209 access sites via
ts-morph codemod + manual Wave 2. Hotfixes fixed runtime-broken ACP
setModel and fs-temp body keys that the skill pilot's handoff had
flagged as followups.

All tests green; packaging smoke confirmed self-contained binary
serves snake_case to clients.

Branches merged into feat/backend-migration-coordinator-assistant-camel.
Then merged back to feat/builtin-skills (backend) + feat/backend-migration-coordinator (AionUi).
```

- [ ] **Step T4.6: Merge coord-assistant-camel branches back to originating branches**

Backend:
```bash
cd /Users/zhoukai/Documents/worktrees/aionui-backend-assistant-camel

# Switch back to the originating branch in the main checkout (not worktree)
cd /Users/zhoukai/Documents/github/aionui-backend
git checkout feat/builtin-skills
git fetch origin
git merge origin/feat/backend-migration-coordinator-assistant-camel --no-edit
# expect: fast-forward or clean merge
git push origin feat/builtin-skills 2>&1 | tail -3
```

AionUi:
```bash
cd /Users/zhoukai/Documents/github/AionUi
git checkout feat/backend-migration-coordinator
git fetch origin
git merge origin/feat/backend-migration-coordinator-assistant-camel --no-edit
# expect: fast-forward or clean merge
git push origin feat/backend-migration-coordinator 2>&1 | tail -3
```

- [ ] **Step T4.7: Commit handoff + module log + playbook**

Run (from AionUi main checkout):
```bash
cd /Users/zhoukai/Documents/github/AionUi
git add docs/backend-migration/handoffs/coordinator-assistant-snake-case-2026-04-24.md \
        docs/backend-migration/notes/team-operations-playbook.md \
        docs/backend-migration/modules/
git commit -m "docs(backend-migration): T4 closure for assistant snake-case realignment — handoff + module log + playbook lessons"
git push origin feat/backend-migration-coordinator 2>&1 | tail -3
```

- [ ] **Step T4.8: Cleanup worktrees (optional, user-directed)**

Only if user confirms:
```bash
cd /Users/zhoukai/Documents/github/aionui-backend
git worktree remove /Users/zhoukai/Documents/worktrees/aionui-backend-assistant-camel
cd /Users/zhoukai/Documents/github/AionUi
git worktree remove /Users/zhoukai/Documents/worktrees/aionui-assistant-camel
```

Leave worktrees intact if the user wants to inspect / retry — no rush
on removal.

- [ ] **Step T4.9: Final DoD sweep**

Run from AionUi main:
```bash
# no camelCase residue in frontend (outside legacy + external protocol)
grep -rE "\.(nameI18n|descriptionI18n|sortOrder|presetAgentType|enabledSkills|customSkillNames|disabledBuiltinSkills|contextI18n|promptsI18n|lastUsedAt)\b" src/ 2>&1 | grep -v 'migrateAssistants\|LegacyAssistant'
# expect: 0

grep -n 'fileName\|conversationId\|modelId\|configId' src/common/adapter/ipcBridge.ts | grep -E 'httpPost|httpPut|httpGet|(p) => \('
# expect: fileName/conversationId/modelId/configId only appear as local TS param names, never inside body transformers
```

From backend main:
```bash
grep -rln 'rename_all = "camelCase"' crates/aionui-api-types/ crates/aionui-assistant/
# expect: 0 files (only channel/plugins remains, which is out of scope)

grep -cE '"(presetAgentType|nameI18n|descriptionI18n|enabledSkills|customSkillNames|disabledBuiltinSkills|promptsI18n|ruleFile|skillFile)"' crates/aionui-app/assets/builtin-assistants/assistants.json
# expect: 0
```

All green → pilot done. Notify user.

**Task complete =** all DoD sweep greps return expected values AND
handoff + module log + playbook all pushed AND final SHAs recorded in
handoff AND user notified.

---

## Runbook — common team-mode hazards

- **Teammate zombie (10 min silent, no git/TaskUpdate):** per playbook,
  autonomous replace without user confirmation. Delete from
  `~/.claude/teams/<team>/config.json` members array, `rm` inbox,
  respawn with ≤ 40 line prompt, `git fetch && git reset --hard
  origin/<branch>` built into prompt so new agent self-heals.

- **`stat -f` on symlink:** use `stat -L` / `ls -laL`, never bare
  `stat -f`.

- **Teammate "task complete" unpushed:** run
  `git log origin/<branch> --oneline -1`. If SHA missing, teammate is
  lying (by mental-model error, not malice). SendMessage with
  "NOT A REPLAY — commit + push now" + exact git commands. Do not
  mark task completed coordinator-side until verified.

- **Merge conflict on `ipcBridge.ts` during T4.1:** pilot-side wins →
  `git checkout --theirs`. Verify manually because some untouched
  functions might be in the same file.

- **Release binary symlink stale:** the whole reason T4.2 copies the
  binary into `$TMPDIR` alone. Don't trust `~/.cargo/bin/aionui-backend`
  for the smoke — use the direct copy.
