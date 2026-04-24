---
title: Provider API Softening for Pre-Launch Frontend Migration
date: 2026-04-24
status: draft
scope: backend (aionui-backend)
companion_frontend_spec: AionUi/docs/backend-migration/specs/2026-04-24-model-config-frontend-migration-design.md
---

# Provider API Softening — Backend Design Spec

## Background

Pre-launch. Frontend is migrating from a local `model.config` store to
`/api/providers/*`. Three current constraints in the provider API make
the frontend migration require contortions. Since we're pre-launch, we
remove the constraints instead of working around them.

## Changes

### 1. `CreateProviderRequest` — accept optional id + per-model fields

File: `crates/aionui-api-types/src/provider.rs`.

```rust
pub struct CreateProviderRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    pub platform: String,
    pub name: String,
    pub base_url: String,
    pub api_key: String,
    #[serde(default)]
    pub models: Vec<String>,
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default)]
    pub capabilities: Vec<ModelCapability>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_limit: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_protocols: Option<HashMap<String, String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_enabled: Option<HashMap<String, bool>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_health: Option<HashMap<String, ModelHealthStatus>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bedrock_config: Option<BedrockConfig>,
}
```

Service change in `crates/aionui-system/src/provider.rs::create()`:

- If `req.id` is `Some`, trim + validate it; else `Uuid::new_v4().to_string()`.
- **Validation is lenient, not strict UUID.** Frontend's `uuid()` util
  returns an 8-char hex string by default (see `src/common/utils/utils.ts:7`
  in AionUi), not a UUID. Historical provider ids are short hex strings.
  Spec §1 "validate UUID" was a wording error — the real requirement is
  "non-empty, safe-shaped string id":
  - `1..=128` chars after trim
  - charset `[A-Za-z0-9_-]` only (blocks SQL/path/injection footguns)
  - reject if already taken at the repo layer
- Persist `model_protocols / model_enabled / model_health` on create
  (serialize to JSON, pass through `CreateProviderParams` — those fields
  already exist on the params struct, they're just hardcoded to `None`
  today at lines 52–54).
- Reject create if id is already taken (repo-level conflict).

### 2. `ProviderResponse.api_key` — return plaintext

Pre-launch, no leak concern. The frontend is the only consumer and
holds the same key locally already. Masking adds footguns without value.

File: `crates/aionui-system/src/provider.rs`.

- `row_to_response`: decrypt `api_key_encrypted` and return plaintext in
  `api_key`. Remove the mask helper (`mask_api_key` or similar) and its
  unit tests.
- Keep storage encrypted at rest — only the response is plaintext.

Update the doc comment on `ProviderResponse::api_key` to reflect
plaintext.

### 3. `UpdateProviderRequest` — no schema change

Once `api_key` round-trips plaintext, frontend's existing "send back
the whole IProvider" pattern works without a guard. No change required.

### 4. Route / handler — no change

Existing `POST /api/providers` handler (`routes.rs::create_provider`)
already passes the request straight to `provider_service.create`. Only
the request schema and the service `create()` body need editing.

## Tests to add / flip

`crates/aionui-api-types/src/provider.rs`:

- `test_create_provider_request_with_id` — verifies optional id
  round-trips.
- `test_create_provider_request_with_per_model_fields` — verifies
  model_enabled / model_health / model_protocols deserialize on create.
- `test_provider_response_api_key_plaintext` — replaces the masking
  test; asserts the response `api_key` equals the encrypted-then-
  decrypted value.

`crates/aionui-system/src/provider.rs`:

- Flip any test that asserted `api_key` contains `***`.
- Add `test_create_with_provided_id` and `test_create_persists_per_model_fields`.
- Delete the mask helper and its tests.

`crates/aionui-system/tests/providers_e2e.rs` (if present): flip any
assertion that expected `***` in `api_key`.

## Definition of Done

- [ ] `cargo fmt --all -- --check` clean
- [ ] `cargo test -p aionui-api-types` green (new + flipped tests)
- [ ] `cargo test -p aionui-system` green
- [ ] `cargo test --test '*'` baseline unchanged for everything else
- [ ] `cargo clippy --workspace -- -D warnings` baseline unchanged
- [ ] Live probe:
  ```
  # Accepts frontend-style 8-char hex id:
  POST /api/providers {"id":"a1b2c3d4","platform":"openai","name":"test","base_url":"https://a","api_key":"sk-xxx","models":["gpt-4"],"model_enabled":{"gpt-4":true}}
  → 201, response.id == "a1b2c3d4", response.api_key == "sk-xxx" (not masked), response.model_enabled == {"gpt-4": true}
  # Also accepts real UUID:
  POST /api/providers {"id":"11111111-1111-4111-8111-111111111111", ...} → 201, id preserved
  # Rejects unsafe:
  POST /api/providers {"id":"../etc/passwd", ...} → 400
  POST /api/providers {"id":"", ...} → 400
  POST /api/providers {"id":"x".repeat(200), ...} → 400
  GET /api/providers → includes the above, api_key == "sk-xxx"
  ```

## Rollout

Backend branch: `feat/model-sync-be` (worktree
`/Users/zhoukai/Documents/worktrees/aionui-backend-model-sync-be`, based
on `origin/feat/builtin-skills`).

Ships before frontend T2. Not a breaking change: `id` optional, new
per-model fields optional, `api_key` unmasking is observable only to
the one caller we're updating.
