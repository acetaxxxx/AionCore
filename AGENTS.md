# AGENTS.md

Project-specific rules and conventions for AI assistants and contributors.

## Build & Test

```bash
cargo build                          # Build (debug)
cargo build --release                # Build (release)
cargo test --workspace               # Run all tests
cargo clippy --workspace -- -D warnings  # Lint (warnings = errors)
cargo fmt --all                      # Format
cargo fmt --all -- --check           # Format check (CI enforces this)
```

Binary name: `aionui-backend` (produced by `crates/aionui-app`).

## Architecture

Cargo workspace with 17 crates under `crates/`. Dependencies flow downward:

- `aionui-common` — shared types, enums, error types, crypto utilities
- `aionui-api-types` — API request/response types, shared across crates
- `aionui-db` — SQLite database layer (sqlx), repository traits and implementations
- `aionui-auth` — JWT, CSRF, password hashing, auth middleware
- `aionui-realtime` — WebSocket manager, event broadcasting
- Domain crates (`aionui-conversation`, `aionui-channel`, `aionui-team`, `aionui-cron`, `aionui-file`, `aionui-office`, `aionui-shell`, `aionui-mcp`, `aionui-ai-agent`, `aionui-extension`, `aionui-system`) — each owns its routes, service, and tests
- `aionui-app` — top-level binary, composes all crates into the axum server

Never introduce circular dependencies or upward references.

## Test Organization

| Location | What goes there |
|----------|----------------|
| Inline `#[cfg(test)]` in each `.rs` file | Unit tests for that module's internals |
| `crates/<crate>/tests/` | Integration / E2E tests for that crate |

## Code Style

- Rust 2024 edition, stable toolchain
- `cargo clippy` must pass without warnings
- `cargo fmt` must pass
- Comments in English, commit messages in English

## Architecture Rules

> For detailed background and design decisions, see [ARCHITECTURE.md](./ARCHITECTURE.md).

### Crate Hierarchy & Dependencies

- Four layers: Foundation → Capability → Domain → Composition
- ✅ Upper layers may depend on lower layers (including cross-layer)
- ✅ Same-layer interaction through trait abstractions only
- ❌ No lower-layer depending on upper-layer
- ❌ No circular dependencies
- Changes to foundation crates (common, api-types, db) require impact assessment

### Domain Crate Structure

Every domain crate must follow:
- `lib.rs` — module exports only, no business logic
- `routes.rs` — export `domain_routes(state) -> Router`, handlers do request/response transformation only
- `service.rs` — sole location for business logic, must not import axum
- `state.rs` — `#[derive(Clone)]` RouterState holding Arc-wrapped dependencies

### API Conventions

- Route prefix: `/api/`
- Resource names: kebab-case
- Response format: `ApiResponse<T>` (success) / `ErrorResponse` (failure)
- All request/response types defined in `aionui-api-types`
- `aionui-api-types` must NOT depend on axum/tower or any HTTP framework

### WebSocket Events

- Format: `domain.camelCaseAction` (two-level structure)
- Message type: `WebSocketMessage<T>` (name + data)
- Existing kebab-case or three-level names are legacy — new events must follow the convention

### Data Layer

- Repository traits in `aionui-db`, prefixed with `I`
- Concrete implementations prefixed with `Sqlite`
- Row models in `aionui-db/src/models/`
- Params objects co-located in repository files
- Migrations: `NNN_descriptive_name.sql`, no manual DB modifications
- Services depend on traits, never on concrete implementations

### Dependency Injection

- `AppServices` is the sole service construction center
- Domain crates only define RouterState, never construct their own dependencies
- All assembly happens in `aionui-app`'s `build_*_state()` functions

### Security

- New endpoints must be evaluated for auth middleware requirement
- State-changing operations must be CSRF-protected
- Sensitive operations should have rate limiting
- Error responses must not leak internal details
- Secrets must never be hardcoded

### Testing

- Unit tests: `#[cfg(test)]` inline in corresponding `.rs` files
- Integration tests: `crates/<crate>/tests/` directory
- E2E tests: `crates/aionui-app/tests/`
- Database tests use `init_database_memory()`
- Prefer real in-memory DB over mocks; mock only to isolate unneeded dependencies
- New features must include tests

#### Test Failure Handling

When a test fails, do NOT modify the test to make it pass. First determine:

1. **Test assertion still represents correct behavior** → fix implementation, not the test
2. **Requirements/interface intentionally changed** → may update test, but must confirm:
   - The change is intentional (not an unintended side effect)
   - New assertions still validate meaningful behavior
3. **Uncertain** → stop, trace back the change, clarify before proceeding

Prohibited:
- ❌ Deleting failing tests to "fix" the problem
- ❌ Weakening specific assertions to vague ones (e.g., `assert_eq!(status, 201)` → `assert!(status.is_success())`)
