# aionui-team HTTP API & Data Model Investigation Report

**Task:** Investigate aionui-team's REST API layer and data model for comprehensive client understanding.

**Scope:** Full REST endpoint mapping, DB schema, core types, and client-side requirements.

---

## 1. REST Endpoints Summary

All team endpoints are defined in `crates/aionui-team/src/routes.rs` (lines 24-48) and registered with auth middleware in `crates/aionui-app/src/lib.rs`.

**Auth Status:** All endpoints require `CurrentUser` extension (auth middleware validates JWT token). No public endpoints.

### 1.1 Team Lifecycle Management

| Endpoint | Method | Request Type | Response Type | Auth | Purpose |
|----------|--------|--------------|---------------|------|---------|
| `/api/teams` | POST | `CreateTeamRequest` | `ApiResponse<TeamResponse>` | ✅ Required | Create a new team with initial agents |
| `/api/teams` | GET | (none) | `ApiResponse<Vec<TeamResponse>>` | ✅ Required | List all teams |
| `/api/teams/{id}` | GET | (none) | `ApiResponse<TeamResponse>` | ✅ Required | Fetch single team by ID |
| `/api/teams/{id}` | DELETE | (none) | `ApiResponse<()>` | ✅ Required | Delete entire team (cascades to agents, mailbox, tasks) |
| `/api/teams/{id}/name` | PATCH | `RenameTeamRequest` | `ApiResponse<()>` | ✅ Required | Rename team |

**File Evidence:**
- Routes definition: `crates/aionui-team/src/routes.rs:26-28`
- Handlers: `routes.rs:50-92` (create_team, list_teams, get_team, remove_team, rename_team)

---

### 1.2 Agent Management

| Endpoint | Method | Request Type | Response Type | Auth | Purpose |
|----------|--------|--------------|---------------|------|---------|
| `/api/teams/{id}/agents` | POST | `AddAgentRequest` | `ApiResponse<TeamAgentResponse>` | ✅ Required | Add agent to existing team |
| `/api/teams/{id}/agents/{slot_id}` | DELETE | (none) | `ApiResponse<()>` | ✅ Required | Remove agent from team |
| `/api/teams/{id}/agents/{slot_id}/name` | PATCH | `RenameAgentRequest` | `ApiResponse<()>` | ✅ Required | Rename agent |

**File Evidence:**
- Routes: `crates/aionui-team/src/routes.rs:29-36`
- Handlers: `routes.rs:100-134` (add_agent, remove_agent, rename_agent)
- Agent model: `crates/aionui-team/src/types.rs:88-123`

**Key Detail:** Each agent gets its own `conversation_id` created automatically. Agents are stored as JSON array in the `teams.agents` column. The first agent in a team creation request automatically becomes the `lead_agent_id`.

---

### 1.3 Messaging & Session Control

| Endpoint | Method | Request Type | Response Type | Auth | Purpose |
|----------|--------|--------------|---------------|------|---------|
| `/api/teams/{id}/messages` | POST | `SendTeamMessageRequest` | `ApiResponse<()>` | ✅ Required | Send message to team lead's mailbox (wakes team) |
| `/api/teams/{id}/agents/{slot_id}/messages` | POST | `SendAgentMessageRequest` | `ApiResponse<()>` | ✅ Required | Send message to specific agent's mailbox |
| `/api/teams/{id}/session` | POST | (none) | `ApiResponse<()>` | ✅ Required | Ensure session is running (idempotent) |
| `/api/teams/{id}/session` | DELETE | (none) | `ApiResponse<()>` | ✅ Required | Stop session (no-op if already stopped) |

**File Evidence:**
- Routes: `crates/aionui-team/src/routes.rs:38-46`
- Handlers: `routes.rs:136-173` (send_message, send_message_to_agent, ensure_session, stop_session)

---

## 2. Request & Response Type Definitions

All types in `crates/aionui-api-types/src/team.rs` (file serves as single source of truth for HTTP contracts).

### 2.1 Request Types (DTOs)

#### `CreateTeamRequest` (lines 27-30)
```rust
{
  "name": string,
  "agents": [
    {
      "name": string,
      "role": string,        // "lead" or "teammate"
      "backend": string,     // "acp", "claude", "gemini", "nanobot", "aionrs", etc.
      "model": string,       // e.g., "claude-opus", "gpt-4"
      "custom_agent_id"?: string  // Optional registry ID
    }
  ]
}
```
**Constraint:** At least one agent required (validated in service.rs:48-51).

#### `AddAgentRequest` (lines 47-54)
```rust
{
  "name": string,
  "role": string,
  "backend": string,
  "model": string,
  "custom_agent_id"?: string
}
```

#### `RenameTeamRequest` (lines 34-36)
```rust
{ "name": string }
```

#### `RenameAgentRequest` (lines 58-60)
```rust
{ "name": string }
```

#### `SendTeamMessageRequest` (lines 71-73)
```rust
{ "content": string }
```

#### `SendAgentMessageRequest` (lines 79-81)
```rust
{ "content": string }
```

### 2.2 Response Types (DTOs)

#### `TeamResponse` (lines 108-116)
```rust
{
  "id": string,
  "name": string,
  "agents": [TeamAgentResponse],
  "lead_agent_id"?: string,
  "created_at": integer (ms since epoch),
  "updated_at": integer (ms since epoch)
}
```

#### `TeamAgentResponse` (lines 91-102)
```rust
{
  "slot_id": string,
  "name": string,
  "role": string,               // "lead" or "teammate"
  "conversation_id": string,
  "backend": string,
  "model": string,
  "custom_agent_id"?: string,
  "status"?: string             // "idle", "working", "thinking", "tool_use", "completed", "error"
}
```

#### `TeamListResponse` (line 119)
```rust
type TeamListResponse = Vec<TeamResponse>
```

---

## 3. Database Schema

All tables in `crates/aionui-db/migrations/001_initial_schema.sql`.

### 3.1 teams table (lines 241-252)

```sql
CREATE TABLE teams (
    id             TEXT PRIMARY KEY NOT NULL,
    user_id        TEXT    NOT NULL DEFAULT 'system_default_user',
    name           TEXT    NOT NULL,
    workspace      TEXT    NOT NULL DEFAULT '',
    workspace_mode TEXT    NOT NULL DEFAULT 'shared',
    agents         TEXT    NOT NULL DEFAULT '[]',     -- JSON array of TeamAgent objects
    lead_agent_id  TEXT,
    session_mode   TEXT,
    created_at     INTEGER NOT NULL,
    updated_at     INTEGER NOT NULL
);
```

**Key Points:**
- `agents` column: Stores JSON-serialized `Vec<TeamAgent>` (not normalized; all team state in one column)
- `lead_agent_id`: slot_id of the team's primary agent
- `workspace` and `workspace_mode`: Currently unused (reserved for future multi-workspace feature)
- `session_mode`: Currently unused

### 3.2 mailbox table (lines 256-267)

```sql
CREATE TABLE mailbox (
    id            TEXT    PRIMARY KEY NOT NULL,
    team_id       TEXT    NOT NULL,
    to_agent_id   TEXT    NOT NULL,      -- slot_id or "lead"
    from_agent_id TEXT    NOT NULL,
    type          TEXT    NOT NULL CHECK (type IN ('message', 'idle_notification', 'shutdown_request')),
    content       TEXT    NOT NULL,
    summary       TEXT,                  -- Optional summary for idle notifications
    files         TEXT,
    read          INTEGER NOT NULL DEFAULT 0,
    created_at    INTEGER NOT NULL
);

-- Indexes for atomic read-unread-mark operations
CREATE INDEX idx_mailbox_team_to_read ON mailbox(team_id, to_agent_id, read);
CREATE INDEX idx_mailbox_team_id ON mailbox(team_id);
```

**Message Types:**
- `message`: User-initiated or agent-to-agent communication
- `idle_notification`: Agent signals completion (summary field typically set)
- `shutdown_request`: Graceful shutdown signal

### 3.3 team_tasks table (lines 276-289)

```sql
CREATE TABLE team_tasks (
    id          TEXT    PRIMARY KEY NOT NULL,
    team_id     TEXT    NOT NULL,
    subject     TEXT    NOT NULL,
    description TEXT,
    status      TEXT    NOT NULL DEFAULT 'pending'
                        CHECK (status IN ('pending', 'in_progress', 'completed', 'deleted')),
    owner       TEXT,                     -- slot_id of responsible agent
    blocked_by  TEXT    NOT NULL DEFAULT '[]',   -- JSON array of task IDs
    blocks      TEXT    NOT NULL DEFAULT '[]',   -- JSON array of task IDs
    metadata    TEXT,                     -- Arbitrary JSON
    created_at  INTEGER NOT NULL,
    updated_at  INTEGER NOT NULL
);

CREATE INDEX idx_team_tasks_team_id ON team_tasks(team_id);
```

**Task Dependency Model:**
- DAG-based: `blocked_by` lists tasks that must complete before this task
- `blocks` lists tasks waiting on this one
- Atomic append/remove operations in repository (lines 93-101 in `team.rs`)

---

## 4. Core Domain Types

All in `crates/aionui-team/src/types.rs`.

### 4.1 TeammateRole enum (lines 13-36)

```rust
pub enum TeammateRole {
    #[serde(alias = "leader")]
    Lead,
    Teammate,
}
```

**Parsing:** Accepts "lead", "leader", "teammate" (case-sensitive).

### 4.2 TeammateStatus enum (lines 44-82)

```rust
pub enum TeammateStatus {
    #[serde(alias = "pending")]
    Idle,
    #[serde(alias = "active")]
    Working,
    Thinking,
    ToolUse,
    #[serde(alias = "completed")]
    Completed,
    #[serde(alias = "failed")]
    Error,
}
```

**Aliases:** Support legacy AionUi names ("pending"→Idle, "active"→Working, etc.).

### 4.3 TeamAgent struct (lines 89-123)

Core team member representation stored in JSON:

```rust
pub struct TeamAgent {
    pub slot_id: String,
    pub name: String,
    pub role: TeammateRole,
    pub conversation_id: String,
    pub backend: String,
    pub model: String,
    pub custom_agent_id: Option<String>,  // Registry lookup ID
    pub status: Option<TeammateStatus>,
    pub conversation_type: Option<String>,
    pub cli_path: Option<String>,
}
```

**Implementation Detail:** Agents do NOT have individual DB rows; they're embedded in the team's JSON `agents` column. Updates to agent lists re-serialize the entire array.

### 4.4 Team struct (lines 130-138)

```rust
pub struct Team {
    pub id: String,
    pub name: String,
    pub agents: Vec<TeamAgent>,
    pub lead_agent_id: Option<String>,
    pub created_at: TimestampMs,
    pub updated_at: TimestampMs,
}
```

### 4.5 MailboxMessage & MailboxMessageType

Defined in types.rs:177-190 for domain logic but NOT exposed via HTTP (used internally by session layer).

### 4.6 TeamTask & TaskStatus

Task board representation (types.rs:232-248):

```rust
pub struct TeamTask {
    pub id: String,
    pub team_id: String,
    pub subject: String,
    pub description: Option<String>,
    pub status: TaskStatus,
    pub owner: Option<String>,         // slot_id
    pub blocked_by: Vec<String>,       // Task IDs
    pub blocks: Vec<String>,           // Task IDs
    pub metadata: Option<serde_json::Value>,
    pub created_at: TimestampMs,
    pub updated_at: TimestampMs,
}
```

**Note:** No HTTP endpoints currently exposed for task management; only DB/MCP layer (see Task #2).

---

## 5. Data Access Layer

### 5.1 Repository Interface

`crates/aionui-db/src/repository/team.rs:ITeamRepository` (trait, lines 28-105):

**Team CRUD:**
- `create_team(&row: TeamRow)` → Insert
- `list_teams()` → All teams ordered by created_at
- `get_team(team_id)` → Single team or None
- `update_team(team_id, params: UpdateTeamParams)` → Selective field updates
- `delete_team(team_id)` → Full deletion

**Mailbox Operations:**
- `write_message(row)` → Insert single message
- `read_unread_and_mark(team_id, to_agent_id)` → Atomic read + mark-read
- `get_history(team_id, to_agent_id, limit?)` → Message history pagination
- `delete_mailbox_by_team(team_id)` → Cascade delete

**Task Operations:**
- `create_task(row)`, `find_task_by_id()`, `update_task()`, `list_tasks(team_id)`
- `append_to_blocks(task_id, blocked_task_id)` → Transactional JSON array append
- `remove_from_blocked_by(task_id, unblocked_task_id)` → Transactional array remove
- `delete_tasks_by_team(team_id)` → Cascade delete

### 5.2 SQLite Implementation

`crates/aionui-db/src/repository/sqlite_team.rs:SqliteTeamRepository` implements all trait methods:

- Selective UPDATE queries for team updates (only modified fields)
- Parameterized queries throughout (no SQL injection risk)
- JSON array manipulation via SQLx for task dependencies

---

## 6. Service Layer & Business Logic

`crates/aionui-team/src/service.rs:TeamSessionService`

### 6.1 Team Creation Flow (lines 43-133)

1. Validate: At least one agent in request
2. Generate: Unique `team_id` and `slot_id` per agent
3. Assign Roles: First agent → Lead, others → parse request.role
4. Create Conversations: Call `ConversationService::create()` for each agent
5. Build Agents Array: Serialize as JSON string
6. Insert Team Row: Single DB insert
7. Emit Event: (not shown in HTTP routes; handled by session layer)

**Evidence:** Service calls are synchronous on conversation service (lines 83-89).

### 6.2 Agent Addition (lines 198-271)

1. Load team from DB
2. Parse role, create conversation
3. Append to agents array
4. Re-serialize and update DB
5. If session active: notify session of new agent (line 265-267)

### 6.3 Agent Removal (lines 273-316)

1. Load team
2. Find agent by slot_id
3. Delete agent's conversation (cascades to messages)
4. Re-serialize agents array
5. Update team
6. If session active: notify session (line 311-313)

### 6.4 Session Management (lines 357-422)

Sessions are in-memory: `DashMap<String, TeamSession>` in service (line 24).

- `ensure_session(team_id)` → Create if missing (line 357-382)
- `stop_session(team_id)` → Remove from map (line 384-388)
- Message routing: Sessions field (line 390-409)

**Critical:** Messages require an active session; returns `SessionNotFound` error if not.

---

## 7. Agent Backend Type Parsing

`crates/aionui-team/src/service.rs:parse_agent_type()` (lines 453-469)

Maps string backends to `aionui_common::AgentType`:

| Input | Output | Logic |
|-------|--------|-------|
| "acp" | AgentType::Acp | Exact match |
| "claude" / "gemini" / "qwen" | AgentType::Acp | Valid AcpBackend variant → always Acp |
| "nanobot" | AgentType::Nanobot | Exact match |
| "remote" | AgentType::Remote | Exact match |
| "aionrs" | AgentType::Aionrs | Exact match |
| "openclaw-gateway" | AgentType::OpenclawGateway | Exact match |
| Other | Error | Unsupported |

**Note:** "acp" as AgentType string is NOT stored in conversation.extra["backend"] to avoid deserialization failure (lines 505-531 in types.rs tests document this design).

---

## 8. REST API Error Handling

All handlers return `Result<T, AppError>` (axum integration handles serialization to JSON).

**Common Errors (from types.rs):**
- `TeamError::TeamNotFound(team_id)` → 404
- `TeamError::AgentNotFound(slot_id)` → 404
- `TeamError::SessionNotFound(team_id)` → 400 (user must POST `/api/teams/{id}/session` first)
- `TeamError::InvalidRequest(reason)` → 400
- JSON parse errors → 400

---

## 9. WebSocket Events (Not HTTP, but Relevant)

**Events emitted by session layer (not directly exposed in HTTP routes):**

| Event | Payload Type | Context |
|-------|--------------|---------|
| `team.agent.status` | `TeamAgentStatusPayload` | Agent runtime status changed |
| `team.agent.spawned` | `TeamAgentSpawnedPayload` | New agent dynamically added |
| `team.agent.removed` | `TeamAgentRemovedPayload` | Agent removed |
| `team.agent.renamed` | `TeamAgentRenamedPayload` | Agent name changed |

**File:** Payload types in `crates/aionui-api-types/src/team.rs:125-163`.

---

## 10. Authentication & Authorization

**Current State:**
- All endpoints require `Extension(CurrentUser)` (JWT middleware)
- No role-based access control at HTTP layer
- User can only operate on teams under their own `user_id` (enforced in removal flow)

**Limitation:** `list_teams()` returns **all teams** regardless of ownership (line 60-65 in routes.rs). This is likely a bug or intentional for admin views.

---

## 11. Key Architectural Decisions

### 11.1 JSON Embedding (agents, blocked_by, blocks)

**Why:** Simplified in-memory state management. Avoids N+1 queries for team.agents.

**Trade-off:** Every agent update re-serializes entire array; no partial updates.

**Affected Columns:**
- `teams.agents`: Array of TeamAgent
- `team_tasks.blocked_by` / `blocks`: Task ID arrays

### 11.2 Session as In-Memory State

**Why:** Decouples HTTP state from DB; supports in-process MCP coordination.

**Trade-off:** Sessions lost on server restart; multi-process deployments need session affinity.

### 11.3 Conversation per Agent

**Why:** Reuses existing conversation/message model; agents have independent message histories.

**Constraint:** Conversations are created in aionui-conversation crate; team service has no direct DB write for messages.

### 11.4 No Normalization of Mailbox

**Why:** Flat table, no foreign key on agents (agents are JSON).

**Implication:** Deleting an agent doesn't orphan mailbox messages (no constraint); cascade delete handled in service layer.

---

## 12. Missing / Planned Features

Based on unused schema columns:

| Column | Status | Note |
|--------|--------|------|
| `teams.workspace` | Unused | Reserved for multi-workspace support |
| `teams.workspace_mode` | Unused | "shared", "private" modes TBD |
| `teams.session_mode` | Unused | Future: different session lifecycle policies |
| `mailbox.files` | Unused | Placeholder for attachment support |
| `team_tasks.*` | Unused | Full task management layer exists but no HTTP endpoints |

---

## 13. Answer to Key Question: Electron Client Requirements

### Can the Electron client operate teams via HTTP REST API alone?

**YES, with one caveat:**

#### What IS Pure REST (no special client logic needed):

1. **Team CRUD:** Create, list, get, delete, rename → straight HTTP ✅
2. **Agent Management:** Add, remove, rename → straight HTTP ✅
3. **Messaging:** Send to team or agent → straight HTTP ✅
4. **Session Lifecycle:** Ensure/stop session → straight HTTP ✅

#### What REQUIRES Client-Side Logic:

1. **Real-time Status Updates:**
   - Agent status (idle→working) is **only broadcast via WebSocket** (team.agent.status event)
   - HTTP polling would be inefficient
   - **Client needs:** WebSocket listener or periodic polling fallback

2. **Conversation Browsing:**
   - Each agent has a `conversation_id`; agent messages are stored in `conversations` table
   - **NOT accessible via team endpoints** (no GET /api/teams/{id}/agents/{slot_id}/messages)
   - **Client needs:** Make separate GET call to conversation routes (aionui-conversation crate)

3. **Task Management:**
   - Task CRUD exists in DB but **NO HTTP endpoints** exposed
   - **Client needs:** Either call MCP layer or wait for HTTP endpoints to be added

4. **Session Awareness:**
   - Sending messages requires session to be active
   - **Client needs:** Call POST /api/teams/{id}/session first before sending messages
   - **Error:** SessionNotFound if not done

#### HTTP + WebSocket Hybrid Model (Recommended):

```
Electron Client
  ├─ HTTP: Team/agent lifecycle (create, rename, delete)
  ├─ HTTP: Messaging (POST /api/teams/{id}/messages)
  ├─ HTTP: Ensure session (POST /api/teams/{id}/session)
  ├─ WebSocket: Status updates (team.agent.status)
  ├─ HTTP: Conversation history (GET /api/conversations/{id}/messages)
  └─ MCP: Task board, advanced coordination (see Task #2)
```

---

## 14. Endpoint Completeness Checklist

| Feature | GET | POST | PATCH | DELETE | Notes |
|---------|-----|------|-------|--------|-------|
| List teams | ✅ | - | - | - | No filter; returns all |
| Create team | - | ✅ | - | - | Agents required |
| Get team | ✅ | - | - | - | Single fetch |
| Rename team | - | - | ✅ | - | Via /name suffix |
| Delete team | - | - | - | ✅ | Cascades agents, mailbox, tasks |
| Add agent | - | ✅ | - | - | Via /agents suffix |
| Rename agent | - | - | ✅ | - | Via /agents/{slot}/name |
| Remove agent | - | - | - | ✅ | Via /agents/{slot} |
| Send to team | - | ✅ | - | - | Via /messages suffix |
| Send to agent | - | ✅ | - | - | Via /agents/{slot}/messages |
| Ensure session | - | ✅ | - | - | Via /session suffix |
| Stop session | - | - | - | ✅ | Via /session suffix |
| Task CRUD | ❌ | ❌ | ❌ | ❌ | Not exposed (see Task #2) |
| Mailbox | ❌ | ❌ | ❌ | ❌ | Not exposed (internal use) |

---

## 15. Summary Table: What Electron Client Should Know

| Concern | Answer | Evidence |
|---------|--------|----------|
| All endpoints auth? | Yes, all require JWT | routes.rs Extension(CurrentUser) |
| Can bulk-fetch teams? | Yes, GET /api/teams (returns all) | routes.rs:60-65 |
| Can fetch team details? | Yes, GET /api/teams/{id} | routes.rs:67-73 |
| Agent status real-time? | Only via WebSocket (team.agent.status) | Not in HTTP routes |
| Can browse agent messages? | Via conversation_id → GET /api/conversations/{id}/messages | types.rs line 95 |
| Session required before messaging? | Yes, POST /api/teams/{id}/session | service.rs:391-396 |
| Can update task board? | Not via HTTP (use MCP) | Task #2 investigation |
| Delete cascades? | Yes: team → agents, mailbox, tasks | service.rs:162-173 |
| Concurrent teams? | Single user can have many; no team.user_id check in list | Likely bug (line 62) |
| Agent re-ordering? | No endpoints to reorder agents | JSON array, would need PUT /api/teams/{id}/agents |

---

## Appendix: File Reference Map

```
crates/aionui-team/
  ├─ src/
  │   ├─ routes.rs              ← All HTTP endpoints + handlers
  │   ├─ service.rs             ← Business logic (team creation, agent ops, sessions)
  │   ├─ types.rs               ← Domain types (Team, TeamAgent, TeammateRole, etc.)
  │   ├─ session.rs             ← TeamSession (in-memory)
  │   ├─ scheduler.rs           ← Scheduler (team lifecycle automation)
  │   ├─ mailbox.rs             ← Mailbox semantics
  │   ├─ task_board.rs          ← Task board management
  │   ├─ events.rs              ← Event definitions
  │   └─ mcp/                   ← MCP tools & server (see Task #2)

crates/aionui-api-types/
  └─ src/team.rs               ← Request/response DTOs (single source of truth)

crates/aionui-db/
  ├─ src/repository/
  │   ├─ team.rs               ← ITeamRepository trait
  │   └─ sqlite_team.rs        ← SqliteTeamRepository implementation
  └─ migrations/001_initial_schema.sql  ← teams, mailbox, team_tasks tables

crates/aionui-app/
  └─ src/lib.rs               ← team_routes() registration
```

---

**Report Completed:** 2026-04-29  
**Investigator:** api-investigator  
**Status:** Ready for handoff to scheduler/MCP investigator (Task #2)
