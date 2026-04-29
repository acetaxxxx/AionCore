# aionui-team Internal Scheduling Architecture Investigation

**Author:** Investigator Agent  
**Date:** 2026-04-29  
**Scope:** scheduler.rs, mailbox.rs, session.rs, mcp/server.rs, mcp/tools.rs, prompts.rs, events.rs, task_board.rs

---

## 1. TeammateManager State Machine

### States & Transitions

The `TeammateManager` (crates/aionui-team/src/scheduler.rs:84) maintains per-agent state in a `HashMap<slot_id, AgentSlot>` where each slot tracks:

```rust
struct AgentSlot {
    agent: TeamAgent,
    status: TeammateStatus,  // Idle | Working | Thinking | ToolUse | Completed | Error
}
```

**State Transitions (scheduler.rs:120-198):**

1. **Idle → Working**  
   - Triggered by: `try_wake()` (scheduler.rs:164)
   - Checks: Agent must be in `Idle` state; returns `None` if already `Working` (anti-duplicate-wake)
   - Sets agent status to `Working` via `set_status()` (scheduler.rs:120)
   - Broadcasts event: `"team.agent.status"` (scheduler.rs:129, events.rs:28)

2. **Working → Idle**  
   - Triggered by: `mark_idle()` (scheduler.rs:182)
   - Automatically called during `finalize_turn()` (scheduler.rs:281-302)
   - Sets agent status to `Idle` via `set_status()` (scheduler.rs:183)
   - Exception: If agent includes `IdleNotification` action in turn response, `finalize_turn()` skips double-idle (scheduler.rs:297-301)

3. **Leader Wake Signal**  
   - After **all teammates** become idle, `mark_idle()` triggers `maybe_wake_leader_when_all_idle()` (scheduler.rs:197, 482)
   - Conditions checked (scheduler.rs:489-520):
     - Has teammates (not solo team)
     - All teammates status == `Idle`
     - Lead agent exists and status == `Idle`
   - Returns lead's `slot_id` to caller (scheduler.rs:530)

4. **No Self-Wake for Lead**  
   - When lead calls `mark_idle()`, returns `None` immediately (scheduler.rs:185-194)
   - Prevents lead from infinite self-wake loops

### Key Constants

- `WAKE_TIMEOUT_MS = 60_000` (scheduler.rs:16) — timeout value (currently unused in code; stored for future reference)

---

## 2. Mailbox Message Flow

### Message Architecture (mailbox.rs:11-72)

The `Mailbox` wraps the database repository (`ITeamRepository`) to implement async message queue semantics:

**Data Structure:**
```rust
pub async fn write(
    &self,
    team_id: &str,
    to_agent_id: &str,
    from_agent_id: &str,
    msg_type: MailboxMessageType,    // Message | IdleNotification | ShutdownRequest
    content: &str,
    summary: Option<&str>,
) -> Result<MailboxMessage, TeamError>
```

**Message Types (types.rs, inferred from mailbox.rs & scheduler.rs):**

| Type | Writer | Reader | Trigger |
|------|--------|--------|---------|
| `Message` | Any agent via MCP tool `team_send_message` (server.rs:339) | Target agent in `read_unread()` | Agent-to-agent communication |
| `IdleNotification` | Agent via MCP tool implicit in turn response | Lead agent in `read_unread()` | Teammate finishing work (scheduler.rs:411-435) |
| `ShutdownRequest` | Lead via MCP tool `team_shutdown_agent` (server.rs:479) | Target agent in `read_unread()` | Lead initiating agent shutdown (scheduler.rs:437-476) |

### Message Lifecycle (mailbox.rs:20-72)

1. **Write Path** (`mailbox.rs:20-54`)
   - `write()` creates `MailboxMessageRow` with timestamp, assigns unique ID
   - Persists to DB via `repo.write_message()`
   - Returns deserialized `MailboxMessage`

2. **Read Path** (`mailbox.rs:56-72`)
   - `read_unread()` fetches unread messages for `(team_id, agent_id)` tuple
   - Calls `repo.read_unread_and_mark()` which **atomically marks all fetched messages as read**
   - Subsequent reads for same agent return empty (no duplicate delivery)
   - Example test (mailbox.rs:113-120): second call to `read_unread()` returns `[]`

3. **History Path** (`mailbox.rs:74-83`)
   - `get_history()` returns **all messages** (read + unread) for agent
   - Used for mailbox UI rendering, not for agent wake dispatch

4. **Cleanup** (`mailbox.rs:85-89`)
   - `delete_by_team()` removes all messages for a team (called when team deleted)

### Who Writes / Who Reads

| Message Type | Writer | Written By | Reader | Read By |
|--------------|--------|-----------|--------|---------|
| `Message` | Any agent or user | MCP `team_send_message` call via scheduler.rs:369-409 | Target agent slot_id | `build_wake_payload()` (prompts.rs:91) includes unread messages in agent's turn context |
| `IdleNotification` | Teammate | scheduler.rs:411-435 writes to lead's mailbox | Lead agent | prompts.rs:91 includes in turn context |
| `ShutdownRequest` | Lead only | scheduler.rs:437-476 (lead-only gate at line 451) | Target teammate | prompts.rs:91 includes in turn context; agent expected to respond "shutdown_approved" or "shutdown_rejected" |

### Atomic Consumption Guarantee

**No message loss between agent turns:**
- `read_unread()` returns all unread messages and marks them read in one DB transaction
- If agent crashes mid-turn, unread messages remain in DB (marked as read but available via `get_history()`)
- Next wake will not re-deliver those messages (they are marked read)
- Design ensures **at-most-once delivery** but not **exactly-once** (agent can see old messages in history)

---

## 3. MCP Server & Protocol

### TCP-Based MCP Server (mcp/server.rs:25-70)

**Startup (session.rs:50-51):**
```rust
let mcp_server = TeamMcpServer::start(auth_token, scheduler.clone()).await?;
// Returns port dynamically allocated on 127.0.0.1:0
```

**Socket Properties:**
- **Listen address:** `127.0.0.1:0` (localhost, dynamic ephemeral port) (server.rs:37)
- **Port returned:** `self.addr.port()` accessible via `mcp_server.port()` (server.rs:58-59)
- **Auth token:** 128-bit random ID (session.rs:50) passed to agents
- **Lifetime:** Starts when `TeamSession::start()` called, stops when `stop()` called (server.rs:66-69)

### Connection Lifecycle (server.rs:117-159)

1. **Accept Loop** (server.rs:82-111)
   - Spawns `handle_connection()` task per TCP client
   - Listens for shutdown signal via `watch::channel`

2. **Per-Connection State Machine** (server.rs:117-159)
   - Initial: `authenticated = false`, `caller_slot_id = None`
   - First request must be `initialize` with `auth_token` parameter (server.rs:138, 170-217)
   - Extract `slot_id` from initialize params (server.rs:195-200)
   - Set `authenticated = true` and proceed to method dispatch (server.rs:139-145)

3. **Method Router** (server.rs:223-238)
   - `notifications/initialized` → acknowledge (server.rs:229)
   - `tools/list` → return all MCP tool descriptors (server.rs:230, 240-242)
   - `tools/call` → dispatch to individual tool handler (server.rs:231, 249-307)
   - Unknown methods → `METHOD_NOT_FOUND` error

### MCP Tools Exposed (mcp/tools.rs:18-114)

**8 tools available via `tools/list` response:**

| Tool | Signature | Caller Gate | Handler |
|------|-----------|-------------|---------|
| `team_send_message` | `{to: string, message: string}` | None | server.rs:339-357 |
| `team_spawn_agent` | `{name, role?, backend}` | Lead only (tools.rs:192-193) | server.rs:359-388 |
| `team_task_create` | `{subject, description?, owner?, blocked_by?}` | None | server.rs:390-406 |
| `team_task_update` | `{task_id, status?, description?, owner?, blocked_by?}` | None | server.rs:408-425 |
| `team_task_list` | `{}` | None | server.rs:427-444 |
| `team_members` | `{}` | None | server.rs:446-462 |
| `team_rename_agent` | `{slot_id, new_name}` | None | server.rs:464-477 |
| `team_shutdown_agent` | `{slot_id, reason?}` | Lead only (server.rs:485) | server.rs:479-504 |

**Backend Whitelist for spawn_agent (tools.rs:167):**
```rust
const SPAWN_BACKEND_WHITELIST: &[&str] = &["claude", "codex"];
```
Attempted spawn of "gemini" or other backends is rejected (tools.rs:370-374).

### Tool Call Execution Path (server.rs:313-332)

1. Agent makes `tools/call` request with `{name, arguments}`
2. `handle_tools_call()` extracts tool name and arguments (server.rs:265-276)
3. Looks up caller's `TeammateRole` via `scheduler.get_agent(caller_slot_id)` (server.rs:278-281)
4. Routes to `dispatch_tool()` (server.rs:283-290)
5. Dispatch matches tool name and executes handler (server.rs:320-332)
6. Handler either:
   - Calls `scheduler.execute_action()` for state-changing tools (tools that create `SchedulerAction`)
   - Returns JSON directly (read-only tools like `team_task_list`, `team_members`)
7. Response wrapped in JSON-RPC: `{content: [{type: "text", text: result}], isError: bool}`

---

## 4. Complete wake→dispatch Flow: From User Message to Agent Response

### Sequence: User sends message to team lead

**Step 1: User HTTP Request → Message to Lead Mailbox**  
User calls REST API → `TeamSession::send_message()` (session.rs:87-106)
```rust
pub async fn send_message(&self, content: &str) -> Result<(), TeamError> {
    let lead_slot_id = self.scheduler.find_lead_slot_id().await?;
    self.mailbox.write(
        &self.team.id,
        &lead_slot_id,      // to: lead agent
        "user",             // from: user
        MailboxMessageType::Message,
        content,
        None,
    ).await?;
    self.wake_and_dispatch(&lead_slot_id).await  // ← trigger wake
}
```

**Step 2a: try_wake() — Status Transition (Idle → Working)**  
`wake_and_dispatch()` calls `scheduler.try_wake()` (session.rs:136)
```rust
pub async fn try_wake(&self, slot_id: &str) -> Result<Option<WakePayload>, TeamError> {
    let current = self.get_status(slot_id).await?;          // Check current status
    if current != TeammateStatus::Idle {
        return Ok(None);  // ← Skip if already Working (duplicate-wake guard)
    }
    self.set_status(slot_id, TeammateStatus::Working).await?;  // Idle → Working
    let payload = self.build_wake_payload(slot_id).await?;     // Assemble context
    Ok(Some(payload))
}
```
Events: `team.agent.status` broadcasted with `status: "working"` (scheduler.rs:129, events.rs:28)

**Step 2b: build_wake_payload() — Assemble Context**  
Construct `WakePayload`:
```rust
pub async fn build_wake_payload(&self, slot_id: &str) -> Result<WakePayload, TeamError> {
    let agent = self.get_agent(slot_id).await?;
    let tasks = self.task_board.list_tasks(&self.team.id).await?;
    let unread = self.mailbox.read_unread(&self.team.id, slot_id).await?;  // ← Atomic mark-read
    Ok(WakePayload { agent, tasks, unread_messages: unread })
}
```
Note: `read_unread()` **marks all fetched messages as read** in the database.

**Step 3: Build Prompt for Agent**  
`wake_and_dispatch()` calls `build_wake_payload()` from prompts.rs (session.rs:145):
```rust
let prompt = build_wake_payload(&payload.agent, &payload.tasks, &payload.unread_messages);
```

Prompt structure (prompts.rs:91-157):
1. `## New Messages` section — lists all unread messages with type labels
2. `## Current Task Board` — markdown table of all tasks (status, owner, blocked_by)
3. Agent identity: `"You are **{name}** (role: {role}). Proceed with your work."`

**Step 4: Spawn Background Task — Send to Conversation Service**  
`wake_and_dispatch()` spawns fire-and-forget task (session.rs:163-180):
```rust
tokio::spawn(async move {
    match conv_service.send_message(&user_id, &conversation_id, req, &task_manager).await {
        Ok(()) => { /* silent success */ }
        Err(e) => {
            warn!("wake_and_dispatch: failed, resetting to idle");
            scheduler.set_status(&slot_id, TeammateStatus::Idle).await;  // ← Rollback on error
        }
    }
});
```
HTTP handler returns immediately; agent conversation runs in background.

**Step 5: Agent Processes & Generates Response (Outside Team Module)**  
Conversation service handles:
- Runs agent through underlying LLM with assembled prompt
- Agent calls MCP tools via TCP connection to `TeamMcpServer`
- Agent's response is converted to `SchedulerAction` set

**Step 6: finalize_turn() — Execute Actions & Mark Idle**  
After agent response received, `finalize_turn()` called (scheduler.rs:281-302):
```rust
pub async fn finalize_turn(
    &self,
    slot_id: &str,
    actions: &[SchedulerAction],
) -> Result<Option<String>, TeamError> {
    let mut wake_signal = None;
    for action in actions {
        if let Some(leader_id) = self.execute_action(slot_id, action).await? {
            wake_signal = Some(leader_id);  // ← Capture if action returns wake signal
        }
    }

    let has_idle_notification = actions
        .iter()
        .any(|a| matches!(a, SchedulerAction::IdleNotification { .. }));

    if !has_idle_notification {
        self.mark_idle(slot_id).await  // ← Mark agent Idle
    } else {
        Ok(wake_signal)  // ← IdleNotification already called mark_idle internally
    }
}
```

**Step 7a: Execute Individual Actions** (scheduler.rs:200-277)

For each action:

- **SendMessage** → Write to target mailbox (scheduler.rs:369-409)
  - If `to == "*"`: broadcast to all agents except self
  - Else: write to specific `to` agent
  
- **TaskCreate** → Create task on board (scheduler.rs:211-227)
  - Validates `blocked_by` dependencies exist
  - Stores task in DB
  
- **TaskUpdate** → Update task status (scheduler.rs:228-249)
  - If status becomes `Completed`, trigger dependency unblocking (task_board.rs:105-108)
  
- **IdleNotification** → Write to lead's mailbox (scheduler.rs:411-435)
  - Only teammate agents send to lead (not lead to self)
  - Returns optional wake signal if all teammates now idle
  
- **ShutdownAgent** → Write shutdown request to target (scheduler.rs:437-476)
  - Lead-only gate (line 451)
  - Target receives `ShutdownRequest` message in next wake
  
- **SpawnAgent** → Currently no-op (scheduler.rs:254-265)
  - Logs intent; actual spawn handled elsewhere (session-level)
  
- **RenameAgent** → Update agent name in-memory (scheduler.rs:272-275)

**Step 7b: mark_idle() — Status Transition (Working → Idle)**  
After actions executed, `mark_idle()` called (scheduler.rs:182-198):
```rust
pub async fn mark_idle(&self, slot_id: &str) -> Result<Option<String>, TeamError> {
    self.set_status(slot_id, TeammateStatus::Idle).await?;
    
    // If lead, don't wake self
    if is_lead {
        return Ok(None);
    }
    
    // If teammate, check if all teammates idle now
    self.maybe_wake_leader_when_all_idle().await
}
```
Events: `team.agent.status` broadcasted with `status: "idle"`

**Step 7c: maybe_wake_leader_when_all_idle() — Conditional Lead Wake**  
Called by `mark_idle()` for non-lead agents (scheduler.rs:482-531):
```rust
if all_teammates_idle && lead_is_idle {
    return Ok(Some(lead_id));  // ← Return lead's slot_id as wake signal
}
```

Returns lead's `slot_id` if:
1. Lead agent exists
2. Lead currently `Idle`
3. All teammates currently `Idle`
4. At least one teammate exists (not solo team)

**Step 8: Next Iteration (If Wake Signal)**  
Caller of `finalize_turn()` receives lead's `slot_id` and should call `wake_and_dispatch(lead_id)` to restart cycle.

---

## 5. finalize_turn / mark_idle Life Cycle

### When finalize_turn is Called

**From conversation service after agent turn completes:**
- Conversation module receives agent response
- Parses actions from LLM structured output or tool calls
- Calls back into team module to finalize

**Responsibility of caller:**
- Agent is in `Working` state
- Actions have been parsed and validated
- Agent is ready to be marked `Idle`

### State After finalize_turn Completes

```
Agent Status:      Idle (or Working if error rolled back)
Messages read:     All fetched in build_wake_payload() are marked read
Tasks updated:     Per actions
Lead wake signal:  Maybe returned if all teammates now idle
Broadcast events:  team.agent.status (idle) + per-action events
```

### Error Handling

**In wake_and_dispatch()** (session.rs:163-180):
- If `conv_service.send_message()` fails, agent is rolled back to `Idle` via `set_status()`
- No actions executed
- No wake signal generated

**In finalize_turn()** — Currently doesn't validate actions before execution:
- Actions execute sequentially; later actions don't see errors from earlier ones
- If `execute_action()` fails, finalize_turn() propagates error
- Agent status remains `Idle` if all actions fail early, or partially executed if some succeeded

### Anti-Deadlock Guarantees

1. **Lead never wakes self:**
   - `mark_idle()` for lead returns `Ok(None)` immediately (scheduler.rs:193-194)
   - Prevents infinite loop if lead messages self

2. **All teammates idle → wake lead (once):**
   - `maybe_wake_leader_when_all_idle()` only signals **if all are idle**
   - Lead status also checked: must be `Idle` (scheduler.rs:513-520)
   - Only called after non-lead `mark_idle()` (scheduler.rs:197)
   - Subsequent lead wakes only happen if lead receives new message

3. **Solo team (lead only):**
   - `maybe_wake_leader_when_all_idle()` returns `None` if no teammates (scheduler.rs:505-506)
   - Lead never auto-wakes after its own turn

---

## 6. Known Issues & Bug Points

### Issue 1: Message Loss If Agent Crashes Before turn_finalize

**Scenario:**
1. Agent woken with N unread messages → all marked read in DB (mailbox.rs:61)
2. Agent processes messages and starts generating response
3. **Agent crashes or timeout occurs mid-response**
4. finalize_turn() never called
5. Messages are stuck marked-as-read but agent didn't process them

**Symptom:** User bubble sent, but agent didn't respond or response got lost.

**Root Cause:** Read marking is done in `read_unread()` before agent response is ready. No transactional rollback if turn fails.

**Current code evidence:**
- mailbox.rs:56-72: `read_unread_and_mark()` marks all fetched messages as read
- session.rs:145: `build_wake_payload()` calls `read_unread()` which marks messages read
- session.rs:163-180: tokio::spawn sends async task; if it panics, mark_idle never called

**Fix direction (not implemented):** 
- Delay mark-as-read until finalize_turn() succeeds
- Or: Track "in-flight" messages separately from "read"

---

### Issue 2: Duplicate Message if WAKE_TIMEOUT_MS Fires (Currently Unused)

**Scenario:**
1. Agent woken, status set to `Working`
2. WAKE_TIMEOUT_MS elapses (60 seconds defined but not used)
3. User sends new message → writes to mailbox
4. Next `wake_and_dispatch()` called
5. `try_wake()` sees agent already `Working` → returns `None`
6. New message is queued but doesn't trigger wake (guard at scheduler.rs:166)

**Symptom:** Subsequent user messages after agent stalls are silently enqueued but never delivered.

**Current code evidence:**
- scheduler.rs:16: `WAKE_TIMEOUT_MS = 60_000` defined but not used in code
- scheduler.rs:164-174: `try_wake()` skips if not `Idle`, silent no-op
- session.rs:135-142: If `try_wake()` returns `None`, handler just returns `Ok(())` without warning

**Fix direction (not implemented):**
- Implement timeout timer: if agent stays `Working` > 60s, auto-rollback to `Idle`
- Or: Log warning when user message arrives for `Working` agent
- Or: Force-reset `Working` agent to `Idle` if new message arrives and mailbox has unread items

---

### Issue 3: IdleNotification Action Skips Double-Idle Broadcast

**Scenario:**
1. Teammate finishes work, includes `IdleNotification` action
2. `finalize_turn()` receives actions
3. Detects `IdleNotification` present (scheduler.rs:293-295)
4. **Skips calling `mark_idle()`** — already handled inside `handle_idle_notification()` (scheduler.rs:434)
5. But `broadcast_agent_status("idle")` called inside `mark_idle()`, so broadcast happens only once ✓

**Non-Issue:** This is actually correct behavior (double-idle is prevented per test at scheduler.rs:1192).

---

### Issue 4: SpawnAgent Action is No-Op in Scheduler

**Scenario:**
1. Lead calls MCP tool `team_spawn_agent(name, backend)`
2. Server dispatches to `exec_spawn_agent()` (server.rs:359-388)
3. Executes `SchedulerAction::SpawnAgent` via scheduler (server.rs:381-386)
4. Scheduler's `execute_action()` just logs and returns `Ok(None)` (scheduler.rs:254-265)
5. **Agent is not actually added to scheduler slots**

**Symptom:** After agent spawn command, new agent doesn't appear in `team_members()` list or get tasks assigned.

**Root Cause:** Spawn is intended to be handled at session level, not scheduler level (scheduler.rs:263).

**Current code evidence:**
- scheduler.rs:254-265: SpawnAgent action is no-op
- Comment says "requires TeamSession to complete"
- session.rs has no corresponding spawn_agent implementation

**Fix direction (not implemented):**
- Either implement in TeamSession or clarify this is deferred work

---

### Issue 5: No Validation of circular dependencies in blocked_by

**Scenario:**
1. Create task A with blocked_by: [B]
2. Create task B with blocked_by: [A]
3. Neither task can ever be completed

**Root Cause:** task_board.rs:39-43 only validates that each dependency **exists**, not that it forms a DAG.

**Current code evidence:**
- task_board.rs:31-73: `create_task()` checks `if dep.is_none()` but doesn't detect cycles
- task_board.rs:39-43: No cycle detection

**Fix direction (not implemented):**
- Perform DFS or topological sort before accepting blocked_by dependencies

---

## 7. Architecture Summary

### Three-Layer Model

1. **Dispatch Layer** (session.rs)
   - HTTP endpoint receives user message
   - Writes to mailbox, calls wake_and_dispatch()
   - Spawns background task with conv_service.send_message()

2. **Scheduler & Mailbox Layer** (scheduler.rs, mailbox.rs)
   - TeammateManager tracks agent state machine (Idle ↔ Working)
   - Mailbox atomically delivers and marks messages read
   - finalize_turn() executes batched actions and manages transitions
   - Prevents deadlock via lead self-wake guard and all-teammates-idle wake

3. **MCP Server Layer** (mcp/server.rs, mcp/tools.rs)
   - TCP server on localhost ephemeral port
   - Agents connect with auth token + slot_id
   - Implements 8 tools: messaging, tasks, agent lifecycle
   - Tool calls converted to SchedulerAction for execution

### Data Flow Diagram

```
User Message (HTTP)
    ↓
TeamSession.send_message()
    ↓
Mailbox.write() → DB
    ↓
wake_and_dispatch()
    ├─→ try_wake() → Idle→Working (synchronous)
    └─→ tokio::spawn(conv_service.send_message() → MCP calls → finalize_turn())
    
Agent Turn (async background):
    ↓
Agent connects to MCP Server (127.0.0.1:port)
    ├─ Initialize with auth_token + slot_id
    └─ tools/call → dispatch_tool() → execute_action()
    
After agent response:
    ↓
finalize_turn(actions)
    ├─→ execute_action() per action (SendMessage, TaskCreate, TaskUpdate, etc.)
    ├─→ mark_idle() → Working→Idle
    └─→ maybe_wake_leader_when_all_idle() → return lead slot_id if ready
    
Lead Woken (if signal returned):
    ├─→ wake_and_dispatch(lead_slot_id)
    └─→ Cycle repeats
```

### Invariants Maintained

- **At-most-once message delivery per turn:** `read_unread_and_mark()` marks atomically
- **No self-wake for lead:** `mark_idle()` guards at line 193
- **No duplicate wake of agent:** `try_wake()` guards at line 166
- **No message loss during handoff:** All actions from one turn execute before next wake
- **Task dependency validation:** `blocked_by` tasks checked to exist (but not cycle-free)
- **Role-based authorization:** `team_spawn_agent` and `team_shutdown_agent` lead-only

---

## 8. File Cross-References

| File | Lines | Purpose |
|------|-------|---------|
| scheduler.rs | 84-532 | TeammateManager state machine, action execution, finalize_turn |
| mailbox.rs | 11-72 | Message write/read with atomic mark-as-read |
| session.rs | 19-183 | TeamSession wraps scheduler, wake_and_dispatch spawns task |
| mcp/server.rs | 25-505 | TCP MCP server, initialize handshake, method dispatch |
| mcp/tools.rs | 11-172 | Tool descriptors (8 tools), input types, backend whitelist |
| prompts.rs | 91-157 | build_wake_payload() constructs agent context |
| events.rs | 11-77 | TeamEventEmitter broadcasts status/spawn/remove/rename events |
| task_board.rs | 12-147 | TaskBoard creates/updates tasks, manages blocked_by |

---

## Conclusion

The aionui-team module implements a **state-machine-based coordinator** for multi-agent workflows:

- **Lead agent** receives user requests and delegates tasks
- **Teammate agents** execute tasks and report progress via mailbox
- **Scheduler** orchestrates wake/idle transitions and prevents deadlocks
- **MCP server** provides isolated, authenticated tool access for each agent
- **Task board** tracks dependencies and unblocks downstream tasks

**Key Design Principles:**
1. Explicit state transitions (Idle ↔ Working) prevent race conditions
2. Atomic mailbox reads ensure no message loss within turns
3. Role-based gates (Lead-only spawn/shutdown) enforce policy
4. Fire-and-forget background dispatch keeps HTTP paths responsive

**Known Gaps:**
1. No recovery if agent crashes mid-turn (messages marked read but unprocessed)
2. WAKE_TIMEOUT_MS defined but unused — stalled agents stay Working forever
3. SpawnAgent action is no-op at scheduler level (deferred to session?)
4. No circular dependency detection for task blocked_by edges
