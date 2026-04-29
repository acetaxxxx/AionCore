# Team 内部调度

## Agent 状态机

```
           ┌─────────────────────────────────────┐
           │                                     │
           │   try_wake()  (收到新消息)           │
           ▼                                     │
        ┌──────┐                             ┌───┴────┐
        │ Idle │ ──── Idle → Working ──────▶ │Working │
        └──────┘                             └───┬────┘
           ▲                                     │
           │                                     │
           │    finalize_turn() / mark_idle()    │
           └─────────────────────────────────────┘

非 Lead 变 Idle 时：若所有 teammate + lead 均为 Idle → 返回 lead slot_id（调用方负责再 wake 一次 lead）
Lead 变 Idle 时：立刻返回 None（防止自唤醒死循环）
try_wake 发现非 Idle：直接返回 None（防止重复唤醒）
```

状态枚举：`Idle / Working / Thinking / ToolUse / Completed / Error`（见 `types.rs`）。

## wake → dispatch 时序

```
User                HTTP            TeamSession       Scheduler        Mailbox      ConvService       MCP Server       Agent
 │                   │                  │                 │                │              │                │             │
 │ POST /messages ─▶ │                  │                 │                │              │                │             │
 │                   │─ send_message ─▶ │                 │                │              │                │             │
 │                   │                  │─ mailbox.write ─┼──────────────▶ │              │                │             │
 │                   │                  │─ wake_and_dispatch ─▶            │              │                │             │
 │                   │                  │                 │─ try_wake ────▶│              │                │             │
 │                   │                  │                 │◀── Idle→Working│              │                │             │
 │                   │                  │                 │─ read_unread ─▶│ (原子标记已读)│                │             │
 │                   │                  │                 │                │              │                │             │
 │                   │                  │─ tokio::spawn(conv_service.send_message) ─────▶ │                │             │
 │                   │◀── 200 OK ───────│                 │                │              │                │             │
 │                   │                  │                 │                │              │─ 启动 agent ───┼──────────▶  │
 │                   │                  │                 │                │              │                │◀── connect ─│
 │                   │                  │                 │                │              │                │── tools/call ◀─│
 │                   │                  │                 │◀─ execute_action ─────────────┼────────────────│             │
 │                   │                  │                 │                │              │                │── 结果 ────▶│
 │                   │                  │                 │◀─ finalize_turn(actions) ─────│                │             │
 │                   │                  │                 │─ mark_idle ────│              │                │             │
 │                   │                  │                 │ broadcast team.agent.status   │                │             │
 │                   │                  │                 │ maybe_wake_lead (若都 idle)   │                │             │
```

> 注：`POST /messages` 在本图中泛指触发路径（来自单聊 API 或 agent-to-agent MCP 调用），team 模块本身不再暴露消息端点。

关键点：
- HTTP 立刻 200 返回，agent 回合在 `tokio::spawn` 里跑，失败会把 agent rollback 到 Idle。
- `read_unread_and_mark` 是**原子**的：一次 SQL 拿走所有未读并标记已读（见 bug #1）。
- 所有 WS 事件由 `TeamEventEmitter` 发：`team.agent.status / spawned / removed / renamed`。

## Mailbox

三种消息（`mailbox` 表 `type` 列）：
- `message` — agent→agent（lead 派单、teammate 汇报）
- `idle_notification` — teammate 完成后写给 lead（带 `summary`）
- `shutdown_request` — lead 要求某个 teammate 下线

所有消息读路径都只走 `read_unread`（原子标记已读），历史查询走 `get_history`。邮箱**不对外暴露 HTTP**。

用户消息不再经过 mailbox：用户→agent 直接走单聊 API，写入 `messages` 表，走 conversation 的常规 send/stream 路径。mailbox 只承载 agent 内部消息。

## MCP Server（Agent ↔ Scheduler 的桥）

完整协议、工具清单、后端 GAP 请看 [mcp.md](./mcp.md)。这里只记跟调度强相关的要点。

架构里的位置：

```
┌──────────────────────────────────────────────────────────────┐
│  TeamSession (in-memory, per team)                           │
│                                                              │
│   Scheduler ──── 写/读 ───▶ Mailbox (SQLite)                 │
│       ▲                    TaskBoard (SQLite)                │
│       │ execute_action                                       │
│       │                                                      │
│   TeamMcpServer  ◀──── HTTP + JSON-RPC ────  Agent Process   │
│   127.0.0.1:port         (agent 主动连接)                     │
└──────────────────────────────────────────────────────────────┘
```

- 每个 team session 启动时在 `127.0.0.1:<随机端口>` 起 HTTP MCP server
- 传输方式：HTTP transport（commit 6c334a9 从 stdio bridge 切换）— agent CLI 主动连接，无需 stdio bridge 子进程
- Agent 通过 JSON-RPC `initialize(auth_token, slot_id)` 鉴权后才能调工具
- 暴露 8 个工具：`team_send_message / team_spawn_agent / team_task_create / team_task_update / team_task_list / team_members / team_rename_agent / team_shutdown_agent`（AionUi 参考实现有 10 个，差 `team_describe_assistant` 和 `team_list_models`）
- `team_spawn_agent` 和 `team_shutdown_agent` 仅 Lead 可调用
- `team_spawn_agent` 的 backend 白名单：`["claude", "codex"]`
- `team_spawn_agent` 当前为 no-op：工具可调用，返回 success，但不实际创建 agent（Wave 5 待落地）

### MCP 与 Mailbox / TaskBoard 的交互

| MCP 工具 | 落到哪 | 是否触发 wake |
|----------|--------|:---:|
| `team_send_message` | Mailbox.write() (`message` 类型) | ⚠️ 否（bug） |
| `team_shutdown_agent` | Mailbox.write() (`shutdown_request`) | ⚠️ 否 |
| `IdleNotification` action（非 MCP 工具，是 agent 回合结束时 scheduler 自动触发） | Mailbox.write() (`idle_notification`) → `mark_idle` → 可能 wake lead | ✅ 是 |
| `team_task_create / update / list` | TaskBoard（SQLite `team_tasks`） | 不涉及 wake |
| `team_members / team_rename_agent` | 内存 slots（+ WS 广播） | 不涉及 wake |
| `team_spawn_agent` | ⚠️ 空壳：只打 log | — |

**问题**：MCP 写完 mailbox 后没有调 `wake_and_dispatch`，与单聊 API 路径（`POST /api/conversations/{id}/messages` 会走 `TeamSession.wake_and_dispatch`）不一致。agent-to-agent 消息当前靠"下一次外部触发 wake"才被看到。见 bug #2。

## 已知 Bug

| # | 问题 | 现象 | 位置 |
|---|------|------|------|
| 1 | Agent 中途崩溃导致消息丢失 | `read_unread` 已标已读但 agent 没处理完就挂了，用户看到无响应 | `mailbox.rs`, `session.rs` |
| 2 | `WAKE_TIMEOUT_MS` 定义了但没用 | agent 卡在 Working 永不恢复，新消息静默入队不触发 | `scheduler.rs` |
| 3 | `SpawnAgent` action 是 no-op | Lead 调 `team_spawn_agent` 后，新 agent 不会真的加进 scheduler | `scheduler.rs` |
| 4 | 任务依赖无环检测 | A blocked_by B、B blocked_by A 可成功创建，互相死锁 | `task_board.rs` |
| 5 | `list_teams` 不按 user_id 过滤 | 任何登录用户能列出所有人的 team | `routes.rs` |
| ~~6~~ | ~~用户消息气泡不显示~~ | **已解决**：用户→agent 改走单聊 API，直接写入 `messages` 表，自然产生 visible user row | — |
| ~~7~~ | ~~Agent Working 时用户后续消息被吞~~ | **已解决**：用户消息不再经 mailbox，走单聊的常规队列，不受 team scheduler 状态影响 | — |

> bug #6 / #7 通过删除 team 专属消息路由、改走单聊 API 消除。**bug #2 仍存在于 agent-to-agent 路径上**（teammate Working 时收到 lead 派单，若卡死则新消息不会触发 re-wake），尚未修复。

## 不变式

- 单回合消息**至多一次投递**（`read_unread_and_mark` 原子）
- Lead 绝不自唤醒（`mark_idle` 针对 lead 立刻返回 None）
- 同一 agent 不会被重复唤醒（`try_wake` 非 Idle 直接 None）
- 一回合的所有 action 在下一次 wake 前执行完毕
