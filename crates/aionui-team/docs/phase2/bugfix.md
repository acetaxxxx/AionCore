# Phase 2 Bugfix Status — 2026-05-02

## Branch: `fix/team-communication-bugs`

---

## Problem 1: Leader 不 spawn 成员 (卡在 working 状态)

### 状态: 已定位根因，需要修 prompt

### 根因

Leader 收到用户消息后正确调用了 `team_list_models` 和 `team_members`，但随后 **主动结束了 turn（idle）**，没有调用 `team_spawn_agent`。

原因在 leader prompt (`crates/aionui-team/src/prompts/lead.rs`) 里的工作流规则：

```
9. End your turn after the proposal. Do NOT call team_spawn_agent in that same turn
10. Wait for explicit confirmation before using team_spawn_agent, unless the user explicitly told you to create specific teammates immediately
```

在单聊转群聊场景下，用户的确认发生在 guide agent 那边（单聊里说 "ok"），guide 把确认后的 summary 传给了 `aion_create_team`。Leader 的第一条消息（来自 `send_message` fire-and-forget）是 summary 内容，不是"确认 spawn"的指令。

Leader 按规则理解为"这是一个新任务描述，我需要先提出阵容方案让用户确认"，但用户已经不在这个对话里了（用户在 team UI 看到的是 leader 在 "处理中" 但没产出）。

### 修复方案

在 `guide/server.rs` 的 `handle_aion_create_team` 发给 leader 的 summary 消息里，**显式指示 leader 直接 spawn 而不再确认**。修改 `send_message` 的内容格式：

```rust
// 当前（错误）:
let summary = params.summary.clone();  // 纯 summary

// 修改为：
let summary = format!(
    "{}\n\n[SYSTEM NOTE: The user has already confirmed this team lineup. \
    Proceed immediately with team_spawn_agent for each teammate listed above. \
    Do NOT ask for confirmation again.]",
    params.summary
);
```

或者更干净的做法：在 leader prompt 的 "Important Rules" 里加一条规则：

```
- If the first message you receive already contains a complete team configuration
  (roles, types, models), the user has already approved it during team creation.
  Skip the proposal step and proceed directly to spawning teammates.
```

### 需要修改的文件
- `crates/aionui-team/src/guide/server.rs` — send_message 内容加 system note
- 或 `crates/aionui-team/src/prompts/lead.rs` — 加规则让 leader 识别"预确认"场景

---

## Problem 2: 前端不自动跳转 (单聊 → team)

### 状态: 需要前端 + 服务端联合修复

### 分析

后端在 `create_team` 时通过 WebSocket broadcast 了 `team.listChanged` 事件。但前端没有做两件事：
1. 刷新 conversation 列表（让已标记 teamId 的原会话消失）
2. 自动跳转到 `/team/{teamId}`

### 前端分支

已在 AionUi 项目创建了 `fix/team-auto-redirect` 分支（从 `feat/backend-migration` 切出），包含：
- `src/renderer/pages/team/hooks/useTeamCreatedRedirect.ts` — 新 hook
- `src/renderer/components/layout/Sider/index.tsx` — 挂载 hook

但这个修复可能**不够**，因为：
- `team.listChanged` 事件可能在前端还没渲染完 team 列表时就到了
- 或者事件的 payload 里没带 `teamId`，导致 hook 无法跳转

### 需要验证
1. 后端 `team.listChanged` 事件的 payload 格式是否包含 `teamId` 和 `action: "created"`
2. 前端 `useTeamList` hook 是否正确刷新
3. 验证 WebSocket 连接是否在此时间点活跃

### 相关代码
- 后端: `crates/aionui-team/src/events.rs` — broadcast team events
- 前端: `src/renderer/pages/team/hooks/useTeamList.ts` — team list 刷新
- 前端: `src/common/adapter/ipcBridge.ts` — WebSocket 事件绑定

---

## 已修复的问题（本次 session）

| # | Issue | Commit | Status |
|---|-------|--------|--------|
| 1 | spawn_agent 缺 finish_subscriber | PR #140 | ✅ |
| 2 | finalize_turn 去重窗口丢事件 | PR #140 | ✅ |
| 3 | wake/finish 竞态 | PR #140 | ✅ |
| 4 | 单聊转群聊会话复用 | bcc89b2 | ✅ |
| 5 | guide server user_id | bcc89b2 | ✅ |
| 6 | MCP 工具权限白名单 | bcc89b2 | ✅ |
| 7 | MCP bridge 缺 JSON-RPC id → 死锁 | bcc89b2 | ✅ |
| 8 | Guide HTTP 大 body 读取不完整 | a3b6cb9 | ✅ |
| 9 | spawn_agent warmup 阻塞 MCP 响应 | 9f31504 | ✅ |

---

## 复现步骤

1. 新建单聊（claude backend）
2. 发送"创建团队进来随机主题辩论"
3. Agent 提出阵容表，用户回复 "ok"
4. Agent 调用 `aion_create_team` → 成功
5. **期望**：前端自动跳转 team 页面，leader 立即 spawn 成员
6. **实际**：前端不跳转；leader idle 不 spawn

---

## 日志确认（08:29-08:30 时间段）

```
08:29:28 — aion_create_team succeed, team_id=019de7ce-...
08:29:28 — Team created
08:29:28 — TeamSession started (port 62979)
08:29:30 — leader wake, session/new sent (team MCP injected)
08:29:36 — leader prompt sent (full role prompt + user summary)
08:29:54 — leader calls team_list_models → success
08:29:54 — leader calls team_members → success
08:30:06 — leader status → IDLE (turn ended without spawning!)
```

Leader 结束 turn 是因为 prompt 规则要求"先提方案等确认"。但用户已经确认过了。

---

## 环境信息

- Backend binary: `/Users/zhuqingyu/project/aionui-backend/target/release/aionui-backend`
- Symlink: `~/.local/bin/aionui-backend` → 上述 binary
- Frontend: `/Users/zhuqingyu/project/AionUi` branch `feat/backend-migration`
- Frontend fix branch: `fix/team-auto-redirect`
- Backend fix branch: `fix/team-communication-bugs`
- Log file: `/Users/zhuqingyu/Library/Logs/AionUi-Dev/2026-05-02.backend.log`
