# 开发流程:AionUi + aionui-backend 联调

本文档描述本地同时开发前端 `AionUi`(Electron) 和后端 `aionui-backend`(Rust) 的推荐流程。

## 仓库关系

```
AionUi (Electron 桌面应用)
  └── 启动时 spawn 子进程: aionui-backend 二进制
      └── 监听随机端口
          └── preload 把端口注入 window.__backendPort
              └── renderer 通过 http://127.0.0.1:{port} 访问 REST + WS
```

- 前端仓库:`~/Documents/github/AionUi`(bun 管理依赖)
- 后端仓库:`~/Documents/github/aionui-backend`(cargo 管理)
- 前端通过 `src/process/backend/binaryResolver.ts` 查找后端二进制,查找顺序:
  1. `resources/bundled-aionui-backend/{platform}-{arch}/aionui-backend`(打包产物)
  2. 系统 `PATH`(**开发模式依赖这一项**)

## 一次性环境准备

### 1. 安装 cargo-watch(可选,但强烈推荐)

```bash
cargo install cargo-watch
```

作用:后端源码改动后自动重新编译,无需手动 `cargo build`。

### 2. 把后端 debug 产物 symlink 到 PATH

让 `which aionui-backend` 能解析到本地 cargo 编译出的二进制:

```bash
# 先确保至少编译过一次,产物路径存在
cd ~/Documents/github/aionui-backend
cargo build

# 建立 symlink(~/.cargo/bin 在 PATH 首位,确保被 Electron 找到)
ln -sf ~/Documents/github/aionui-backend/target/debug/aionui-backend \
       ~/.cargo/bin/aionui-backend

# 验证
which aionui-backend
# 应输出: /Users/zhoukai/.cargo/bin/aionui-backend
aionui-backend --help
```

> **注意**:symlink 指向的是 `target/debug/aionui-backend`,每次 `cargo build` 会覆盖该文件,symlink 自动跟随,无需重建。
>
> **避坑**:如果之前用过 `cargo install --path .` 装过这个 crate,`~/.cargo/bin/aionui-backend` 会是一个**真实二进制**(不是 symlink),且优先级高于 `~/.local/bin`,会导致你的代码改动看似不生效。确认方法:`ls -la ~/.cargo/bin/aionui-backend` 看输出开头是不是 `l`(l 表示 symlink)。不是就删掉重建。

### 3. 安装前端依赖(前端仓库用 bun)

```bash
cd ~/Documents/github/AionUi
bun install
```

## 日常开发循环

### 终端 1:后端

```bash
cd ~/Documents/github/aionui-backend
cargo watch -x build
```

- 改 Rust 源码 → `cargo watch` 自动触发增量编译
- 编译成功 → `target/debug/aionui-backend` 更新 → symlink 自动跟随

如果没装 `cargo-watch`,手动跑:

```bash
cargo check                          # 只类型检查,最快
cargo build                          # 改动完成后手动编译
```

### 终端 2:前端

```bash
cd ~/Documents/github/AionUi
bun start                            # 等价于 electron-vite dev
```

前端启动时:
- Electron 主进程调用 `resolveBinaryPath()` 找到 `~/.local/bin/aionui-backend`(你的 symlink)
- `BackendLifecycleManager` spawn 子进程,等 `/health` 就绪
- preload 把端口写入 `window.__backendPort`,renderer 通过 `httpBridge` 发请求

**前端热更新**:renderer 代码改动走 Vite HMR,浏览器自动刷新,无需重启。

**后端改动生效方式**:
1. 后端重新编译完成
2. **关闭并重新启动 Electron**(`bun start` 重跑)
3. Electron 会 spawn 新的后端进程

> 后端是启动时 spawn 一次的子进程,**不会热重载**。改后端 = 重启 Electron。

## 常用前端命令(bun)

```bash
bun start                            # 普通开发模式
bun run webui                        # WebUI 模式(浏览器访问)
bun run lint                         # oxlint
bun run format                       # oxfmt
bun test                             # vitest
```

## 常用后端命令(cargo)

```bash
cargo check --workspace              # 类型检查(最快)
cargo build                          # 编译 debug(symlink 会更新)
cargo test --workspace               # 跑测试
cargo clippy --workspace -- -D warnings  # lint
cargo fmt --all                      # 格式化
```

## 同时改前后端的工作流

### 分支命名(建议,非强制)

两个仓库开关联的分支,方便跨仓检索。**不强制完全同名**,选用下列任一方式:

**方式 A:完全同名(最简单)**

```bash
# 后端
cd ~/Documents/github/aionui-backend
git checkout -b feat/assistant-user-data

# 前端
cd ~/Documents/github/AionUi
git checkout -b feat/assistant-user-data
```

**方式 B:前缀约定(适合前端仓已有 pilot 前缀惯例)**

前端仓历史上用 `feat/backend-migration-*` 前缀(例如
`feat/backend-migration-coordinator`、`feat/backend-migration-fe-skill-library`),
新工作流延续前缀即可:

```bash
# 后端
git checkout -b feat/assistant-user-data

# 前端(保留前缀以对齐仓内历史)
git checkout -b feat/backend-migration-assistant-user-data
```

关键要求是**去掉仓名前缀后尾段相同**(`assistant-user-data`),这样任何
跨仓搜索都能命中。

### PR 互引(硬约定)

不论采用哪种命名方式,两仓 PR 描述中必须互相粘贴对方仓库的 PR 链接,
这是避免分支名不对齐导致的信息丢失的硬约束。

### API 契约先行

1. **后端**:先在 `crates/aionui-api-types` 里定义请求/响应类型
2. **后端**:在对应 domain crate 实现 routes + service + 测试
3. **前端**:在 `src/renderer/api/` 或 `src/common/adapter/httpBridge.ts` 里手写对应 TS 类型和调用
4. **联调**:Electron 起来,浏览器 devtools 看网络请求,两边都能改

### 提交与合并顺序

- 通常**后端先合并**(前端依赖后端接口存在)
- 或者后端先上线 feature flag / 兼容层,前后端任意顺序合并
- PR 描述里互相引用对方仓库的 PR 链接

### 类型同步

目前前端手写 TS 类型,容易和后端漂移。如果后续痛点明显,可考虑:

| 方案 | 成本 | 同步强度 |
|------|------|----------|
| 手写(现状) | 低 | 弱 |
| OpenAPI 生成 TS | 中 | 中 |
| `ts-rs` / `specta` 从 Rust 导出 TS | 中高 | 强 |

推荐先用现有方式,等痛点积累到一定程度再引入 `ts-rs`(`aionui-api-types` 已是独立 crate,接入成本最低)。

## 故障排查

### `Cannot find "aionui-backend" binary`

Electron 报这个错,说明 `binaryResolver.ts` 没找到二进制。检查:

```bash
which aionui-backend                 # 应指向 ~/.cargo/bin/aionui-backend
ls -la ~/.cargo/bin/aionui-backend   # 应是一个 symlink(开头是 l)
ls -la ~/Documents/github/aionui-backend/target/debug/aionui-backend
                                     # 实际产物应存在
```

缺任何一项就重新执行"一次性环境准备"对应步骤。

### 改了后端代码,前端没效果

- 确认后端是否**重新编译完成**(看 `cargo watch` 输出)
- 确认 Electron 是否**重启**(子进程不会热重载)
- 确认 Electron 启动日志是否有 `[aionui-backend] listening on port XXXX`

### `aionui-backend failed to start within timeout`

后端启动超过 10 秒未就绪(`lifecycleManager.ts:134`)。可能原因:
- 编译出的 debug 二进制启动本身就慢 → 首次启动正常
- 后端启动时 panic → 看终端日志(Electron 会把 stderr 转发到 console)
- `/health` 端点没实现或 500 → 直接手动跑 `aionui-backend --port 13400 --data-dir /tmp/aionui-test`,`curl http://127.0.0.1:13400/health` 验证

### 端口冲突

`lifecycleManager.ts:27` 用 `findAvailablePort` 随机取可用端口,通常不冲突。如果要固定端口调试,手动跑后端:

```bash
cd ~/Documents/github/aionui-backend
cargo run -- --port 13400 --data-dir ./data
```

然后临时改 `httpBridge.ts` 的兜底端口,或者后续引入 external backend 开关(见下文)。

## 后续可选改进

### external backend 开关(按需)

如果前端改得多、后端改得少,每次重启 Electron 成本高。可以给 `BackendLifecycleManager` 加环境变量开关:

- `AIONUI_BACKEND_EXTERNAL=1` → 跳过 spawn,直接使用固定端口
- 开发者手动跑 `cargo run -- --port 13400`,后端独立运行
- 前端只重启 Electron UI,不影响后端状态

改动点:`AionUi/src/process/backend/lifecycleManager.ts` 的 `start()` 方法。约 30 行。

### 自动化启动脚本

可在后端仓库加 `justfile` 或 shell 脚本,一键启动 `cargo watch + symlink 检查`,进一步减少心智负担。
