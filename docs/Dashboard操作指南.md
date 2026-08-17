# Configraft Dashboard 操作指南（前端逐功能测试）

> 配套：`docs/演示脚本.md` 第 9 节测试清单（表格速查版）。本文是**教学式**详解：每一步"怎么操作 → 应该出现什么现象 → 这个功能解决什么"，
> 用于面试讲解 / 自测 / 录屏演示。所有现象描述基于 `web/` 前端（2026-08-17 实现）实际渲染逻辑。

## 0. 环境准备与界面导览

```bash
cd configraft
bash scripts/run_cluster.sh        # 启动 3 节点集群（8001-8003）
```

> **演示完毕停止集群**（一条命令，自动清理进程与端口）：
> ```bash
> bash scripts/run_cluster.sh stop
> ```

浏览器打开（任意节点均可，建议先开 leader）：

```
http://127.0.0.1:8001/dashboard
```

> 若页面只有文字、没有排版颜色：是浏览器缓存了旧版页面，**Ctrl+Shift+R 硬刷新**。

页面四个区域：

| 区域 | 内容 | 自动/手动 |
|---|---|---|
| 顶栏右侧 | 集群在线状态 `● 集群在线 3/3` | 自动（2s 轮询） |
| 左侧「集群节点」 | 3 个节点卡片：端口、角色、term/commit/applied、peers | 自动 |
| 中间「配置 / KV 操作」 | 4 个输入框（Key / Value / CAS expect / 回滚目标版本）+ 7 个按钮 | 手动 |
| 中间下方「结果区 + 版本链」 | 操作响应、配置历史版本链 | 手动触发 |
| 右侧「实时变更流 Watch」 | 所有写入的实时事件（`#revision` + PUT/DELETE + key/value） | 自动（长轮询） |

操作面板按钮与输入框：

| 控件 | 用途 | 需要的输入 |
|---|---|---|
| `Key` | 操作目标 key | 必填 |
| `Value` | 写入/发布的值 | Put / Publish / CAS 用 |
| `CAS expect` | CAS 期望值（空串=期望该 key 不存在） | CAS 用 |
| `回滚目标版本` | 回滚到哪个版本 | Rollback 用 |
| **写入 Put** | 普通 KV 写入 | Key + Value |
| **读取 Get** | 线性一致读 | Key |
| **删除 Delete** | 删除 KV | Key |
| **CAS 原子更新** | 条件更新 | Key + expect + Value |
| **发布新版本 Publish** | 发布配置版本 | Key + Value |
| **配置 + 历史** | 读最新配置 + 版本链 | Key |
| **一键回滚 Rollback** | 回滚到指定版本 | Key + 回滚目标版本 |

---

## 1. 查看集群状态（打开即见）

### 操作步骤
打开页面即可，无需任何点击。

### 应该出现的现象
- 顶栏右侧：`● 集群在线 3/3`；
- 左侧 3 个节点卡片，各有端口（:8001/:8002/:8003）、角色徽章、`term / commit / applied` 三个指标、peers 成员列表；
- **Leader 节点卡片金色高亮 + `LEADER` 徽章**，两个 follower 蓝色 `FOLLOWER`；
- 顶栏「集群节点」标题旁显示当前 leader 地址 `→ 127.0.0.1:800X`。

### 功能解释
- **多副本**：同一份配置由 3 个节点各存一份（每节点独立 LevelDB），页面卡片就是 3 个真实副本进程；
- **Leader 概念**：Raft 中所有写操作必须在唯一 Leader 上执行，卡片金色高亮的就是当前 Leader；
- **term / commit / applied**：term 是选举代数（每次选主 +1）；commit 是多数派确认过的日志位置；applied 是已写入状态机的位置。三者是 Raft 一致性的核心指标，可在故障转移章节看到它们变化。

---

## 2. 发布配置 + 实时推送（核心演示）

### 操作步骤
1. `Key` 输入 `gateway.rate_limit`
2. `Value` 输入 `2000`
3. 点「**发布新版本 Publish**」

### 应该出现的现象
- **结果区**出现 4 行：
  ```
  key       gateway.rate_limit
  value     2000
  version   1
  revision  1
  ```
- **右侧 Watch 流立即弹出**一条事件（无需等待）：
  ```
  #1  [PUT]  gateway.rate_limit    value: 2000 · v1
  ```

### 功能解释
- **配置中心 vs 普通 KV**：Publish 除了写主索引，还会写配置版本索引 `cfg/{key}/{ver}`（MVCC），所以这条配置进入"版本历史"、可回滚；
- **revision（全局逻辑时钟）**：每次任何写入全局 +1，是 MVCC / Watch / CAS 的共同锚点；
- **Watch 长轮询**：右侧事件流挂着一个 30s 的长轮询请求（监听所有 key），配置一变立即推送——对应"**改配置不重启、实时生效**"这个配置中心的核心卖点。

---

## 3. 读取配置

### 操作步骤
保持 `Key=gateway.rate_limit`，点「**读取 Get**」。

### 应该出现的现象
- 结果区显示与发布一致：`value=2000, version=1, revision=1`；
- Watch 流**不**新增事件（读不产生写入）。

### 功能解释
- 读走的是**线性一致读**：请求会路由到 Leader，Leader 确认 Lease/日志追平后才返回，保证"刚发布的配置立刻读到"——**杜绝读到过期配置**（如果直接让 Follower 读本地，可能读到旧值）。

---

## 4. KV 写入、读取、删除

### 操作步骤
1. `Key=app.timeout`，`Value=3000ms`，点「**写入 Put**」→ 结果区出现该 KV（version=1, revision=2），Watch 流弹 `#2 PUT`；
2. `Key=app.timeout`，点「**读取 Get**」→ 结果区显示 `value=3000ms`；
3. 点「**删除 Delete**」→ 结果区显示删除成功，**Watch 流弹 `#3 DELETE`**（红色标签）；
4. 再点「**读取 Get**」→ 结果区显示 `✗ code=1 KEY_NOT_FOUND`。

### 功能解释
- **Put / Get / Delete** 是基础 KV 三件套，全部经 Raft 多数派复制后才返回（写）、线性一致读（Get）；
- **DELETE 产生墓碑**：删除不是物理抹掉，而是写一条带 tombstone 的版本（MVCC 原则，保证 Watch 事件可追溯）；
- **Watch 区分 PUT/DELETE**：订阅方收到 DELETE 事件就知道"这个配置被下线了"，可做默认值/告警处理。

---

## 5. CAS 原子更新（并发治理）

### 操作步骤
1. `Key=gateway.rate_limit`，`CAS expect=2000`，`Value=8000`，点「**CAS 原子更新**」→ 成功（version 变 2）；
2. 再把 `CAS expect=2000`（旧值），`Value=9999`，点「**CAS 原子更新**」→ 失败：`✗ code=2 expect value mismatch`；
3. 点「**读取 Get**」→ value 仍是 8000，**没被 9999 覆盖**。

### 应该出现的现象
第一次成功、第二次 `code=2`、最终值保持第一次的 8000。

### 功能解释
- **Compare-And-Swap**：只有"当前值 == 期望值"才更新，否则拒绝——防止**多人并发改同一配置时互相覆盖**（同学 A 以为值是 2000 改成 8000，同学 B 拿着旧值 2000 想改 10000，会被原子拒绝）；
- **原子性来源**：CAS 作为 Raft 日志走 `on_apply` 串行执行，天然串行化，并发 CAS 恰好一个成功；
- **expect 空串 = 期望该 key 不存在**（可用于"仅当配置不存在时写入"的初始化场景）。

---

## 6. 多版本与历史（配置可追溯）

### 操作步骤
1. 用「**发布新版本 Publish**」再发布两次：`Value=8000`、`Value=12000`；
2. 点「**配置 + 历史**」（Key 仍为 gateway.rate_limit）。

### 应该出现的现象
- 结果区显示最新值 `value=12000, version=3, revision=4`；
- 下方**版本链**列出所有 Publish 过的版本：
  ```
  配置版本链（cfg/{key}/{ver} 索引）
  v1  rev 1   2000
  v2  rev 3   8000
  v3  rev 4   12000
  ```

### 功能解释
- **MVCC 多版本**：每次 Publish 保留一个版本副本，版本号（version）按 key 递增、revision 全局递增，两者共同构成可追溯的变更历史；
- **只有 Publish 产生版本**：普通 Put / Delete / CAS 不写配置版本索引，所以"配置 + 历史"只显示 Publish 的版本链——这是"配置版本"区别于"普通 KV 写入"的设计（面试可讲）。

---

## 7. 一键回滚（改错快速恢复）

### 操作步骤
1. `回滚目标版本` 输入 `1`；
2. 点「**一键回滚 Rollback**」。

### 应该出现的现象
- 结果区：`value=2000, version=4, revision=5`（**值回到 v1 的内容，但版本号是新的 4**）；
- Watch 流弹 `#5 PUT`；
- 再点「**配置 + 历史**」：版本链变成 `v1 / v2 / v3 / v4`（旧版本**全部保留**，v4 是回滚产生的新版本）。

### 功能解释
- **回滚 = 写回旧值产生新版本**，而不是"删掉新版本"——因为 Raft 日志是 append-only，不能抹除历史；
- 这保证**可全量追溯**：即使回滚了，审计日志里依然能看到"发布过 12000、曾回滚过"的完整轨迹（对齐 etcd 语义）。

---

## 8. Watch 实时事件流详解

### 操作步骤
无需操作，页面打开即自动运行。演示时在页面做任意写操作（或终端 curl 写入）。

### 应该出现的现象
- 每次任何写入（Put / Publish / Delete / Rollback / CAS 成功）右侧立即弹出事件卡片，revision 持续递增；
- PUT 绿色标签、DELETE 红色标签；
- 事件流自动滚动到底部；
- 页面静置超过 30s 无写入，事件流不报错，随后有写入仍能正常弹出（长轮询自动续接）。

### 功能解释
- **长轮询机制**：前端发起 `GET /v1/watch?from_revision=<last>&timeout_ms=30000` 挂住，30s 内无事件则返回并立刻发起下一个（带新锚点），实现"准实时"；
- **断点续传（revision 锚点）**：事件带全局 revision，断线重连后从 `current_revision` 继续，**不丢不重**；
- **监听所有 key**：key 为空即全局监听，适合演示"所有配置变更一屏尽收"。

---

## 9. 故障转移可视化（Raft 容灾演示）

### 操作步骤
1. 终端确认当前 leader 端口（页面顶栏有 `→ 127.0.0.1:800X`）；
2. 找到该端口进程并 `kill -9`：
   ```bash
   LPID=$(ss -tlnp 2>/dev/null | grep ":$L " | grep -oP 'pid=\K[0-9]+')
   kill -9 $LPID      # $L = 当前 leader 端口
   ```
3. 观察页面（最多几秒后自动刷新）。

### 应该出现的现象
- 被杀节点卡片**变红 + OFFLINE**，顶栏变 `部分节点离线 2/3`；
- 约 4 秒后，**另一节点卡片金色高亮 + LEADER**，顶栏恢复 `集群在线 3/3`；
- 用「**读取 Get**」读 `gateway.rate_limit`，**仍能读到数据**（value=2000, version=4），数据没丢；
- 期间若有人写，Watch 流照常推送。

### 功能解释
- **自动选主**：Leader 宕机后 Follower 超时发起选举（term+1），数秒内选出新 Leader——这就是 Raft 的 `Leader Election`；
- **数据不丢**：写请求必须多数派（2/3）确认才返回，所以任何单点宕机都不丢已提交配置——`Majority Replication`；
- **强一致读**：新 Leader 上读到的是已提交数据，不会出现"配置中心挂了 → 服务拉不到配置"的单点故障。

---

## 10. 跨节点访问：用 Follower 打开页面

### 操作步骤
1. 确认当前 leader 是 8001（页面顶栏）；
2. 用 **follower 节点**地址打开另一个标签页：`http://127.0.0.1:8002/dashboard`；
3. 在 follower 的页面上做「**发布新版本 Publish**」。

### 应该出现的现象
- 页面正常加载（样式/功能完整）；
- 写入**成功**（不是报错）；
- 按 F12 打开 Network 面板：请求 URL 实际发往 **8001（leader）**，响应头含 `Access-Control-Allow-Origin: *`。

### 功能解释
- **写请求自动打 leader**：前端从 `/healthz` 拿到 `leader_id`，写操作一律发往 leader，用户不用记哪个端口是 leader；
- **CORS（跨端口）**：8002 页面请求 8001 是跨端口请求，后端每个 REST 服务加了 `Access-Control-Allow-Origin` 头并处理 OPTIONS 预检，浏览器才放行。

---

## 附：前端操作 ↔ curl 命令对照（用于核对后端一致性）

| 前端按钮 | 等价 curl（`$L`=leader 端口） |
|---|---|
| 写入 Put | `curl -X POST http://127.0.0.1:$L/v1/kv/{key} -d '{"value":"v"}'` |
| 读取 Get | `curl http://127.0.0.1:$L/v1/kv/{key}` |
| 删除 Delete | `curl -X DELETE http://127.0.0.1:$L/v1/kv/{key}` |
| CAS 原子更新 | `curl -X POST http://127.0.0.1:$L/v1/kv/{key}/cas -d '{"expect":"e","value":"v"}'` |
| 发布新版本 | `curl -X POST http://127.0.0.1:$L/v1/config/{key} -d '{"value":"v"}'` |
| 配置 + 历史 | `curl "http://127.0.0.1:$L/v1/config/{key}"` |
| 一键回滚 | `curl -X POST http://127.0.0.1:$L/v1/config/{key}/rollback -d '{"target_version":N}'` |
| Watch 事件流 | `curl "http://127.0.0.1:$L/v1/watch?from_revision=0&timeout_ms=30000"` |
| 集群状态（节点卡片） | `curl http://127.0.0.1:$L/healthz` |
