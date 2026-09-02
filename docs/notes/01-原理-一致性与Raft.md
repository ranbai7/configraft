# 01 · 原理：一致性模型 · 复制状态机 · Raft 核心 · 一致读问题

> 本文件回答面试官会问的**第一层原理题**。考点驱动的取舍是：**必考的概念讲到能推导、能举例、能接到本项目的代码；太深但不常考的内容给一句结论 + 延伸文档**。
> 深度延伸请读 `../raft-and-braft.md`（Raft 原理+braft 源码精读+落地，全库天花板）与 `../m5.md`（Lease 论证）。

---

## 1.1 一致性模型辨析（送分但必须概念干净）

面试最常见的是让你比较几个"一致性"。**用"一个例子"讲最有效**：

> 例子：配置 `max_conn = 100`，t1 时刻被改成 `200`。之后 t2 发起一次读，能读到什么？

| 模型 | 含义（直观） | 上例结果 | 本项目 |
|---|---|---|---|
| **强一致（线性一致 Linearizability）** | 读到的结果就像系统只有一个副本：**一旦某次写被确认，之后开始的读一定能看到它**；所有操作能排成一个全局合法的先后序 | t2 的读必为 200 | 写（Raft 提交）；Get 默认 |
| **顺序一致（Sequential）** | 所有进程看到同一个合法的操作顺序，但**该顺序不必与真实时间一致**（写可能"晚于真实时间"才生效） | 可能读到 100 | —（Raft 满足的比这更强） |
| **因果一致（Causal）** | 有因果关系的操作按因果序可见；无因果的可乱 | 弱于顺序一致 | — |
| **最终一致（Eventual）** | 不保证读到新值，只保证"停止写后最终收敛" | 可能读到旧值 | `serializable=true` 的 Follower 本地读（用于监控） |

**本项目对外承诺**（见 README「一致性保证」表，面试直接背）：

| 操作 | 一致性 | 实现 |
|---|---|---|
| 所有写 | 线性一致 | Raft 多数派提交 → `on_apply` 串行落库 |
| `Get`（默认） | 线性一致 | Leader Lease 校验 + applied≥commit 后本地读 |
| `Get`（`serializable=true`） | 可轻微过期 | Follower 本地读（监控场景） |
| `Watch` | 有序、不丢、可续传 | 事件来自 apply 序列，按全局 revision 排序 |

> **背诵点**：Raft 保证的是**复制状态机层面的强一致（可等价看成线性一致）**；它给系统提供的是"日志序列全副本一致"，业务在此基础上再决定读怎么走（ReadIndex/Lease）。

---

## 1.2 复制状态机（SMR）与共识问题

**状态机复制（State Machine Replication, SMR）** 是分布式一致性的经典蓝图：

```text
初始状态相同
   + 相同的输入序列（日志）
   + 确定性状态机（同样的输入产生同样的状态转移）
= 各副本最终状态相同
```

**关键推论**：我们不直接同步"状态"，而是同步"操作序列"。只要各副本按**同一个顺序**执行同一条日志，状态就一致。这也是为什么 Configraft 把"写 = 一条 RaftCmd 进日志、`on_apply` 串行落库"作为铁律——**串行**是 SMR 的灵魂，任何副本都不能跳过日志直接改状态（否则一旦 Leader 故障，新旧 Leader 状态就对不上了）。

**共识问题**（Consensus）：一堆节点对"下一条日志是什么"达成一致，并且要容忍部分节点故障（CFT，Crash Fault Tolerance）。Raft 就是 Paxos 家族里**可读性最好**的一个工程化实现，被 etcd、TiKV、braft 等大量使用。

**为什么单机/主从复制不够，要共识？**
- 主从（异步复制）：主挂了，从库可能没收到最后几条 → 丢数据 / 脑裂双主。
- 共识的答案：**多数派**。只有"超过一半节点确认"才算提交，任何时刻最多只有一个多数派能推进 → 天然排除双主同时提交（两个多数派必有交集）。

> 面试一句话："我选择 Raft 而不是主从复制，是因为要**强一致 + 自动选主 + 容忍节点故障**；Raft 把'谁说了算'收敛到多数派，多数派两两相交，杜绝脑裂。"

---

## 1.3 Raft 核心（可讲版）

> 目标不是复述论文，是让你能**用本项目的话**讲清楚。每个机制都给"为什么 + 本项目在哪用"。

### 1.3.0 基本概念

- **节点角色**：Leader（处理写）/ Follower / Candidate。正常时一个 Leader，其余 Follower。
- **任期 Term**：单调递增的逻辑时钟，每次选举 +1。**任期是安全性的锚**——所有消息都带 term，term 大的消息才能覆盖 term 小的。
- **日志条目**：每台机器一份，`(term, index)` 全局唯一定位一条日志；`commitIndex` 已提交，`appliedIndex` 已应用。
- **选举超时（Election Timeout）**：Follower 随机化（如 150–300ms）的计时器，超时未收到 Leader 心跳就发起选举。**随机化**避免所有节点同时竞选导致选票分裂。

### 1.3.1 选举（Leader Election）

过程：Follower 超时 → term+1 转 Candidate → 先投自己 → 广播 `RequestVote` → 收到**多数派**投票（含自己）→ 成为 Leader → 立刻发心跳（AppendEntries，空日志也算）稳住任期。

本项目落点：braft 默认选举超时 1000ms（`run_cluster.sh` 里配 `election_timeout_ms=1000`）；Leader 挂了约几秒内自动选出新 Leader。

### 1.3.2 日志复制（Log Replication）

1. 客户端写 → Leader 把条目追加到自己的日志（term 用当前任期）；
2. 并行发 `AppendEntries` 给所有 Follower；
3. 条目被**多数派**（含 Leader 自己）复制 → Leader 更新 `commitIndex`，并推进 `appliedIndex` 应用到状态机；
4. 下一轮心跳/追加时把 commitIndex 带给 Follower，Follower 也提交并应用。

### 1.3.3 提交条件与 Leader Completeness（必考，你的 Lease 论证用它）

**提交规则（只由 Leader 决定）**：**当前任期**的日志条目只有在它被复制到多数派后才算提交。**上一任期的条目不会直接提交**——必须等到当前任期有一条新日志被提交，通过日志匹配规则间接提交（防止 Figure 8 问题）。

**Leader Completeness（领导人完备性）**：如果一个日志条目在某个任期被提交，那么它**必然出现在之后所有任期的 Leader 日志中**。

**推论（本项目 Lease 论证的承重墙）**：新 Leader 当选后，**braft 会自动往日志里写一条配置条目（no-op）**并提交。此时由 Leader Completeness：**当前任期之前所有已提交的日志条目都必然已经在新 Leader 的日志里**——于是只要新 Leader 把它们 apply 掉，它的状态机就包含了此前一切已提交的写。这就是"读"能安全的前提。

> 面试一句话："Raft 为什么强制新 Leader 先提交 no-op？表面是'借一条日志把 commitIndex 推过旧任期日志'，本质是**用 Leader Completeness 保证旧任期的已提交写都已就位**——这让后来的本地读不会漏掉历史。"

### 1.3.4 安全性五不变量（能报出名字+一句话即可）

1. **Election Safety**：任一任期最多一个 Leader 当选。
2. **Leader Append-Only**：Leader 从不在自己的日志中覆盖或删除条目。
3. **Log Matching**：两日志某 index 的 term 相同 ⇒ 该 index 之前的所有条目一致。
4. **Leader Completeness**：已提交的条目必然出现在之后所有 Leader 日志中（见上）。
5. **State Machine Safety**：一旦状态机应用了某 index 的条目，就不会有别的节点在该 index 应用不同的条目。

### 1.3.5 日志不一致与追赶（Log Mismatch & Repair）

Follower 可能缺日志/多日志/有冲突（曾宕机、曾脑裂）。Leader 用 `nextIndex[]` 从后往前探测：AppendEntries 带 `prevLogIndex/prevLogTerm` 做**一致性检查**，不匹配就回退 `nextIndex` 重试，直到找到公共前缀，然后把 Leader 的日志**强制覆盖**给 Follower。**冲突条目只可能被 Leader 覆盖，Follower 从不反向改 Leader。**

本项目落点：新节点加入或落后节点恢复时，靠这套机制+快照追赶（见 05 章 3.8）。

### 1.3.6 成员变更（Configuration Change）与快照（Snapshot）

- **成员变更**：Raft 论文建议 joint consensus（两阶段）；工程实现（braft 等）常简化为**单增/单删**配置变更（先加日志里的配置条目，一次只加/删一个节点，并保证新节点先追平数据再提交配置）。详见 05 章 3.8。
- **快照**：日志无限增长 → 定期把状态机状态存成快照并丢弃旧日志；落后节点装快照追赶（InstallSnapshot）。本项目快照=把 LevelDB 当前全量状态导出成 `SnapshotData`（含主索引 + 配置索引 + revision），详见 05 章 3.7。

> 深度延伸：Figure 8 反例、乱序场景、`raft-and-braft.md` §6–§11 有源码级讲解。

---

## 1.4 一致读问题：为什么"Leader 本地读"不够

> 这一节是**整个项目技术含量最高的一层**，也是面试官最想挖的地方。把它彻底搞懂，本项目就立住了。

### 1.4.1 问题定义

写走 Raft 后是线性一致的。那 **读** 呢？最直觉的方案：读也打到 Leader，Leader 读自己的状态机。但这**不保证线性一致**，因为：

**Leader 本地状态机可能滞后于"已提交"的日志。**

经典反例（Raft 论文 §8 / Figure 8 精神）：

1. 旧 Leader L1 在 term 1 复制了一条写 W 到多数派并提交；
2. L1 还没来得及 apply W 到自己的状态机就宕机/与集群断开；
3. 集群选出新 Leader L2（它的日志里含有 W，因为 W 已在多数派）；
4. **如果 L1 还活着但在网络孤岛上继续对外提供"读"**（它以为自己还是 Leader）：
   - L1 本地状态机没有 W → 读到旧值；
   - 而客户端此刻可能已经被 L2 确认过"W 已生效" → 读到旧值 = 违背线性一致。

**还有第二种更隐蔽的滞后**：某节点是 Leader，但它的 `appliedIndex` 落后于 `commitIndex`（新日志刚提交、正在 apply 或刚从故障恢复还没追上），此刻读状态机也会漏掉刚确认过的写。

### 1.4.2 为什么必须"过了某道闸门"才能读

所以一致读 = **读之前，先保证"本节点状态机 ≥ 所有在此之前已提交的日志"**，且 **"没有别的 Leader 在我不知道的情况下提交过新东西"**。业界三种主流方案：

| 方案 | 思路 | 优点 | 缺点 | 代表 |
|---|---|---|---|---|
| **Quorum（多数派）读** | 读也进 Raft：读当前 commitIndex 的 Quorum，读最新 | 实现简单直接 | 每次读一次 RPC，吞吐上不去 | ZooKeeper sync 读等 |
| **ReadIndex** | Leader 先跟多数派确认"我仍是 Leader"并取回 commitIndex，**本地等 applied≥commitIndex 后**本地读 | 读只需一次轻量 Quorum 心跳；吞吐高 | 需要框架支持 | etcd（老版本）、TiKV、**新 braft** |
| **Lease（租约）读** | Leader 利用"最近收到过多数派心跳/确认"作为"我是唯一 Leader"的证据，到期前本地读 | 读**零 RPC**，纯本地 → 吞吐最高 | 依赖时钟/lease 参数；可用性窗口略差 | 本项目（braft lease）、etcd 早期 |

**etcd 的演进正是个绝佳的对照素材**：etcd v2 时代读是"leader 直接读 + lease"，后来为绕开 lease 的时钟依赖、让 Follower 也能做一致读，v3 才引入基于 **ReadIndex** 的方案。而**本项目选 Lease 是被动且正确的**——braft v1.1.2 这个版本**根本没有暴露 ReadIndex API**（作者搜过源码确认），所以走它内置的 leader lease。

### 1.4.3 本项目的一致性读设计（预告，04 章完整证明）

**braft 的内置 leader lease 机制**：开启 `FLAGS_raft_enable_leader_lease` 后，Leader 通过周期性成功的多数派心跳/响应**续租**；lease 有效期内：
- Leader 认为自己是"稳定 Leader"（不会被静默赶下台）；
- **Follower 也遵守该 lease**——lease 有效期间不会给"任期更高"的候选者投票、不发起新选举（这是"所有节点必须一致开启"的原因）。

于是 Lease 读的完整逻辑（本项目 `RaftNode::Get`，`src/raft/raft_node.cpp:166`）：

```
① Lease VALID
   含义1: 本任期 no-op 配置条目已提交        → 由 1.3.3：旧任期已提交写都已在 Leader 日志里
   含义2: 最近与多数派有交互，我是唯一 Leader → 不会有别的 Leader 提交过我不知道的写
② applied ≥ commit                        → 把上面"已在日志里"的都 apply 进状态机
③ 本地 store->Get(key)                    → 现在状态机 ≥ 一切此前已提交的写 → 线性一致
```

**为什么 Lease 方案能保证读到"最新已提交写"而不是"最新日志"**：读语义只需要"读到 ≤ 读操作开始时刻之前的所有已提交写"，不需要"和写同时并发"，所以"多数派确认过的写 + 我已追上 apply"就足够。

### 1.4.4 Lease vs ReadIndex 取舍表（背诵）

| 维度 | Lease | ReadIndex |
|---|---|---|
| 每次读额外开销 | 无（读窗口内纯本地） | 1 次多数派轻量 RPC |
| 依赖 | 系统时钟偏差小、lease 参数合理 | 无时钟依赖 |
| Leader 切换后的可用窗口 | 需等 lease 重新 VALID（有短暂不可读） | 当选 + 追平即可读 |
| 提供方 | braft v1.1.2 内置 | 该版本未提供 |
| 本项目选择 | ✅ | ❌（不可用） |

> 诚实加分句："如果 braft 提供了 ReadIndex 我会更倾向 ReadIndex（不依赖时钟、Leader 切换后可读更快）；但 v1.1.2 没有，所以我用 Lease 并配了 'lease 无效期读会短暂不可用' 的兜底。这是工程上的务实选择，我在文档里写明了两者差异。"

### 1.4.5 一致性概念常被追问的几个细节

- **"serializable=true 是什么意思？"**：允许读打到 Follower，读本地状态机（可能轻微过期）。一致性模型是**可串行化**弱一档到**最终一致**之间——README 定义为"可容忍轻微过期，用于监控"。相当于给了用户"我要一致读 / 我要高可用读"的开关。实现上只是 `Get` 里 `serializable` 为真时**跳过 lease 校验直接读本地**（Follower 本地读）。
- **"读会阻塞写吗？"**：不会。Lease 读是本地读，Store 层读持**共享锁**、写持**互斥**语义仅在 `LocalNode` 单机串行写时用一把 `std::mutex`；集群写发生在 braft 提交阶段，与本地读并发。
- **"为什么写请求打到 Follower 会返回 NOT_LEADER？"**：只有 Leader 能决定日志序；Follower 收到写会回 `NOT_LEADER` + `LeaderId`，客户端据此重发（见 04 章 3.3）。

---

## 【本章自查】

1. 用配置改动的例子讲清线性一致 vs 顺序一致 vs 最终一致；本项目各操作属哪种？
2. 解释"为什么同步日志而非同步状态"；SMR 的三个条件是什么？
3. 复述 Raft 选举/复制/提交的流程；多数派为什么两两相交？
4. 说出安全性五不变量的名字；**Leader Completeness** 为什么是 Lease 读的前提？
5. 讲出"Leader 直接本地读为什么不线性一致"的两个滞后场景。
6. 对比 Quorum/ReadIndex/Lease 三种一致读方案，说出本项目选 Lease 的直接原因。
7. Lease VALID 在 braft 里为什么需要"所有 peer 一致开启"？（Hint：Follower 也要遵守 lease 不投票。）

---
**下一篇**：[02-原理-brpc与存储.md](02-原理-brpc与存储.md)——为什么系统能快：bthread 与 LevelDB。
