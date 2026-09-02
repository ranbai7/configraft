# 05 · 代码走读：Watch 长轮询 · 配置语义 · 快照 · 成员变更 · 存储精讲

> 本章覆盖"实时推送"与"存储落地"两块：Watch 链路、发布/回滚/CAS、Compaction、快照、成员变更，最后是**存储 Key 布局与线程安全模型**（第 4 部分）。素材来源含 `src/watch/watch_hub.cpp`、`src/store/*`、`src/raft/*`；设计取舍可对照 `../watch.md`、`../m6.md`。

---

## 3.4 Watch 长轮询全链路

### 3.4.1 为什么选 HTTP 长轮询（不是 gRPC 流 / WebSocket）

| 维度 | HTTP 长轮询(本项目) | gRPC 流 / WebSocket |
|---|---|---|
| 实现复杂度 | 低；curl 即可演示 | 高；需维护长连接与心跳 |
| 连接成本 | 每次请求即用即走 | 长连接常驻 |
| 实时性 | 毫秒级(轮询挂起) | 事件一到即推 |
| 适用 | 演示/低规模订阅 | 大规模实时推送 |

**取舍**：本项目面向"配置变更推送"这种低频事件 + 追求可演示/可 curl 验证 → 长轮询最务实。etcd v2 正是长轮询、v3 才上 gRPC 流；**诚实承认这是工程权衡而非最优解**即可（面试不丢分）。

### 3.4.2 架构：一个 WatchHub，写路径是唯一事件源

```
ConfigraftServer 持唯一 WatchHub (watch_hub_)
事件只在 on_apply(集群) / LocalNode::Apply(单机) 落库成功后产生:
  写成功 → ApplyCmdToStore 返回 events → WatchHub::Broadcast(events)
        → 命中每个匹配的 waiter → 唤醒 → 长轮询返回
历史事件 → 不靠内存, 靠 Store::ReplayEventsLocked 扫 v/ 记录重放
```

- **Leader 和 Follower 都会在 on_apply 后 Broadcast**（`state_machine.cpp`）→ **Watch 可以挂在任意节点**。
- 事件只有两种：`PUT` / `DELETE`，由 `kv.deleted()` 判定。

### 3.4.3 长轮询一次请求的生命周期（`watch_hub.cpp:Watch`）

HTTP：`GET /v1/watch[/{key}]?from_revision=N&timeout_ms=MS`（key 空 = 监听全部；from_revision 默认 0 = 从现在开始，不重放历史；timeout 默认 30000ms）。

```text
WatchService::Rest → node_->Watch → WatchHub::Watch
① 追平检查: 若本地节点落后于 from_revision(重启/快照追赶中)
      → 等本地 applied 追平 from_revision (最多 ~5s)
      → 超时返回 INTERNAL + 保留锚点, 客户端重试
      ★原因: 防重放 ≤ from_revision 的旧事件造成重复
② Register(waiter) 挂入 hub
③ 立即重放 (from_revision, current] 区间已提交历史:
      事件来自 Store::ReplayEventsLocked 扫 v/ 前缀
      ★先 Register 后重放 = 关闭"新事件 vs 历史"的缝隙窗口
④ 若还没事件:
      等待循环挂 bthread cv (≤ timeout_ms) 等 Broadcast 唤醒
⑤ 返回:
      有事件  → events + current_revision = 最后一条事件的 revision
      超时空 → 空结果 + current_revision = 当前 store revision
      缓冲溢出→ code=INTERNAL + current_revision(供重连重放)
      历史被回收→ code=COMPACTED + current_revision(供重建)
```

### 3.4.4 断点续传的锚点语义（最容易被问的细节）

**`current_revision` 是下一次请求的 `from_revision`。** 规则：

- **有事件时：锚点 = 最后一条事件的 revision**（`watch_hub.cpp`）。为什么不能直接用 store 的当前 revision？因为从"最后一条事件"到"本轮结束"之间可能又发生了新写——若用 store 当前值，那些新写的事件就**被跳过**了。用"最后事件的 rev"保证不丢。
- **无事件时（超时空返回）：锚点 = 当前 store revision**——因为没有事件发过，从当前继续即可。

**不丢不重的三层保证**：①事件都带单调 revision，客户端用锚点续传 → 不丢；②重放区间开 `(from, current]`，本地落后先追平 → 不重；③历史若已被 Compaction 回收 → 明确返回 `COMPACTED`，不会静默跳过。

### 3.4.5 慢消费者背压（绝不让慢消费者拖垮 on_apply）

每个订阅者（waiter）有一个事件队列，上限 `kMaxWatchEvents=1024`（`watch_hub.h`）。**Broadcast 时若队列已满**：

- 置 `overflow` 标志 → **清空缓冲并断开该订阅者**；
- 响应 `code=INTERNAL(100)`，**并带上 `current_revision`**；
- 客户端拿这个锚点**重新长轮询重放**补齐。

为什么是"断开"而不是"无限缓冲/丢弃"：配置中心订阅者可能长时间不消费（如断网），无限缓冲 = 内存膨胀 + 事件陈旧；直接丢弃 = 静默丢事件；**断开 + 锚点重放** = 明确让客户端自己恢复，且不阻塞 on_apply（写路径绝不等待任何订阅者消费）。

### 3.4.6 COMPACTED 判定（两处，都要知道）

1. **快速路径**（`watch_hub.cpp`）：`from_revision > 0 && store->CompactRev() > from_revision` → 直接 `COMPACTED`；
2. **重放内再判**（`store.cpp`）：迭代中若发现 `from < compact_rev` 也返回 `COMPACTED`。
   响应带 `current_revision = store->CurrentRevision()`，客户端从该点重建。`compact_rev` 的含义与维护见 3.6。

### 3.4.7 WatchHub 并发模型（背诵要点）

- 锁序：**`hub_mu_` → waiter->mu**（单向加锁，避免死锁）。
- Broadcast：先拷 **weak_ptr 快照**（放锁后再逐个 lock），避免持 hub 锁做耗时操作；对不匹配 key 的 waiter 用 `Waiter::Matches` 过滤。
- 等待循环挂 **bthread cv**（原因见 02 章：不占 pthread worker，可承载海量长轮询）。

---

## 3.5 配置语义：发布 / 历史 / 回滚 / CAS

### 3.5.1 Publish 与普通 Put 的关系

`Publish` = 普通写 + **额外维护 cfg 版本索引**：同一 WriteBatch 里除了写 `k/{key}`（主索引）、`v/{rev}/{key}`（全局历史），再多写一条 `cfg/{key}/{version}`（配置版本索引）。`version` 是该 key 的修改序号（从 1 递增），`revision` 是全局时钟——两者在 Publish 时同时推进。

### 3.5.2 GetConfig(key, version)（`store.cpp`）

```text
version ≤ 0 → 读主索引 k/{key} = 最新版
version > 0 → 读 cfg/{key}/{ver} 索引
    命中且非 tombstone → 返回该版本 KV
    未命中或已删除     → VERSION_NOT_FOUND (版本已被 Compaction 回收或不存在)
```

### 3.5.3 历史查询 GetHistory（层级 key 前缀碰撞，M3 修复点）

遍历 `cfg/{key}/` 前缀下的所有版本——但 `key="a"` 与 `key="a/b"` 的 `cfg/` 前缀会**碰撞**（`cfg/a/` 前缀能匹配到 `cfg/a/b/`）。修复：**每条记录都要校验 `kv.key() == key`**，过滤掉前缀碰撞混进来的别的 key（`store.cpp:GetHistoryLocked`）。天然按 version 升序返回（hex 编码保序）。

### 3.5.4 Rollback：为什么不删历史，而是"写回产生新版本"

`Rollback(key, target_version)` 的实现：

```text
1. 读 cfg/{key}/{target_version} → 拿目标版本的值
2. 用该值执行一次 Publish (写回) → 产生一个"新的更高版本"
```

**为什么不直接让主索引指回旧版本 / 删掉后续历史？**
- 本项目底层是 **Raft 复制日志（append-only）**：日志不可改写，状态机自然也只能向前推进，不能"回到过去"地覆盖。
- 这也正是 etcd 的语义：**历史是不可变的事实**，回滚只是"再发布一次旧值"，审计上"X 在 rev N 回滚到旧值"也是一条可追溯的写。
- 好处：Watch 会照常推一条 PUT（订阅者感知到"值变了"），各副本天然一致（因为回滚也是一条日志）。

### 3.5.5 CAS：原子 + 失败不消耗 revision（`store.cpp:CompareAndSwapLocked`）

CAS 作为一条 RaftCmd 进日志，在 on_apply **串行执行**（04 章 4.4 已铺垫），细节：

```text
当前值(主索引) 与 expect 比较:
  匹配     → 写新值: NextRevision + 落盘 + 产生 PUT 事件 → 成功
  不匹配   → 返回 CAS_FAILED, 不写任何东西       ← 不消耗 revision!
```

- `expect == ""`（空串）表示**期望 key 不存在**（含 tombstone：`absent = !existed || old_kv.deleted()`）。
- **文档化的歧义**：若一个 key **真实存在且值就是空串**，则无法用 CAS 更新它（因为空串被当成了"期望不存在"）。这是设计上的取舍，README/注释里写明了。
- **并发 N 个 CAS 恰一个成功** 的原子性由"单 Leader 串行 apply"保证，`raft_cas_test` 专门验证（07 章）。

---

## 3.6 Compaction 与 compact_rev 水位线（`store.cpp:CompactionLocked`）

**为什么需要**：MVCC 每次写追加历史，不回收会无限膨胀（空间放大）。后台线程周期回收（`server.cpp`：每 60s 一次、每 key 保留最近 10 版）。

```text
Compaction 一轮(持 Store 共享锁, 与读并发):
① 遍历 v/ 前缀, 按 key 归集所有 (revision, version) —— hex 编码 ⇒ 天然升序
② 对每个 key: 把最旧的 "excess = 数量 - 保留上限(10)" 个版本的
   v/{rev}/{key} 与 cfg/{key}/{ver} 记录 batch.Delete
③ 把这些删除的最高 revision 记为 max_deleted_rev
④ 把 max_deleted_rev 并入 meta/compact_rev, 与删除放同一个 WriteBatch 原子提交
```

**关键设计点**：
- **compact_rev 与删除同批原子提交** → 读者要么"既看到删除也看到水位上涨"，要么都看不到——杜绝"Watch 判断 from<compact_rev 该报 COMPACTED、但那条历史其实还在"的不一致。
- 持**共享锁** → 与并发读/写/快照导出并行，互不阻塞（LevelDB 单进程读安全）。
- 客户端若用已被回收的 from_revision 续传 → 返回 `COMPACTED`（见 3.4.6），从 current_revision 重建——这是"不能无限留历史"与"Watch 不丢"之间的明确边界。

---

## 3.7 快照：Save / Load（新节点追平的弹药）

### 3.7.1 Save（Leader/Follower 都会定期做）

```text
ConfigraftStateMachine::save_snapshot
  → 在独立 bthread 上执行(不阻塞 apply)
  → Store::ExportSnapshot(同一个 LevelDB Snapshot 下完成三件事):
      ① 读 meta/revision
      ② 遍历主索引 k/  (全部当前 KV)
      ③ 遍历配置索引 cfg/(全部配置版本)
    → 打包成 proto SnapshotData{revision, kvs, cfg_kvs} 落盘
```

**为什么必须"同一个 LevelDB Snapshot"**：LevelDB 的 `GetSnapshot()` 钉住一个一致的读视图。如果分别读 revision、k/、cfg/ 而中间有写进来，导出的三部分可能**来自不同时间点** → 新节点装完快照后 revision 与数据对不上（跨节点发散）。审查修复的 M1 项就是"快照 revision 在 snapshot 外读取"。

**为什么 cfg_kvs 必须带上**（M2 修复项）：`SnapshotData` 初版只有主索引 kvs，没有 cfg 版本索引 → 新节点装快照后**按版本读配置（GetConfig(version>0)）与历史全丢**。

### 3.7.2 Load（新节点 / 落后节点追上时）

```text
on_snapshot_load → Store::LoadSnapshot
  (持 Store 独占锁 db_mu_ —— 见 3.8.4 / 4.3 线程模型)
① db_.reset()                // 释放旧 DB(可能正在被并发读!→ 故必须独占锁, 防 UAF)
② remove_all 删除数据目录, 重开 LevelDB
③ 用一个大 WriteBatch 恢复: 主索引 k/ + (由 kvs 回填的)历史 v/ + cfg/ + meta/revision
④ compact_rev 重置为快照的 revision
   (因为快照只带当前状态、历史已被丢弃 ⇒ 旧 from_revision 一律按 COMPACTED 处理)
```

**面试高频题"快照安装时的 UAF 怎么修"**：`LoadSnapshot` 会把 `db_` 指针 reset 并重建目录。若此刻并发读/Compaction 正在用旧 `db_`，就是悬垂指针 → UAF（审查 H1，HIGH）。修复 = Store 引入共享互斥量：**重建（LoadSnapshot）持独占锁，读/写/Compaction/快照导出持共享锁**（详见 4.3）。

---

## 3.8 成员变更 + LocalNode

### 3.8.1 AddPeer / RemovePeer（`raft_node.cpp:AddPeer/RemovePeer`）

```text
AddPeer(peer)
① IsLeader? 否 → NOT_LEADER + 重定向
② 解析 peer 地址 → node_->add_peer(pid, new ConfChangeClosure(outcome))
③ bthread cv wait_for(100ms) 轮询, 总超时 ~60s
④ ConfChangeOutcome 用 shared_ptr 缓冲结果:
     同步等待超时先返回后, braft 回调仍能安全写入(无栈悬垂→防 UAF)
     超时提示 "成员变更仍可能在进行"
```

**RemovePeer 移除 leader 自身**：braft 提交配置变更后会**主动让位**（leader 被移除 → `ELEADERREMOVED`），集群自动重新选举。这避免"Leader 被移除但仍占着领导位置"的悬空态。

### 3.8.2 为什么新节点能追平（braft 帮你做的）

`add_peer` 成功路径上，braft **先让新节点通过快照/日志追平到当前**，然后才把"成员变更配置日志"提交进集群——即"先追数据、后改配置"，避免新节点带着空状态加入而拖垮提交。实测：新节点经快照安装（index 可能一下追到几十）后即可服务。**你不用自己写 InstallSnapshot 追赶逻辑，braft 在 `add_peer` 里处理**——但你要理解它的存在，才能回答"扩容为什么不会瞬间丢请求"。

### 3.8.3 单机模式 LocalNode 与"为什么要加锁"（审查 H2）

`LocalNode` 是"没有 Raft 的单机直调 Store"，用于调试/Dashboard/http_test。它的 `Apply`：

```cpp
LocalNode::Apply(cmd):
  lock_guard mu_                     // ★为什么需要这把锁
  ApplyCmdToStore(cmd) → Store 落库 → WatchHub.Broadcast
```

**为什么单机也要锁**：集群模式下"串行"由 braft 的 `on_apply` 天然保证（一条条来）；但单机模式没有 Raft，多个 brpc worker 会**并发**调用 `Apply`，而 Store 的 revision "读-改-写"不是原子的 → 并发写会拿到**重复的 revision**（Watch 续传错乱）。审查确认后在此加 `std::mutex` 串行化——本质是**把 Raft 的"单写者"约束在单机模式里手动补上**。

### 3.8.4 Store 线程安全模型总览（第 4 部分的桥梁）

Store 用一把 `shared_mutex db_mu_` 保护 `db_` 生命周期与并发访问：

| 操作 | 锁 | 说明 |
|---|---|---|
| LoadSnapshot（重建 DB：reset+删目录+重开） | **独占锁** | 防 db_ 被并发读/Compaction 使用期间释放 → 防 UAF |
| 读 / 写 / Compaction / ExportSnapshot | **共享锁** | 彼此并行，LevelDB 单进程并发读安全 |

详见下方 4.3 的 *Locked 设计。

---

## 4.1 存储 Key 布局与编码（看懂一半存储设计的钥匙）

LevelDB 是按字典序排 key 的。本项目利用"编码保序"把 revision 编成可范围扫的 key：

```text
前缀          内容                                  编码
meta/revision   全局逻辑时钟(8B)                    EncodeUint64 大端
meta/compact_rev Compaction 水位(8B)                EncodeUint64 大端
k/{key}         主索引 = 当前最新 KV(整体 proto)      明文 key
v/{rev:16hex}/{key}   全局历史: 每次写追加一份完整 KV   EncodeOrd: %016llx 16位小写hex
cfg/{key}/{ver:16hex} 配置版本索引                     EncodeOrd
```

**两种编码为何不同**（`src/common/storekey.cpp`）：
- `EncodeOrd`（16 位定宽 hex）：**字符字典序 == 数值序** → 扫 `v/` 前缀即按 revision 升序遍历、Seek 到 `v/{from+1}` 即可从某点开始。范围遍历/续传都靠它。
- `EncodeUint64`（8 字节大端）：供 `DecodeUint64` 快速读写的计数器（meta 前缀），不需要按内容范围遍历。

> 面试一句话："我让 revision 在磁盘上的编码保持字典序=数值序，于是'从第 N 个 revision 之后的历史'就变成一次前缀 Seek，Watch 历史重放、GetHistory、Compaction 全都复用它。"

### 4.1.1 主索引里存什么

`k/{key}` 存的是**整个当前 KV（proto，含 value/version/revision/deleted 字段）**，不是只存 revision。Delete = 写一条 `deleted=true` 的空 KV 进主索引与历史（**tombstone**），并让 version+1；Get 读主索引时若 tombstone/缺失 → `KEY_NOT_FOUND`。**删除不物理删旧数据，只是盖一层墓碑**——这也是 MVCC 让"历史审计"完整的原因。

---

## 4.2 revision 的原子性与单批处理

### 4.2.1 NextRevisionLocked(WriteBatch*)：为什么放一个 batch

```cpp
rev = meta/revision 当前值 + 1
batch.Put(meta/revision, rev)      // ★递增写进调用方传入的 WriteBatch
... 调用方继续往同一 batch 写业务数据 ...
batch 原子提交
```

**若不这么做会怎样**：revision 独立写、业务数据独立写，两步之间若失败 → 出现"revision 已推进但这条记录没落盘"的**半提交** → 不同节点看同一 revision 却对应不同数据 → 状态发散（审查 M4）。合成一个 WriteBatch 后，**要么都生效、要么都不生效**。

### 4.2.2 BatchPut 为什么"先读一次、本地递增"，而不是逐条调 NextRevision

WriteBatch 未提交时**读不到自己刚写的内容**——若 BatchPut 里逐条 `CurrentRevisionLocked()` 都去读 db，会读到同一个旧值 → **重复 revision**。正确做法（`store.cpp:BatchPutLocked`）：

```cpp
rev = CurrentRevisionLocked()        // 只读一次
for 每条: 本批内分配 rev+i; 每条数据用本地递增的 rev 写 batch
批末: batch.Put(meta/revision, rev + n)   // 只写一次 meta
```

---

## 4.3 Store 线程安全模型：shared_mutex + *Locked（审查 H1 的核心）

### 4.3.1 背景：快照安装 UAF

`LoadSnapshot` 会 `db_.reset()` + 删目录 + 重开。修复前无锁，若此刻有并发读/Compaction 正在用旧 `db_` → **UAF 崩溃**（follower 装快照 + 并发读即复现）。

### 4.3.2 锁分工与 *Locked 防重入

- `db_mu_`（shared_mutex）：LoadSnapshot 持**独占锁**；公共读/写/Compaction/ExportSnapshot 各自持**共享锁**。
- **问题**：shared_mutex **不是递归锁**。Store 的公共方法存在嵌套调用（`GetConfig → Get`、`Rollback → GetConfig + Publish`、`Compaction → CompactRev`、`ReplayEvents → CompactRev`）——若每个公共方法都各自 `shared_lock`，嵌套时会**二次加同一把 shared_mutex** = 未定义行为。
- **解法**：拆成**公共方法 + 私有 `*Locked` 实现**两层：公共方法加一次锁后调用私有 `*Locked`，嵌套调用全部走 `*Locked` 版，不再重复加锁。

```text
公共:  GetConfig(key, ver)  { shared_lock; return GetConfigLocked(key,ver); }
私有:  GetConfigLocked(...) { ...GetLocked(...)   // 直接调私有, 不重新加锁
                             ...PublishLocked(...) }
```

> 面试一句话："我引入 shared_mutex 让快照重建与并发读互斥，但 shared_mutex 不是递归锁，公共方法嵌套调用会二次加锁 → 我把所有实现拆成公共方法 + *Locked 私有层，公共方法只加一次锁。加锁后同环境复测性能零回退（±5% 内）。"

### 4.3.3 ExportSnapshot 的一致性（呼应 3.7.1）

`ExportSnapshot` 也走 `*Locked` 并在**同一个 LevelDB snapshot** 下读 revision + k/ + cfg/（`store.cpp` 注释说明为什么不能分开读）。

---

## 【本章自查】

1. 讲清 Watch 一次长轮询的生命周期（追平→Register→重放→等待→返回）。
2. `current_revision` 锚点在"有事件/无事件/溢出/COMPACTED"四种情况下各是什么？为什么有事件时不能用 store 当前值？
3. 慢消费者为什么用"断开+锚点重放"而不是"无限缓冲/静默丢弃"？为什么不阻塞 on_apply？
4. Rollback 为什么不删历史而是写回新版本？（Raft append-only / etcd 语义）
5. CAS 的原子性来源、失败不消耗 revision、`expect=""` 的 tombstone 语义、空串值歧义。
6. compact_rev 为什么与删除同批原子提交？COMPACTED 在快速路径和重放内各怎么判？
7. LoadSnapshot 为什么持独占锁？快照导出为什么必须同一个 LevelDB snapshot？cfg_kvs 为什么必须带？
8. 单机 LocalNode 为什么也要加锁？（H2）
9. 画出 k/ v/ cfg/ meta 四类 key；解释 EncodeOrd 与 EncodeUint64 的区别与各自用途。
10. NextRevisionLocked 为什么与业务写同批？BatchPut 为什么先读一次本地递增？

---
**下一篇**：[06-踩坑与关键问题.md](06-踩坑与关键问题.md)——工程史：这些问题和修复是你"亲写过"的最好证明。
