# 02 · 原理：brpc/bthread 运行模型 · LevelDB/LSM · MVCC 与逻辑时钟

> 本文件回答"这个项目为什么快、这套存储为什么够用"。这三块是后端大厂面试**反复出现**的原理题，且都能在本项目找到真实落点（尤其 bthread 那节——你真实踩过 `std::condition_variable` 假死的坑）。
> 注：仓库里 `../brpc.md` 是空文件，本节是把这块**从零补齐**的重点章节。

---

## 2.1 brpc & bthread：为什么"阻塞"必须用对原语

### 2.1.1 传统的线程模型 vs brpc 的 bthread

- 经典**线程池模型**：每个请求占一个 OS 线程。请求里若有阻塞 IO/锁等待，**该 OS 线程被挂起**——线程池很快被占满，只能靠"线程数=核数×N"硬扛。上下文切换成本高。
- **brpc 的 bthread 模型**：M 个用户态协程跑在 **N 个 pthread worker** 上（**M:N 调度**，本项目 `server.cpp` 里 `num_threads=4`，即 4 个 worker 线程）。bthread 切换是**用户态**的，微秒级，远快于内核线程切换。

```text
经典线程池                           brpc bthread
┌─────────────┐                    ┌───────────────────────┐
│ OS 线程 1     │ ← 请求A(阻塞→卡死)     │ pthread worker 1      │
│ OS 线程 2     │ ← 请求B(阻塞→卡死)     │   ↕ 调度 bthread A / B / C
│ ...(池满,新请求排队)                  │ pthread worker 2      │
└─────────────┘                    │ ... (4 个 worker, 跑成千上万个 bthread)
                                   └───────────────────────┘
```

**关键差异在于"阻塞谁"**：
- bthread 阻塞在 **`bthread::Mutex` / `bthread::ConditionVariable` / `bthread_usleep`** 等 **bthread 原语**上 → 只挂起**当前 bthread**，让出底层 pthread worker 去跑别的 bthread → worker 永远不会被请求占满。
- 反之，如果 bthread 里用了 **`std::mutex` / `std::condition_variable` / 系统调用级阻塞** → 底层 **pthread worker 被整个挂起**。4 个 worker 被 4 个阻塞请求占满后，整个服务"假死"——但进程还活着（因为不是死锁，只是没有 worker 能干活了）。

### 2.1.2 本项目踩过的真实坑（面试讲这个=亲写实锤）

**M7 性能优化**中，写路径"提交 Raft 日志后要等 `on_apply` 结果"，最初实现用的是 **`std::condition_variable`**：

```text
高并发写 →
  N 个 bthread 处理写请求 → 每个都 std::cv.wait() 等 apply 结果
  → 每个都占住一个 pthread worker → num_threads=4 的 worker 全被占
  → 新的请求 / Raft 心跳 / on_apply 线程都没 worker 跑 → 节点假死/崩溃
```

**修复**：换成 **`bthread::Mutex` + `bthread::ConditionVariable`**（见 `src/raft/state_machine.h` 的 `WaitState`）。挂起的是 bthread，4 个 worker 得以继续跑 on_apply 与网络回调。
**权衡记录**：期间还试过 bthread 轮询 `bthread_usleep`（慢）、`bthread::butex`（在 pthread 上下文挂死无法唤醒，弃用）——最后 `bthread cv` 是正确且足够快的方案。这些试错本身就是最好的面试素材（详见 06 章）。

> 面试一句话："在 brpc 里写服务，**凡是可能等待的同步，都必须用 bthread 原语**；用 std 同步会把底层的 4 个 pthread worker 全部挂死，高并发下表现为假死。我们压测时真实复现并修复过这个 bug。"

### 2.1.3 brpc 为什么快（还能讲的其他点，点到即可）

- **单端口多协议**：brpc 从**同一个端口**按首字节识别协议（HTTP / gRPC(h2) / baidu_std / Raft 内部 RPC…）。这是本项目"一个端口同时承载 gRPC+HTTP+Raft 通信"的技术基础（03 章详述）。
- **内存管理**：连接级内存池、对象复用，降低高频分配。
- **并发模型**：bthread work-stealing 调度、无锁/自旋锁在热点路径。
- 不必背全，讲"**M:N + 用户态切换 + 单端口多协议**"三点已足够有深度。

### 2.1.4 长轮询为什么也要 bthread（Watch 的落点）

HTTP Watch 长轮询会"挂起等待事件最多 30s"。如果每挂一个长轮询占一个 OS 线程，几千个订阅就把线程打爆；但挂的是 **bthread**，一个 worker 可以同时背成千上万个挂起的 Watch。**WatchHub 的等待循环就挂在 bthread 条件变量上**（`src/watch/watch_hub.cpp`）。这也是"选 brpc 栈"的一个隐性收益——长轮询在这种模型下几乎零成本。

---

## 2.2 LevelDB / LSM：够用且恰好匹配的存储引擎

> 深读请翻 `../LevelDB.md`（LevelDB Handbook 整理版）。本节只讲**本项目用得着、面试爱考**的部分，并补上该文档缺失的"项目侧论证"。

### 2.2.1 LevelDB 是什么

单机嵌入式 KV 存储，**LSM-Tree**（Log-Structured Merge-Tree）结构：**写只追加、不原地更新**，靠后台合并（Compaction）维持读性能。相比 B+Tree（如 InnoDB），LSM **写放大换写吞吐**——顺序写远快于随机写。

### 2.2.2 组件与一次写、一次读（能画出来）

```text
写: 顺序追加 WAL(log) → 写 memtable(内存跳表) → 满后转 immutable → 后台刷成
    L0 sstable → 逐层 Compaction 下沉(后台), 同时做多路归并去重
读: memtable → immutable → L0 → L1... 逐层查，每层内部二分/布隆过滤
```

核心对象：
- **WAL（log）**：崩溃恢复的保证，**写先落 WAL**。
- **memtable / immutable**：内存中的有序结构。
- **SSTable**：磁盘上的有序不可变文件，分 L0~Ln，层级归并。
- **Manifest / CURRENT**：记录各 SSTable 属于哪层、文件清单；恢复时据此重建。
- **布隆过滤器（可选）**：快速判断 key 不在某层。

**两个面试高频词**：
- **写放大 / 读放大 / 空间放大**：写放大 = 一次写入最终被反复归并写入的量（LSM 的代价）；读放大 = 一次读要查多层；空间放大 = 旧版本数据占的空间。项目里 Compaction 就是调这三者的旋钮。
- **Compaction**：后台把小的/重叠的 SSTable 归并成更大的有序文件并丢弃已删除/过期数据。

### 2.2.3 本项目最看重的三个 LevelDB 特性

1. **WriteBatch 原子性**：多个写打包进一个 `WriteBatch`，**要么全部生效、要么全不生效**。本项目把"业务数据写 + revision 递增 + cfg 索引"放进**同一个 WriteBatch 原子提交**（04/05 章详述）——这是状态一致的关键。
2. **Snapshot（快照）**：`db->GetSnapshot()` 钉住一个一致的读视图，之后的读都基于该视图，不被并发写/Compaction 干扰。本项目用它做**一致性快照导出**（导出 revision + k/ + cfg/ 三部分，保证跨节点一致）。
3. **有序迭代器**：`Iterator` 天然按键字典序遍历。本项目把 revision 编成**保序的 16 进制 key**，扫 `v/` 前缀即按 revision 升序遍历历史——MVCC 历史查询和 Watch 历史重放都靠它（05 章）。

### 2.2.4 为什么"串行单写 + 无事务"对本项目不是缺点（关键论证）

LevelDB 本身只支持**单写者**（写线程要拿同一把锁），且**没有跨 key 事务**（除了 WriteBatch 的原子性）。这在一般业务里是短板，但**对本项目恰好是优点**：

> **Raft 已经把"写序"变成全局唯一的串行序列（日志）**，`on_apply` 本来就是串行执行。所以：
> - LevelDB 的单写者约束 → 与 Raft 串行 apply **天然匹配**，不存在并发写冲突；
> - "没有事务" → 一致性**由 Raft 日志序保证**，不需要数据库事务；
> - 需要"多条写原子生效"的地方（一次 Put 要同时写主索引/历史/cfg/revision）→ 用 **WriteBatch** 补齐原子性即可。

**一句话**："我不需要 LevelDB 提供事务或并发写——因为我上面有 Raft 已经把并发收敛成串行日志流，LevelDB 只做'按序落盘'这一件事；多对象原子性我用 WriteBatch 解决。**选型的关键不是引擎多强，而是引擎的约束恰好贴合上层的一致性模型。**"

（这也解释为什么选 LevelDB 而不是 MySQL/RocksDB：配置中心的读写模型 + 单机内嵌，LevelDB 最简。要更大数据量/更复杂查询才会升级到 RocksDB。）

### 2.2.5 fsync：Raft 持久性的"价签"

- WAL 写是**追加到 OS page cache** 就返回（快），**fsync** 才真正落盘。
- 本项目 `--raft_sync` 控制 braft 日志落盘是否 fsync。压测显示：**fsync 开/关，写吞吐相差约 8–10 倍**（3 节点写 ×64：10.0k vs 28.2k QPS）。这就是"崩溃不丢已提交日志"的代价。
- 面试解释：Raft 说"多数派持久化后提交"——**不 fsync 的话"持久化"是假的**（OS 崩溃/断电会丢），所以生产必须开；压测关掉只是为了看"代码本身能到多快"的上限。默认部署 fsync 开。

---

## 2.3 MVCC 与逻辑时钟：为什么配置中心需要"版本"

### 2.3.1 MVCC 是什么

**MVCC（Multi-Version Concurrency Control）多版本并发控制**：数据更新**不覆盖旧值**，而是**追加一个新版本**，读写各版本共存、按需读取。

- 数据库（PostgreSQL/MySQL InnoDB）用它做**无锁读**（读老快照不被写阻塞）。
- 本项目 / etcd 用它做**历史版本 + 审计 + 回滚 + Watch 事件源**。

### 2.3.2 revision（全局） vs version（单 key）——本项目/etcd 的双编号

etcd 的启示（本项目基本复刻了 etcd 的模型）：

| 编号 | 作用域 | 语义 | 本项目用途 |
|---|---|---|---|
| **revision** | **全局**（整个集群） | 每次任意 key 写都 +1 的逻辑时钟；全局单调、永不复用 | MVCC 分版、Watch 断点续传锚点、CAS 版本比对、Compaction 水位 |
| **version** | **单 key** | 该 key 被修改的次数（从 1 开始） | 配置发布的"第几版"，Rollback 目标参数 |

**关键差异示例**：写 10 个不同 key，`revision` 走 10 格，每个 key 的 `version` 各自只 +1。**revision 是"全局统一编年史"，version 是"个人账本"**。

> 面试一句话："我用一个全局单调的 revision 当逻辑时钟，所有 key 共用；它同时充当了 MVCC 的版本号、Watch 的续传断点、和'历史有没有被回收'的判断依据。etcd 也是这个模型，我照着它的语义在 LevelDB 上自己实现了一套。"

### 2.3.3 为什么需要 revision（三个理由，都能对上代码）

1. **历史/回滚**：Rollback 需要"读第 N 版的值"——靠 `cfg/{key}/{ver}` 的 version 索引。
2. **Watch 有序不丢**：Watch 需要"我上次看到哪、接下来从哪开始"的**全局有序锚点** → revision。
3. **一致快照/恢复**：快照导出必须带一个 revision，让新节点知道"我这份数据到哪个时间点"，之后按日志续传。

### 2.3.4 MVCC vs 覆盖写的代价与回收

- **代价**：每次写都追加 → 历史无限增长 → 需要 **Compaction 回收**（每 key 保留最近 N 版，默认 10，见 `server.cpp`），并把已回收的最高 revision 记为 `compact_rev` 水位线。
- **边界语义**：被回收的历史（from_revision < compact_rev）不再可读 → Watch 返回 `COMPACTED`、GetHistory 返回 `VERSION_NOT_FOUND`，客户端从新锚点重建（05 章详述）。

### 2.3.5 "逻辑时钟 vs 物理时间戳"

用 revision 而非 `wall-clock 时间戳` 的好处：**单调、无时钟漂移、天然全局有序**（Leader 串行分配），且**与日志序一致**。物理时钟在多机间会回拨/漂移，不能当版本号。这点被问"为什么用自增号不用时间戳"时要答得上来。

---

## 【本章自查】

1. 画出 bthread M:N 模型；解释"为什么在 brpc 里用 std::cv 会假死"。
2. 讲一遍你踩过的 worker 假死坑：现象、根因、备选方案、最终修复。
3. 画 LevelDB 读写路径；说出 WriteBatch / Snapshot / 有序迭代器三个特性各被本项目用在哪。
4. 论证"为什么 LevelDB 单写者+无事务对本项目不是缺点"。
5. 解释 fsync 开/关为什么差 8~10 倍；Raft 的"持久化"和生产环境为何必须 fsync。
6. revision vs version 的区别；revision 的三个用途；为什么不用物理时间戳。

---
**下一篇**：[03-协议与抽象层.md](03-协议与抽象层.md)——代码结构是怎么组织起来的。
