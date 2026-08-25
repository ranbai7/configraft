# Raft 原理 + braft 工程实现

> **阅读建议**:先通读一遍建立骨架;再对照 raft.github.io 动图过第 4、5 节;读 braft 源码时回头查第 11 节。
>
> **标记约定**:
>
> - ⭐ = 面试必会
>- 🔍 = 需要对照源码/文档深挖
> - 🎬 = 建议用交互演示动手验证

## 目录

1. 为什么需要这份笔记
2. 背景:共识与状态机复制
3. 核心概念与术语
4. 领导者选举 ⭐
5. 日志复制 ⭐
6. 安全性:为什么这套机制是对的 ⭐
7. 日志不一致的处理 ⭐
8. 线性一致读 ReadIndex ⭐
9. 快照与落后节点追赶 ⭐
10. 成员变更
11. braft:从算法到工程 ⭐
12. 面试问答速查表 ⭐
13. 学习路线与资源

附录:如何写出这样的笔记(方法论)

------

## 1. 为什么需要这份笔记

面试官最怕"只会调库"的候选人。`node->apply()` 一行能跑通 demo,但面试官追问三句就会露馅:

- 这一行背后发生了哪几步?哪些是异步的?
- 为什么新 leader 必须先写一条 no-op 日志?
- 为什么 commit 不能只看"复制到了多数派"?

这份笔记就是在 **Raft 算法原理** 和 **braft 源码实现** 之间架一座桥:每个机制先讲清原理,再落到 braft 的哪个类、哪个函数。它不是读书摘要,而是**面试答辩稿**。

## 2. 背景:共识与状态机复制

**问题**:分布式系统中,多副本各自执行,如何保证它们最终状态一致,且已确认的写不丢失?

**状态机复制(State Machine Replication, SMR)**:解决上述问题的通用框架——只要每个副本以**相同顺序**执行**相同命令**,最终状态就一致。于是问题从"如何让状态一致"化简为"如何让所有副本按相同顺序收到相同日志"——这正是共识算法负责的部分。

```
客户端 ──(command)──> 共识模块(日志) ──apply──> 状态机(应用层)
                          │ 复制
                    其他副本的共识模块 ──apply──> 各自状态机
```

**Raft 的定位**:共识算法的一种,核心卖点是**可理解性**。它把问题拆成三个相对独立的子问题:

1. **领导者选举**——谁的日志被当作"事实";
2. **日志复制**——把命令复制到所有节点;
3. **安全性**——保证前两条不会产生错误结果。

**braft 的定位**:百度开源的 C++ 版 Raft,基于 brpc/bthread。它把"共识引擎"做成库,业务方只需实现 `StateMachine`。理解 braft 的关键一句话:**算法在库里,状态在你写的 StateMachine 里,两者通过接口解耦。**

## 3. 核心概念术语和参数

| 术语                   | 含义                                      | 一句话直觉                      |
| ---------------------- | ----------------------------------------- | ------------------------------- |
| Term(任期)             | 单调递增的整数,每开始一次选举 +1          | 全局"代际",用来区分新旧 leader  |
| Leader                 | 唯一能接受写请求、主导日志复制的节点      | 日志的"唯一作者"                |
| Follower               | 被动接收日志、回应投票                    | 大多数时间都是它                |
| Candidate              | 选举中的自荐者                            | 超时后短暂出现的角色            |
| Log Entry              | 一条日志记录 (index, term, command)       | 命令 + 编号                     |
| commitIndex            | 已确认可以执行到哪条日志                  | "落定"水位线                    |
| lastApplied            | 状态机已应用到哪条                        | "执行"水位线                    |
| matchIndex / nextIndex | leader 记录的每个 follower 的日志同步进度 | 对账用的两个指针                |
| Quorum(多数派)         | 超过半数的节点集合                        | 决策门槛,任意两个多数派必有交集 |

**两条核心不变量**:

- 同一 index 的日志只属于一个 term(日志只有追加和截断,没有中间插队);
- 一旦某条日志 commit,它和它之前的所有日志最终都会被执行(commit 单调性)。



**状态**：

所有服务器上的持久性状态 (在响应 RPC 请求之前，已经更新到了稳定的存储设备)

| 参数        | 解释                                                         |
| ----------- | ------------------------------------------------------------ |
| currentTerm | 服务器已知最新的任期（在服务器首次启动时初始化为0，单调递增） |
| votedFor    | 当前任期内收到选票的 candidateId，如果没有投给任何候选人 则为空 |
| log[]       | 日志条目；每个条目包含了用于状态机的命令，以及领导人接收到该条目时的任期（初始索引为1） |

所有服务器上的易失性状态

| 参数        | 解释                                                         |
| ----------- | ------------------------------------------------------------ |
| commitIndex | 已知已提交的最高的日志条目的索引（初始值为0，单调递增）      |
| lastApplied | 已经被应用到状态机的最高的日志条目的索引（初始值为0，单调递增） |

领导人（服务器）上的易失性状态 (选举后已经重新初始化)

| 参数         | 解释                                                         |
| ------------ | ------------------------------------------------------------ |
| nextIndex[]  | 对于每一台服务器，发送到该服务器的下一个日志条目的索引（初始值为领导人最后的日志条目的索引+1） |
| matchIndex[] | 对于每一台服务器，已知的已经复制到该服务器的最高日志条目的索引（初始值为0，单调递增） |

**追加条目（AppendEntries）RPC**：

由领导人调用，用于日志条目的复制，同时也被当做心跳使用

| 参数         | 解释                                                         |
| ------------ | ------------------------------------------------------------ |
| term         | 领导人的任期                                                 |
| leaderId     | 领导人 ID 因此跟随者可以对客户端进行重定向（译者注：跟随者根据领导人 ID 把客户端的请求重定向到领导人，比如有时客户端把请求发给了跟随者而不是领导人） |
| prevLogIndex | 紧邻新日志条目之前的那个日志条目的索引                       |
| prevLogTerm  | 紧邻新日志条目之前的那个日志条目的任期                       |
| entries[]    | 需要被保存的日志条目（被当做心跳使用时，则日志条目内容为空；为了提高效率可能一次性发送多个） |
| leaderCommit | 领导人的已知已提交的最高的日志条目的索引                     |

| 返回值  | 解释                                                         |
| ------- | ------------------------------------------------------------ |
| term    | 当前任期，对于领导人而言 它会更新自己的任期                  |
| success | 如果跟随者所含有的条目和 prevLogIndex 以及 prevLogTerm 匹配上了，则为 true |

**请求投票（RequestVote）RPC**：

由候选人负责调用用来征集选票（5.2 节）

| 参数         | 解释                         |
| ------------ | ---------------------------- |
| term         | 候选人的任期号               |
| candidateId  | 请求选票的候选人的 ID        |
| lastLogIndex | 候选人的最后日志条目的索引值 |
| lastLogTerm  | 候选人最后日志条目的任期号   |

| 返回值      | 解释                                       |
| ----------- | ------------------------------------------ |
| term        | 当前任期号，以便于候选人去更新自己的任期号 |
| voteGranted | 候选人赢得了此张选票时为真                 |

接收者实现：

1. 如果`term < currentTerm`返回 false （5.2 节）
2. 如果 votedFor 为空或者为 candidateId，并且候选人的日志至少和自己一样新，那么就投票给他（5.2 节，5.4 节）





## 4. 领导者选举 ⭐

### 流程(请对照 raft.github.io 动图亲手走一遍 )

1. Follower 在 `election_timeout` 内没收到 leader 心跳 → 认为 leader 失联。
2. 自增 term,转为 Candidate,先投自己一票。
3. 并行向所有节点发 `RequestVote` RPC。
4. 收到**多数派(quorum)**投票 → 成为 leader,立即广播心跳(空 AppendEntries)确立权威。
5. 若超时仍未获胜 → 进入下一轮,term 再 +1,重试。
6. 若收到任期 **>= 自己 term** 的 leader 心跳 → 退回 Follower。

### 关键机制与原因

- **随机化超时**(论文 150–300ms):避免多个节点同时超时导致"**选票分裂(split vote)**"谁也选不出来。braft 用 `election_timeout_ms` 配置,实际超时在配置值上随机抖动。
- **同一任期最多投一票**:每个节点一个 term 只投一次,投出后落盘记录 (term, votedFor)。这保证了 **选举安全(Election Safety):同一任期最多只有一个 leader**。
- **任期传递**:所有 RPC 都携带 term;收到更高 term 立即退回 Follower。term 是全局唯一的"新旧"判定标准。
- **pre_vote(预投票)** :braft 支持(可配置)。网络分区里的节点 term 会不断自增,分区恢复后会打断正常 leader。pre_vote 让节点先问"如果我发起选举,你们支持吗",拿到多数派**预支持**后才真正开始选举,避免 term 爆炸。**面试高频:pre_vote 解决什么问题?**

### 选举限制(Log Up-to-Date)

Candidate 要当选,还要求自己的日志**不比投票者旧**:先比 `lastLogTerm`,再比 `lastLogIndex`。

**为什么**:否则一个日志严重缺失的节点可能当选,把已 commit 的日志覆盖掉,破坏安全性。这条限制属于"安全性"的一部分,面试常和 commit 规则连在一起问。





## 5. 日志复制 ⭐

### 正常流程

```
客户端 ──> Leader.node->apply(cmd)
  1. Leader 把命令追加为一条本地日志 entry(index, term, cmd)
  2. 并行向所有 follower 发 AppendEntries RPC
  3. 多数派落盘成功 → Leader 判定该 entry 已 commit
  4. Leader 推进 commitIndex,并在后续 AppendEntries 里捎带给所有节点
  5. 各节点把 <= commitIndex 的日志逐条 apply 到状态机
  6. Leader 回复客户端成功(注意:只有 commit 后才算成功!)
```

### 对账:matchIndex / nextIndex

- `nextIndex[f]`:leader 认为 follower `f` 下一条应该收到的日志 index。
- `matchIndex[f]`:leader 已确认 follower `f` 复制成功的最大 index。
- 新 leader 上任时,把每个 follower 的 `nextIndex` 初始化为自己的 `lastLogIndex + 1`;对账失败就回退(见第 7 节)。

### 批量与流水线 

braft 的 Replicator 支持**批量发送 + 流水线(pipelining)**:不必等一条 ack 再发下一条,多个 AppendEntries 可同时在途,吞吐大幅提升。源码在 `src/braft/replicator.cpp`,理解机制即可,不必逐行。

### no-op 日志 ⭐⭐(必考)

新 leader 上任后,第一件事是**立即在日志里追加一条空的 no-op entry**,而不是直接处理业务请求。

**为什么需要**:

1. **解决"旧任期日志无法直接提交"的问题**。Raft 规定:leader 只能靠**当前任期**的日志被复制到多数派来推进 commit(见第 6 节)。新 leader 的日志里可能只有旧任期日志,如果不写 no-op,它永远无法提交任何日志,系统停摆。
2. **no-op 一旦被多数派复制并 commit,它之前的旧日志也一并被间接提交**(借助 Log Matching 性质)。
3. **顺带"对外宣布"领导权**:no-op commit 了,才说明自己真正拿到了多数派认可,此时才对外提供写服务。

**反例想清楚**:一个节点带着一条"已复制到多数派"的旧任期日志当选,若允许它直接提交该旧日志,未来这条日志可能被新 leader 覆盖,导致已返回成功的写丢失。no-op 把"能不能提交"推迟到"当前任期日志落定"之后。

## 6. 安全性:为什么这套机制是对的 ⭐

Raft 论文用 5 条不变量保证安全性。**面试考的是"为什么",不要背条文**。

1. **Election Safety**:每个 term 至多一个 leader → 由"一节点一票 + 多数派"保证。
2. **Leader Append-Only**:leader 从不删除或覆盖自己的日志,只追加 → 已 commit 的日志在 leader 上永远保留。
3. **Log Matching**:如果两条日志在相同 index、相同 term,则它们之前的所有日志都相同 → 由 AppendEntries 的 `prevLogTerm` 校验保证。
4. **Leader Completeness**:已 commit 的日志一定出现在之后所有 leader 的日志里 → 由"日志最新者才能当选"(选举限制)保证。
5. **State Machine Safety**:若节点把 index i 的日志 apply 到状态机,则其他节点不会在 i 上 apply 一条不同命令 → 由 3 + 4 推出。

| 特性             | 解释                                                         |
| ---------------- | ------------------------------------------------------------ |
| 选举安全特性     | 对于一个给定的任期号，最多只会有一个领导人被选举出来         |
| 领导人只附加原则 | 领导人绝对不会删除或者覆盖自己的日志，只会增加               |
| 日志匹配原则     | 如果两个日志在某一相同索引位置日志条目的任期号相同，那么我们就认为这两个日志从头到该索引位置之间的内容完全一致 |
| 领导人完整特性   | 如果某个日志条目在某个任期号中已经被提交，那么这个条目必然出现在更大任期号的所有领导人中 |
| 状态机安全特性   | 如果某一服务器已将给定索引位置的日志条目应用至其状态机中，则其他任何服务器在该索引位置不会应用不同的日志条目 |

### Commit 规则(重点中的重点)

- 一个 entry 能判定为 committed,当且仅当:**被多数派复制** 且 **是 leader 当前任期的日志**。
- 旧任期日志不能靠"复制数"直接提交;它只能随着**当前任期某条日志** commit 而被"连带提交"。

### 为什么旧日志不能按复制数直接提交?

经典反例(论文 Figure 8):某旧任期日志曾被复制到多数派,但随后的 leader 变更可能让它被覆盖。若提前提交它并回复客户端成功,这条"成功"的写就丢了。no-op 正是为了打破这个死局。

## 7. 日志不一致的处理 ⭐

**不一致从哪来**:网络分区、旧 leader 的未 commit 日志等,导致 follower 日志可能比 leader **多出一些日志**(未 commit 的脏日志)、**少一些日志**,或**与 leader 冲突**。

**Raft 的哲学:leader 是权威,冲突以 leader 为准。**

处理流程(论文版):

1. Leader 从 `nextIndex[f]` 处发 AppendEntries,带上 `prevLogIndex / prevLogTerm`。
2. Follower 校验自己 `prevLogIndex` 处日志的 term 是否匹配:
   - 不匹配 → 拒绝并返回冲突信息,leader 回退 `nextIndex[f]` 再试;
   - 匹配 → 接受,并截断该点之后与 leader 不一致的日志,覆盖为 leader 的日志。
3. 反复回退直到找到双方一致的**公共前缀**,此后跟随 leader。

**关键性质**:由于 Log Matching,冲突点之后的日志全部删除重建即可,不影响已 commit 的部分(已 commit 的日志必然在公共前缀内)。

**braft 实现 🔍**:同样基于"AppendEntries 拒绝 + nextIndex 回退",但拒绝时会返回更精确的冲突 index/term,一次跳过一段,避免逐条试错。读 `src/braft/node.cpp` 处理 AppendEntries 响应的分支即可。

## 8. 线性一致读 ReadIndex ⭐

**问题**:leader 直接读本地状态机,可能读到**过期数据**——因为读的这一刻它可能已经被分区,不再是法定 leader。要求读操作也要满足**线性一致(Linearizable)**,即读到的必须包含此前所有已确认的写。

**为什么不直接走日志复制?** 写走一遍日志、读也走一遍日志,延迟和吞吐都不可接受。Raft 论文 §6.4 提出 ReadIndex。

### ReadIndex 四步(必背)

1. Leader 记录当前 `commitIndex` 作为 `readIndex`(此刻"已确认到哪")。
2. Leader 通过一次心跳/广播**确认自己仍是 leader**(多数派响应 = 领导权还在)。⚠️ 若期间出现更高 term,本次读中止。
3. 等待状态机 `lastApplied >= readIndex`(确保它已执行到读点之前的所有写)。
4. 在本地状态机上执行读,返回结果。

**为什么正确**:第 2 步保证读时刻 leader 身份有效;第 1 + 3 步保证读到的状态包含 `readIndex` 之前的全部写。

**braft 实现 🔍**:`Node::read_index(Closure* done, int64_t* index)`,回调在状态机 apply 到该 index 后被触发,业务方在回调里执行读。

**工程进阶:租约读(ReadOnlyLeaseBased)** 🔍:若在 `election_timeout` 内没收到更高 term 的请求,可以认为领导权仍有"租约",跳过第 2 步的心跳,读延迟更低。**面试进阶题:租约读为什么安全?风险在哪?(答案:依赖时钟,时钟偏移过大时租约可能失真)**

## 9. 快照与落后节点追赶 ⭐

**问题**:日志无限增长,重启要重放全部历史,启动慢、占磁盘。解决:**定期把状态机状态固化(snapshot),丢弃此前的日志**。

**快照内容**:状态机在 index i 处的完整状态 + 元数据(最后包含的日志 index/term)。

**正常流程**:状态机把状态写入文件 → 保存元数据 → 通知共识模块可以裁剪 i 之前的日志。braft 中由 `StateMachine::on_snapshot_save()` 触发,`snapshot_interval_s` 控制频率。

**落后节点追赶**:

- 若 leader 发现某 follower 需要的日志**已被裁剪**(`nextIndex` 已小于最早日志)→ 改用 **InstallSnapshot** RPC。
- 流程:分块(chunk)发送 → follower 逐块落盘 → 全部收到后把快照加载进状态机(`on_snapshot_load`)→ 丢弃旧日志,从快照点继续复制。

**面试点**:为什么落后节点能用快照追赶上?——快照包含到 index i 为止的**全部状态**,天然覆盖它缺失的日志。注意快照只能包含**已 commit** 的状态(快照推进同样遵守 commit 语义)。

## 10. 成员变更

**难点**:加/删节点改变的是"多数派"集合。若直接切换配置,新老配置的多数派可能**不相交** → 同时出现两个 leader(破坏选举安全)。

**Raft 论文方案**:

- **单节点变更**:一次只加/减一个节点。此时新老配置的多数派必然相交,无需额外机制,工程上最常用。
- **Joint Consensus(联合共识)**:一次变更多个节点时,先进入新老配置**共治**阶段(决策需新老两个多数派都通过),再切换到纯新配置。

**braft 实现 🔍**:`Node::add_peer / remove_peer / change_peers`。braft 通常让新节点先以 **learner(学习者)** 身份加入——只复制日志、**不参与投票**,追赶到接近最新后再提升为正式成员,避免空转节点拖慢集群。相关代码在 `src/braft/configuration.cpp`。

## 11. braft:从算法到工程 ⭐

### 11.1 总体架构(先在心里建这张图)

```
业务代码(你的应用)
   │ 实现 StateMachine 接口
   ▼
braft::StateMachine(你写的类)
   ▲ 回调:on_apply / on_snapshot_save / on_leader_start / on_leader_stop ...
   │
braft::Node(共识引擎,node.cpp)
   │   日志(LogManager) · 元数据(RaftMeta) · 快照(Snapshot)
   │   复制(Replicator) · 选举 · 投票(BallotBox)
   ▼
brpc / bthread(网络与线程模型)
```

### 11.2 核心接口:StateMachine(你写的部分)

```cpp
class MyStateMachine : public braft::StateMachine {
    void on_apply(braft::Iterator& iter) override {
        // 逐条消费已 commit 的日志
        for (; iter.valid(); iter.next()) {
            apply_to_my_state(iter.data());          // 执行命令
            if (iter.done()) iter.done()->Run();     // 通知客户端"已成功"
        }
    }
    void on_leader_start(int64_t term) override { /* 成为 leader:开始接受写 */ }
    void on_leader_stop() override          { /* 不再是 leader:拒绝写 */ }
    void on_snapshot_save(braft::SnapshotWriter* w, braft::Closure* done) override {}
    void on_snapshot_load(braft::SnapshotReader* r) override {}
};
```

**注意**:状态机必须**幂等、可重复执行**——同一日志可能被 apply 多次(例如 leader 变更后的重放)。`on_apply` 里不要做不可重复的副作用。

### 11.3 Node:生命线与 apply 流程

**生命周期**:`NodeOptions`(日志/元数据/快照的存储路径、`election_timeout_ms`、`initial_conf` 等)→ `node.init(opts)` → 运行 → `node.shutdown(...)` / `node.join()`。

**apply 流程(把第 5 节映射到代码)**:

```cpp
braft::Task task;
task.data = &my_command;        // 要复制的数据
task.done = my_done_callback;   // commit + apply 后回调(在 on_apply 里触发)
node->apply(task);              // 注意:异步!
```

背后发生的链路:`apply` 把数据封装成 log entry → 写入日志 → Replicator 广播 → 多数派落盘 → BallotBox 判定 commit → 触发 `on_apply` → 你的 done 被 `Run()`。**能否成功是异步的,不能从 `apply` 的返回值同步拿到**。

### 11.4 客户端视角:RouteTable 与重定向

- 客户端不知道谁是 leader,用 `RouteTable` 维护 group → leader 的映射(通常用全局 `braft::rtb`)。
- 请求打到非 leader 节点时,该节点返回重定向信息;客户端调用 `RouteTable::refresh_leader()` 重新拉取最新 leader,再重试。
- **这就是"客户端如何跟随 leader 变更"的答案**——面试常问。

### 11.5 源码关键路径导读(只读关键路径,不逐行)

| 主题          | 文件                          | 看什么                                        |
| ------------- | ----------------------------- | --------------------------------------------- |
| Node 生命周期 | `src/braft/node.cpp`          | `init / apply / commit`、角色状态机转换       |
| 选举          | `node.cpp` + `election.cpp`   | `election_timeout`、pre_vote、投票规则        |
| 日志复制      | `src/braft/replicator.cpp`    | AppendEntries、pipelining、match/next 推进    |
| ReadIndex     | `node.cpp::read_index`        | ReadIndex 四步在代码里的落点                  |
| 快照          | `src/braft/snapshot.cpp`      | `on_snapshot_save/load`、InstallSnapshot 分块 |
| 成员变更      | `src/braft/configuration.cpp` | add/remove_peer、learner                      |

**读源码方法**:先跑通 `example/counter` 的 server 和 client,打断点看一次 apply 的全链路;再回到上表逐段跟。每一段只回答一个问题:**"这段代码在实现第几节算法?"**

### 11.6 工程问题:不止算法

- **坏节点/网络分区**:靠超时和多数派判定,坏节点不阻塞(只需多数派,不要求全部)。
- **存储**:日志、元数据、快照可分别配置存储介质(如 `local://` 路径),可放到不同磁盘。
- **优雅关闭**:先 `shutdown` 停止复制,再 `join` 等线程退出——顺序反了可能丢数据或卡死。

## 12. 面试问答速查表 ⭐

> 每题先自己讲一遍,再对照。重点在**"为什么"**。

**Q1. Raft 如何保证同一任期只有一个 leader?**
同一 term 每个节点最多投一票;当选需要多数派投票;任意两个多数派必有交集 → 不可能出现两个 leader 都拿到多数派。

**Q2. 为什么新 leader 要先写 no-op?**
Raft 规定只有"当前任期的日志复制到多数派"才能推进 commit。新 leader 日志里可能没有当前任期日志,写 no-op 让 commit 能推进;no-op 提交后,其之前的旧日志被连带提交(Log Matching);同时标志领导权真正生效,之后才对外提供写服务。

**Q3. commit 规则是什么?为什么旧任期日志不能按复制数直接提交?**
见 §6:需要"当前任期 + 多数派"两个条件。旧日志可能已复制到多数派但仍被后续 leader 覆盖(Figure 8 反例),提前提交会丢失已确认的写。

**Q4. 日志不一致如何处理?**
Leader 是权威。AppendEntries 带 `prevLogTerm` 校验;不匹配则拒绝并回退 `nextIndex`,直到找到公共前缀;然后截断 follower 冲突段、覆盖为 leader 的日志。braft 用冲突 index/term 加速回退。

**Q5. ReadIndex 的步骤?**
① 记录 `commitIndex` 为 readIndex;② 心跳/广播确认仍是 leader;③ 等状态机 apply 到 readIndex;④ 本地读。追问:租约读如何优化(跳过第②步)、时钟偏移的风险。

**Q6. 落后节点如何追赶?**
日志还在 → AppendEntries 补齐;日志已被裁剪 → InstallSnapshot 分块传输,加载进状态机后继续。

**Q7. braft 里 StateMachine 和 Node 的分工?**
Node 负责共识(日志复制、选举、commit 判定);StateMachine 由业务实现,负责"如何执行已提交日志"以及快照读写。`node->apply` 提交,`on_apply` 执行。

**Q8. braft 客户端如何找到 leader?**
RouteTable 维护 group → leader 映射;请求打到非 leader 会被重定向;`refresh_leader()` 更新缓存后重试。

**Q9. pre_vote 解决什么问题?**
避免被分区节点 term 持续自增、恢复后干扰正常 leader。先预投票确认自己可能当选,再正式发起选举,term 不会失控。

**Q10. 成员变更为什么难?**
变更会改变"多数派集合",直接切换可能产生两个不相交的多数派 → 双 leader。用单节点变更、joint consensus 或 learner 方式解决。

## 13. 学习路线与资源

按计划走,顺序不能乱:

1. **论文**:《In Search of an Understandable Consensus Algorithm》(Ongaro & Ousterhout),建议中文翻译版 + 英文对照读。边读边在纸上画 term / 日志结构。
2. **交互演示**:raft.github.io 动图。亲手模拟选举、日志复制,直到能**预判下一步**再点下一步。
3. **braft 官方文档**(baidu.github.io/braft):overview → server → client → cli → replication。
4. **示例精读**:`example/counter/server.cpp` 与 `client.cpp`,弄清 StateMachine、`node->apply`、RouteTable 三者的配合。
5. **源码精读**:配合第 11.5 节表格,只读关键路径,不逐行。

**自检标准**:能不看资料,把 §12 的 10 题口头讲清楚,并能画出 §11.1 架构图 → 这一轮过关。

------

## 附录:如何写出这样的笔记(方法论)

> 这一节是"教你写笔记的方法",提交到 repo 前可整体删除。

**1. 先想清楚笔记的用途。** 你的笔记不是读书摘要,而是"面试答辩稿"。唯一标准:**每个知识点,你能否在 30 秒内用自己的话讲清"是什么 + 为什么"**。写每一节之前先问:这一节我要回答什么问题?

**2. 用"三层结构"组织每个机制。**

- **是什么**:定义或步骤,编号列出;
- **为什么**:正确性论证,一定要写"如果没有会怎样"的反例;
- **工程怎么落地**:braft 里对应哪个类 / 哪个函数 / 哪条调用链。

本文第 5 节 no-op、第 6 节 commit 规则、第 8 节 ReadIndex,都是这个三层结构的例子。

**3. 强迫自己写"为什么"。** 这是最容易偷懒的部分。技巧:每写完一个机制,追问一句"如果去掉这一步会出什么错?",把答案写下来。no-op、选举限制、ReadIndex 第 2 步,全是"去掉就出错"的例子。

**4. 画图。** 文字记不住的,图一遍就懂。本文 §2 复制链路、§11.1 架构图都是这个思路。你的笔记里至少要有 3 张自己画过的图(架构、选举时序、apply 链路)。

**5. 源码笔记只记"关键路径 + 一句话定位"。** 不要抄大段代码。每段源码笔记回答一个问题:"这段代码在实现第几节哪个机制?"braft 读 node.cpp、replicator.cpp、snapshot.cpp 就够,别逐行。

**6. 用面试题自测。** 每学完一节,把该节可能被问的问题写成 Q/A(参考 §12)。这是把"输入"转成"输出"的关键一步,也是"不只会调库"的直接证据。

**7. 持续迭代,不要一次写完。** 读源码有新理解就回填到对应节;面试后暴露的盲区补充进去。用复习标记(如 ✅ 已能默讲、⚠️ 还不熟)标注掌握程度。

**8. 复习纪律。** 每周 15 分钟,只看 §12 问题列表:能答就过,卡壳就回到对应节。这比反复读全文有效得多。

------

*笔记维护记录:2026-08-13 创建。*
