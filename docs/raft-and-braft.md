# Raft 原理 + braft 源码分析 + Configraft 落地

> **阅读建议**:第一遍按目录顺序通读,建立"算法概念 → braft 源码 → 项目落地"三层骨架;第二遍对照 raft.github.io 动图过第 4、5 节;读 braft 源码时回头查第 11 节;读本项目代码时对照第 11.7 节的代码地图。
>
> **标记约定**:
>
> - ⭐ = 核心必会
> - 🔍 = 需要对照源码/文档深挖
> - 🎬 = 建议用交互演示动手验证

## 目录

1. 定位:这份笔记是什么
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
    - 11.7 Configraft:把 braft 落到分布式配置中心 ⭐
12. 学习路线与资源

------

## 1. 定位:这份笔记是什么

这份笔记有**两个定位**,先想清楚再读,才不会读偏:

1. **个人学习笔记(第一定位)**:按照"Raft 算法原理 → braft 源码实现 → 本项目(Configraft)落地"三层递进,把算法讲清、把源码读透、把代码对得上。每一层都围绕同一个问题:**"这段代码在实现算法的哪个机制?"** `node->apply()` 一行能跑通 demo,但背后发生了什么、哪些是异步的、为什么这样设计——读透比跑通重要。
2. **面试时展示学习深度的材料(第二定位)**:不是应试速查表,而是记录"从概念一路挖到源码和工程实现"的思考过程。与其背答案,不如能当面把 `node->apply()` 背后的链路、本项目在 Raft 上的取舍(如 M5 用 Leader Lease 替代 ReadIndex)完整讲出来。

怎么读:先通读概念(§2-§10)建立骨架,再对照 braft 源码(§11)逐层加深,最后用本项目代码(§11.7)验证落地。每节末尾的"**自问自答**"是当节知识的即时巩固,不依赖提前背题。

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

**状态**:

所有服务器上的持久性状态 (在响应 RPC 请求之前,已经更新到了稳定的存储设备)

| 参数        | 解释                                                         |
| ----------- | ------------------------------------------------------------ |
| currentTerm | 服务器已知最新的任期(在服务器首次启动时初始化为0,单调递增)   |
| votedFor    | 当前任期内收到选票的 candidateId,如果没有投给任何候选人则为空|
| log[]       | 日志条目;每个条目包含了用于状态机的命令,以及领导人接收到该条目时的任期(初始索引为1) |

所有服务器上的易失性状态

| 参数        | 解释                                                         |
| ----------- | ------------------------------------------------------------ |
| commitIndex | 已知已提交的最高的日志条目的索引(初始值为0,单调递增)         |
| lastApplied | 已经被应用到状态机的最高的日志条目的索引(初始值为0,单调递增) |

领导人(服务器)上的易失性状态 (选举后已经重新初始化)

| 参数         | 解释                                                         |
| ------------ | ------------------------------------------------------------ |
| nextIndex[]  | 对于每一台服务器,发送到该服务器的下一个日志条目的索引(初始值为领导人最后的日志条目的索引+1) |
| matchIndex[] | 对于每一台服务器,已知的已经复制到该服务器的最高日志条目的索引(初始值为0,单调递增) |

**追加条目(AppendEntries)RPC**:

由领导人调用,用于日志条目的复制,同时也被当做心跳使用

| 参数         | 解释                                                         |
| ------------ | ------------------------------------------------------------ |
| term         | 领导人的任期                                                 |
| leaderId     | 领导人 ID,跟随者据此对客户端进行重定向                       |
| prevLogIndex | 紧邻新日志条目之前的那个日志条目的索引                       |
| prevLogTerm  | 紧邻新日志条目之前的那个日志条目的任期                       |
| entries[]    | 需要被保存的日志条目(被当做心跳使用时,则日志条目内容为空;为了提高效率可能一次性发送多个) |
| leaderCommit | 领导人的已知已提交的最高的日志条目的索引                     |

| 返回值  | 解释                                                         |
| ------- | ------------------------------------------------------------ |
| term    | 当前任期,对于领导人而言它会更新自己的任期                    |
| success | 如果跟随者所含有的条目和 prevLogIndex 以及 prevLogTerm 匹配上了,则为 true |

**请求投票(RequestVote)RPC**:

由候选人负责调用用来征集选票

| 参数         | 解释                         |
| ------------ | ---------------------------- |
| term         | 候选人的任期号               |
| candidateId  | 请求选票的候选人的 ID        |
| lastLogIndex | 候选人的最后日志条目的索引值 |
| lastLogTerm  | 候选人最后日志条目的任期号   |

| 返回值      | 解释                                       |
| ----------- | ------------------------------------------ |
| term        | 当前任期号,以便于候选人去更新自己的任期号 |
| voteGranted | 候选人赢得了此张选票时为真                 |

接收者实现:

1. 如果 `term < currentTerm` 返回 false;
2. 如果 votedFor 为空或者为 candidateId,并且候选人的日志至少和自己一样新,那么就投票给他。

## 4. 领导者选举 ⭐

### 流程(请对照 raft.github.io 动图亲手走一遍)

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
- **pre_vote(预投票)**:braft 支持(可配置)。网络分区里的节点 term 会不断自增,分区恢复后会打断正常 leader。pre_vote 让节点先问"如果我发起选举,你们支持吗",拿到多数派**预支持**后才真正开始选举,避免 term 爆炸。**高频考点:pre_vote 解决什么问题?**

### 选举限制(Log Up-to-Date)

Candidate 要当选,还要求自己的日志**不比投票者旧**:先比 `lastLogTerm`,再比 `lastLogIndex`。

**为什么**:否则一个日志严重缺失的节点可能当选,把已 commit 的日志覆盖掉,破坏安全性。这条限制属于"安全性"的一部分,常和 commit 规则连在一起考。

**自问自答**

- **问:Raft 如何保证同一任期只有一个 leader?**
  答:每个节点同一任期最多投一票(votedFor 落盘),当选需要拿到多数派选票;任意两个多数派必有交集,所以不可能有两个候选人都拿到多数派。此外,收到更高任期的 leader 心跳会退回 Follower——term 是全局的"新旧"判定标准。
- **问:pre_vote 解决什么问题?**
  答:网络分区里的节点 term 会不断自增,分区恢复后会打断正常 leader。pre_vote 让节点先发起一轮"预投票",拿到多数派预支持后再真正自增 term 竞选,避免 term 失控、干扰现任 leader。

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

**自问自答**

- **问:为什么新 leader 必须先写一条 no-op 日志?**
  答:Raft 规定只有"当前任期的日志复制到多数派"才能推进 commit。新 leader 日志里可能没有当前任期日志,不写 no-op 就永远无法提交任何日志,系统停摆。no-op 一旦被多数派复制并 commit,它之前的旧日志被连带提交(Log Matching);同时标志领导权真正生效,之后才对外提供写服务。反例见上:若允许直接提交"已复制到多数派"的旧任期日志,未来可能被新 leader 覆盖,已返回成功的写会丢失。

## 6. 安全性:为什么这套机制是对的 ⭐

Raft 论文用 5 条不变量保证安全性。**重点在"为什么",不要背条文**。

1. **Election Safety**:每个 term 至多一个 leader → 由"一节点一票 + 多数派"保证。
2. **Leader Append-Only**:leader 从不删除或覆盖自己的日志,只追加 → 已 commit 的日志在 leader 上永远保留。
3. **Log Matching**:如果两条日志在相同 index、相同 term,则它们之前的所有日志都相同 → 由 AppendEntries 的 `prevLogTerm` 校验保证。
4. **Leader Completeness**:已 commit 的日志一定出现在之后所有 leader 的日志里 → 由"日志最新者才能当选"(选举限制)保证。
5. **State Machine Safety**:若节点把 index i 的日志 apply 到状态机,则其他节点不会在 i 上 apply 一条不同命令 → 由 3 + 4 推出。

| 特性             | 解释                                                         |
| ---------------- | ------------------------------------------------------------ |
| 选举安全特性     | 对于一个给定的任期号,最多只会有一个领导人被选举出来          |
| 领导人只附加原则 | 领导人绝对不会删除或者覆盖自己的日志,只会增加                |
| 日志匹配原则     | 如果两个日志在某一相同索引位置日志条目的任期号相同,那么我们就认为这两个日志从头到该索引位置之间的内容完全一致 |
| 领导人完整特性   | 如果某个日志条目在某个任期号中已经被提交,那么这个条目必然出现在更大任期号的所有领导人中 |
| 状态机安全特性   | 如果某一服务器已将给定索引位置的日志条目应用至其状态机中,则其他任何服务器在该索引位置不会应用不同的日志条目 |

### Commit 规则(重点中的重点)

- 一个 entry 能判定为 committed,当且仅当:**被多数派复制** 且 **是 leader 当前任期的日志**。
- 旧任期日志不能靠"复制数"直接提交;它只能随着**当前任期某条日志** commit 而被"连带提交"。

### 为什么旧日志不能按复制数直接提交?

经典反例(论文 Figure 8):某旧任期日志曾被复制到多数派,但随后的 leader 变更可能让它被覆盖。若提前提交它并回复客户端成功,这条"成功"的写就丢了。no-op 正是为了打破这个死局。

**自问自答**

- **问:commit 规则是什么?为什么旧任期日志不能按复制数直接提交?**
  答:一条 entry 可判定 committed 当且仅当"被多数派复制"且"是 leader 当前任期的日志";旧任期日志只能随当前任期某条日志的提交被连带提交。原因是论文 Figure 8 的反例:某旧任期日志可能曾复制到多数派,但随后的 leader 变更可能让它被覆盖,若提前提交并回复客户端成功,这条"成功"的写就丢了。

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

**自问自答**

- **问:日志不一致时如何处理?**
  答:leader 是权威。AppendEntries 带 prevLogTerm 做一致性检查,不匹配则拒绝并回退 nextIndex,直到找到公共前缀;然后截断 follower 冲突段、覆盖为 leader 的日志。已 commit 的日志必在公共前缀内,不受影响。braft 在拒绝时会返回更精确的冲突 index/term,一次跳过一段,避免逐条试错。

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

**工程进阶:租约读(ReadOnlyLeaseBased)** 🔍:若在 `election_timeout` 内没收到更高 term 的请求,可以认为领导权仍有"租约",跳过第 2 步的心跳,读延迟更低。**进阶思考:租约读为什么安全?风险在哪?——依赖时钟,时钟偏移过大时租约可能失真。**

**自问自答**

- **问:ReadIndex 的四个步骤?租约读如何优化?**
  答:① 记录当前 commitIndex 为 readIndex;② 心跳/广播确认自己仍是 leader;③ 等状态机 applied ≥ readIndex;④ 本地执行读。租约读(ReadOnlyLeaseBased)在 election_timeout 内未收到更高 term 请求时跳过第②步,读延迟更低,但依赖时钟——时钟偏移过大时租约可能失真。

## 9. 快照与落后节点追赶 ⭐

**问题**:日志无限增长,重启要重放全部历史,启动慢、占磁盘。解决:**定期把状态机状态固化(snapshot),丢弃此前的日志**。

**快照内容**:状态机在 index i 处的完整状态 + 元数据(最后包含的日志 index/term)。

**正常流程**:状态机把状态写入文件 → 保存元数据 → 通知共识模块可以裁剪 i 之前的日志。braft 中由 `StateMachine::on_snapshot_save()` 触发,`snapshot_interval_s` 控制频率。

**落后节点追赶**:

- 若 leader 发现某 follower 需要的日志**已被裁剪**(`nextIndex` 已小于最早日志)→ 改用 **InstallSnapshot** RPC。
- 流程:分块(chunk)发送 → follower 逐块落盘 → 全部收到后把快照加载进状态机(`on_snapshot_load`)→ 丢弃旧日志,从快照点继续复制。

**为什么落后节点能用快照追赶上?**——快照包含到 index i 为止的**全部状态**,天然覆盖它缺失的日志。注意快照只能包含**已 commit** 的状态(快照推进同样遵守 commit 语义)。

**自问自答**

- **问:落后节点如何追赶?**
  答:所需日志还在 → 用 AppendEntries 逐批补齐;日志已被裁剪 → 改用 InstallSnapshot RPC 分块传输。快照包含到某 index 为止的全部已提交状态,天然覆盖缺失的日志,follower 加载快照后从快照点继续复制。

## 10. 成员变更

**难点**:加/删节点改变的是"多数派"集合。若直接切换配置,新老配置的多数派可能**不相交** → 同时出现两个 leader(破坏选举安全)。

**Raft 论文方案**:

- **单节点变更**:一次只加/减一个节点。此时新老配置的多数派必然相交,无需额外机制,工程上最常用。
- **Joint Consensus(联合共识)**:一次变更多个节点时,先进入新老配置**共治**阶段(决策需新老两个多数派都通过),再切换到纯新配置。

**braft 实现 🔍**:`Node::add_peer / remove_peer / change_peers`。braft 通常让新节点先以 **learner(学习者)** 身份加入——只复制日志、**不参与投票**,追赶到接近最新后再提升为正式成员,避免空转节点拖慢集群。相关代码在 `src/braft/configuration.cpp`。

**自问自答**

- **问:成员变更为什么难?**
  答:加/删节点改变的是"多数派"集合,若直接切换配置,新老配置的多数派可能不相交 → 同时出现两个 leader,破坏选举安全。解决:单节点变更(一次只加/减一个节点,新旧多数派必然相交)、Joint Consensus(新旧配置共治,需两个多数派都通过)、learner 机制(新节点只复制不投票,追平后再提升)。

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
   │   日志(LogManager) · 元数据(RaftMeta) · 快照(Snapshot)
   │   复制(Replicator) · 选举 · 投票(BallotBox)
   ▼
brpc / bthread(网络与线程模型)
```

### 11.2 核心接口:StateMachine(你写的部分)

```cpp
class MyStateMachine : public braft::StateMachine {
    void on_apply(braft::Iterator& iter) override {
        // 逐条消费已 commit 的日志
        for (; iter.valid(); iter.next()) {
            apply_to_my_state(iter.data());          // 执行命令
            if (iter.done()) iter.done()->Run();     // 通知客户端"已成功"
        }
    }
    void on_leader_start(int64_t term) override { /* 成为 leader:开始接受写 */ }
    void on_leader_stop() override          { /* 不再是 leader:拒绝写 */ }
    void on_snapshot_save(braft::SnapshotWriter* w, braft::Closure* done) override {}
    void on_snapshot_load(braft::SnapshotReader* r) override {}
};
```

**注意**:状态机必须**幂等、可重复执行**——同一日志可能被 apply 多次(例如 leader 变更后的重放)。`on_apply` 里不要做不可重复的副作用。

**自问自答**

- **问:braft 里 StateMachine 和 Node 的分工?**
  答:Node 负责共识——日志复制、选举、commit 判定;StateMachine 由业务实现,负责"如何执行已提交日志"以及快照读写。`node->apply` 提交,`on_apply` 执行。

### 11.3 Node:生命线与 apply 流程

**生命周期**:`NodeOptions`(日志/元数据/快照的存储路径、`election_timeout_ms`、`initial_conf` 等)→ `node.init(opts)` → 运行 → `node.shutdown(...)` / `node.join()`。

**apply 流程(把第 5 节映射到代码)**:

```cpp
braft::Task task;
task.data = &my_command;        // 要复制的数据
task.done = my_done_callback;   // commit + apply 后回调(在 on_apply 里触发)
node->apply(task);              // 注意:异步!
```

背后发生的链路:`apply` 把数据封装成 log entry → 写入日志 → Replicator 广播 → 多数派落盘 → BallotBox 判定 commit → 触发 `on_apply` → 你的 done 被 `Run()`。**能否成功是异步的,不能从 `apply` 的返回值同步拿到**。

### 11.4 客户端视角:RouteTable 与重定向

- 客户端不知道谁是 leader,用 `RouteTable` 维护 group → leader 的映射(通常用全局 `braft::rtb`)。
- 请求打到非 leader 节点时,该节点返回重定向信息;客户端调用 `RouteTable::refresh_leader()` 重新拉取最新 leader,再重试。
- **这就是"客户端如何跟随 leader 变更"的答案**。

**自问自答**

- **问:braft 客户端如何找到 leader?**
  答:客户端用 RouteTable 维护 group → leader 映射;请求打到非 leader 节点会收到重定向信息,调用 `RouteTable::refresh_leader()` 重新拉取最新 leader 再重试。

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

### 11.7 Configraft:把 braft 落到分布式配置中心 ⭐

> 本项目([`configraft`](../README.md))是 braft 的一个完整落地样例。读懂它 = 把前 11 节的"原理 + 源码"串成一条能跑通的全链路。本节每段都回答同一个问题:**"braft 的某个算法机制,在我们的应用代码里对应哪几行、为什么这么写?"**
>
> 代码都在 `src/` 下,建议边读边对照真实源码。阅读前先建这张图:

```
src/raft/node.h           ConfigNode 抽象接口(Apply/Get/Watch/Compaction/AddPeer...)
   ├── src/raft/raft_node.cpp   RaftNode:集群模式,内部持有 braft::Node + 复制状态机
   └── src/raft/local_node.cpp  LocalNode:单机模式,绕过 Raft 直接落库
src/raft/state_machine.cpp      ConfigraftStateMachine:braft::StateMachine 的实现
src/store/store.cpp             Store:LevelDB 封装 + MVCC(状态机背后真正的"状态")
```

- 服务层(`src/server/`)只面向 `ConfigNode` 接口编程——**单机/集群切换对上层透明**;`src/server/server.cpp` 按 `--node` 是否为空决定实例化哪个实现,store 由 server 统一打开后移交节点(避免 LevelDB LOCK 重复占用)。
- `RaftNode::Init`(`src/raft/raft_node.cpp:72`)把 store、fsm、Node 串起来:`NodeOptions.fsm = fsm_.get()`,并把日志/元数据/快照分别指向 `data_dir/raft/{log,raft_meta,snapshot}`,`snapshot_interval_s = 30` 触发周期快照。

**为什么这么拆**:单机模式(LocalNode)不是"废代码"——它是 M1 里程碑先跑通全功能的垫脚石,也是压测/教学时排除 Raft 干扰的对照组。两种模式共用 `ApplyCmdToStore`(`src/store/store_ops.cpp:122`),**状态变更逻辑只有一份**——这正是"同一复制状态机"的代码体现。

#### 11.7.1 写路径:把"日志复制"走一遍 ⭐

客户端 PUT → Leader,对应链路(对照第 5 节六步):

```
KVServiceImpl::Put → node->Apply(RaftCmd)   # src/server/kv_service_impl.cpp
  → RaftNode::Apply                         # src/raft/raft_node.cpp:130
      ├─ IsLeader()? 非 Leader → NOT_LEADER + leader_id(重定向)
      ├─ RaftCmd 序列化为 IOBuf → 构造 braft::Task
      ├─ task.done = new ApplyClosure(out, wait_state)
      └─ node_->apply(task)                 # 算法真正开始:写日志→复制→commit
  → on_apply 里 ApplyCmdToStore(store, cmd)  # 状态机执行(所有节点)
  → ApplyClosure::WaitFor(wait_state) 唤醒,Apply() 返回
```

**要点**:

1. **`node_->apply` 是异步的,同步语义由应用自己造**。braft 的 `apply` 只提交任务,成功与否要等 `on_apply` 回调。Configraft 用 `ApplyClosure` + `WaitState`(`src/raft/state_machine.h:24`)把"异步回调"包装成阻塞式 API,让服务层拿到底层同步语义。
2. **为什么等待状态要独立于 closure(`shared_ptr`)** 🔍:braft 可能在 `apply()` 返回前就调用 `Run()` 并 `delete this`(任务快速完成 / 提交即失败,`--raft_sync=false` 高吞吐压测会复现)。若等待端持有 closure 裸指针,`Run()` 后访问就是 **UAF**。把等待状态装进 `shared_ptr`,`Run()` 里 `unique_ptr<ApplyClosure> self(this)` 自释放,等待端永远安全。
3. **同步原语选 bthread 而非 std::** 🔍:`std::mutex/std::condition_variable` 的 wait 会**占住 pthread worker**;高并发写场景下 worker 耗尽,节点假死(压测复现)。bthread 的 wait 挂起的是 bthread,释放 worker 给别的任务。代码注释(`state_machine.h:26`)原话是"压测复现"。
4. **`task.expected_term` 防 ABA**(`raft_node.cpp:150`):apply 时带上 `fsm_->leader_term()`,若提交前本节点已让位(不再是该任期 leader),braft 直接判定任务失败,避免"把旧任期 leader 的指令写进新任期日志"。

#### 11.7.2 on_apply:复制状态机的执行语义 ⭐

`ConfigraftStateMachine::on_apply`(`src/raft/state_machine.cpp:16`)是整条链路的"执行点",逐条消费已 commit 的日志:

```cpp
for (; iter.valid(); iter.next()) {
    braft::AsyncClosureGuard guard(iter.done());   // 本批结束后异步 Run,不阻塞状态机
    RaftCmd cmd; cmd.ParseFromZeroCopyStream(...);  // 反序列化日志
    ApplyResult result;
    ApplyCmdToStore(store_, cmd, &result);          // 串行落库(所有节点同一逻辑)
    if (hub_ && !result.events.empty())
        hub_->Broadcast(result.events);             // Watch 事件广播,见下
    if (iter.done()) closure->Finish(result);       // 唤醒发起请求的等待端
}
```

对应算法概念:

- **串行 apply**:所有节点的状态变更只发生在这里,天然单线程、无锁。直接满足 SMR"相同顺序执行相同命令 → 状态一致"的要求。
- **幂等 / 可重复**:leader 变更后同一条日志可能被重放。好在 `ApplyCmdToStore` 的每次写都是 LevelDB 的原子 `WriteBatch`(全局 revision 与数据同批提交,`store.h:47`),重放只是"写同样的值、推进同样的 revision",不破坏一致性。
- **Watch 广播为什么 Follower 也要做**:复制到 Follower 的日志 `iter.done() == nullptr`(done 只属于 Leader 上发起请求的那份),但事件流必须每个节点都有——否则 Watch 请求落到 Follower 就收不到实时事件。所以广播与 `iter.done()` 无关,只看 `result.events` 是否非空。

#### 11.7.3 读路径:没有 ReadIndex,用 Leader Lease(M5) ⭐

braft v1.1.2 没有 `read_index` API。Configraft 用 **braft 内置 leader lease** 实现线性一致读(`src/raft/raft_node.cpp:166`,注释把正确性论证写得很完整):

```
WaitLeaderLease()   → braft::LEASE_VALID
    ⟹ 本任期首条配置日志(= no-op)已提交
    ⟹ commit 覆盖先前任期的所有已提交写
    ⟹ (Leader Completeness) 本地状态机已含所有已提交写
WaitAppliedCatchUp() → applied >= committed(补齐异步 apply 的窗口)
    → 本地读 Store
```

- 对比第 8 节 ReadIndex 四步:**用"lease 有效"代替了第 2 步"心跳确认仍为 leader"**——`election_timeout` 内未收到更高 term 的请求,就认为领导权仍在租约内。
- 两个等待都带超时(`kLeaseWaitMs=1000`、`kCatchupWaitMs=1000`),且内部用 `bthread_usleep` 自旋(挂起 bthread,不占 pthread worker)。
- 失败分支有讲究:`WaitLeaderLease` 失败时,若本节点是 leader 但 lease 未就绪(如 NOT_READY 超时 / LEASE_DISABLED),返回 `INTERNAL`("leader not ready, retry")——**不能填自己的 `leader_id`** 让客户端重定向到自己,否则死循环;非 leader 才返回 `NOT_LEADER + leader_id`。
- **风险点(可深挖)** 🔍:lease 依赖时钟,时钟偏移过大时租约可能失真——这正是 etcd 后续改用 ReadIndex / 投票租约的原因之一。对比本项目的取舍见 [m5.md](m5.md)。

#### 11.7.4 快照:状态机的固化与恢复(M3 落地)

对照第 9 节:braft 到点触发 `on_snapshot_save`,Configraft 把它转到 Store 的 `ExportSnapshot`(同一个 LevelDB snapshot 下导出 revision + 主索引 + 配置版本索引,保证 **revision 与数据原子一致**),序列化为 protobuf 文件写进 `writer`。

```cpp
on_snapshot_save   → bthread 里跑 save_snapshot       # state_machine.cpp:92
  → store->ExportSnapshot(&data)                       # 单 leveldb::Snapshot,跨字段原子一致
  → ProtoBufFile.save(...) → writer->add_file("data")
on_snapshot_load   → store->LoadSnapshot(data)         # 关闭旧 DB、重建目录、批量写回
```

值得记住的三点:

- **快照导出放独立 bthread**(`bthread_start_urgent`):导出可能较慢,不能阻塞状态机线程。
- **快照只含已 commit 状态**:`ExportSnapshot` 在 LevelDB 的 snapshot 下读取,天然一致。
- **`LoadSnapshot` 持独占锁重建 DB**(`store.h:137` 的 `shared_mutex`):2026-08-20 代码审查发现,快照安装期间 `db_.reset()` 与并发读/Compaction 竞争导致 **UAF(HIGH 级)**;修复就是"快照重建持写锁,其余读写持读锁"。对应 [安全审查报告.md](安全审查报告.md) 第 1 项。

#### 11.7.5 成员变更:把异步 add/remove_peer 包成同步(M6)

对照第 10 节:braft 的 `add_peer/remove_peer` 是异步的(`done` 在 bthread 中回调)。Configraft 用 `ConfChangeClosure` + `shared_ptr<ConfChangeOutcome>` + bthread cv 把它包成同步 API(`src/raft/raft_node.cpp:344`)。

- 流程:先 `IsLeader()` 校验 → 解析 `PeerId` → `node_->add_peer(peer_id, new ConfChangeClosure(outcome))` → 主流程阻塞在 `cv.wait_for`(100ms 粒度轮询实现超时)。
- **超时 ≠ 失败**:`kConfChangeTimeoutSec = 60` 超时后返回的 message 明确提示"change may still be in progress, check health later"——add_peer 内部要先等新节点追平数据(快照/日志),这个动作无法撤销。
- **移除 Leader 自身**:braft 会在配置提交后让位(`ELEADERREMOVED`),`remove_peer` 返回成功,但本节点随即变成 follower。
- **新节点以 learner 身份追赶**:braft 的 add_peer 在配置提交前先让新 peer 追平,这正是第 10 节"learner 追赶后再提升"的工程落地。

#### 11.7.6 单机模式 LocalNode:没有 Raft,为什么还值得读

`LocalNode::Apply`(`src/raft/local_node.cpp:13`)做的事与集群模式惊人一致:

```cpp
void LocalNode::Apply(const RaftCmd& cmd, ApplyResult* out) {
    std::lock_guard<std::mutex> lock(mu_);      // ← 唯一区别:手动串行化
    ApplyCmdToStore(store_.get(), cmd, out);
    if (hub_ && !out->events.empty()) hub_->Broadcast(out->events);
}
```

- **写路径串行化的来历** 🔍:集群模式下"串行"由 braft `on_apply` 天然保证;单机模式没有 Raft,brpc 多个 worker 会并发调 `Apply`,若不加锁,Store 的 revision "读-改-写"会拿到重复值——2026-08-20 审查发现的 H2:并发 Put/CAS 下全局 revision 重复、Watch 续传错乱。
- 这正是"复制状态机串行 apply"思想的镜像:**哪怕没有 Raft,也要在状态机入口保持单写者**。
- `LocalNode::IsLeader()` 恒为 `true`、`LeaderId()` 为空、成员变更返回 `INTERNAL`(不支持)——把"单机没有一致性问题、没有成员"如实暴露给上层,服务层无需感知差异。

#### 11.7.7 一张表:前 11 节 ↔ Configraft 代码

| 算法概念(§) | Configraft 落点 | 一句话 |
|---|---|---|
| 状态机复制(§2) | `ConfigNode` 抽象 + `ApplyCmdToStore` | 单机/集群共用一份状态变更逻辑 |
| 日志复制(§5) | `RaftNode::Apply` → `node_->apply` → `on_apply` | 异步 apply 包装成同步等待 |
| no-op(§5) | 隐含在 `WaitLeaderLease` 的"配置条目已提交" | lease 就绪 ⟹ no-op 已 commit |
| 安全性(§6) | `task.expected_term` 防 ABA | 写提交打任期戳,防旧 leader 写新日志 |
| 线性一致读(§8) | `WaitLeaderLease` + `WaitAppliedCatchUp` | braft 无 ReadIndex,用 lease 替代 |
| 快照追赶(§9) | `on_snapshot_save/load` + `Store::Export/LoadSnapshot` | LevelDB snapshot 导出,独立 bthread |
| 成员变更(§10) | `RaftNode::Add/RemovePeer` | 异步闭包 → 同步 API + 超时 |
| 状态机接口(§11.2) | `ConfigraftStateMachine`(继承 `braft::StateMachine`) | 业务方只需要实现回调 |
| apply 流程(§11.3) | `ApplyClosure` + `WaitState`(bthread cv) | 同步等待的 UAF 防御与 worker 友好 |

> 读完这一节,建议动手验证:单机跑 `./build/configraft_server --port 8100` 打断点看一次 Apply 全链路;再 `bash scripts/run_cluster.sh` 起 3 节点,kill Leader 观察重定向与重新选举(混沌脚本 `scripts/chaos_test.sh`)。

## 12. 学习路线与资源

按计划走,顺序不能乱:

1. **论文**:《In Search of an Understandable Consensus Algorithm》(Ongaro & Ousterhout),建议中文翻译版 + 英文对照读。边读边在纸上画 term / 日志结构。
2. **交互演示**:raft.github.io 动图。亲手模拟选举、日志复制,直到能**预判下一步**再点下一步。
3. **braft 官方文档**(baidu.github.io/braft):overview → server → client → cli → replication。
4. **示例精读**:`example/counter/server.cpp` 与 `client.cpp`,弄清 StateMachine、`node->apply`、RouteTable 三者的配合。
5. **源码精读**:配合第 11.5 节表格,只读关键路径,不逐行。
6. **项目代码**:对照第 11.7 节,把各节的"自问自答"在 Configraft 源码里找到对应落点,再回到本节。

**自检标准**:能不看资料,把各节末尾的"自问自答"用自己的话讲清楚,并能画出 §11.1 架构图与 §11.7 代码地图 → 这一轮过关。

------

*笔记维护记录:2026-08-13 创建;2026-08-25 新增 11.7 Configraft 落地分析;2026-08-25 调整文档定位为"学习笔记 + 学习深度展示",删除面试速查表,面试问题以自问自答形式融入各知识点。*
