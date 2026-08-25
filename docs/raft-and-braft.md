# Raft 原理与 braft 落地：我的学习笔记

> **这是一份以第一人称写下的学习笔记**。我的学习路径是：先把 Raft 算法讲清楚（概念），再读 braft 源码看它怎么把算法变成代码（braft 精读），最后回到我参与开发的 Configraft 项目看这些机制怎么被用起来、解决什么问题（项目落地）。每一个关键机制后面都附了我自己当初卡住的问题，用"自问自答"的方式写了下来。
>
> **怎么读**：第一遍按目录顺序通读建立骨架；第二遍对照 raft.github.io 动图过第 5、6 节；读 braft 源码时回头查第 4 节与各节的"braft 精读"；读 Configraft 代码时对照各节的"项目落地"与第 12 节代码地图。
>
> **标记约定**：
>
> - ⭐ = 核心必会
> - 🔍 = 需要对照源码/文档深挖

## 目录

1. 我的学习笔记
2. 背景：共识与状态机复制
3. 核心概念与术语
4. braft 框架精读：counter 示例入门 ⭐
   4.1 总体架构：算法在库里，状态在我写的 StateMachine 里
   4.2 server.cpp 精读：StateMachine + Node + apply
   4.3 client.cpp 精读：RouteTable 与重定向
5. 领导者选举 ⭐
6. 日志复制 ⭐
7. 安全性：为什么这套机制是对的 ⭐
8. 日志不一致的处理 ⭐
9. 线性一致读 ReadIndex ⭐
10. 快照与落后节点追赶 ⭐
11. 成员变更
12. Configraft 项目中的 Raft 落地
13. 学习路线与资源

------

## 1. 我的学习笔记

这份笔记有**两个用途**，先想清楚再读才不会读偏：

1. **它首先是我自己的学习记录**。我学 Raft 的路线是三层递进的：**算法概念 → braft 源码 → Configraft 项目落地**。每一层我都逼自己回答同一个问题："**这段代码在实现算法的哪个机制？**" `node->apply()` 一行能跑通 demo，但我关心的是它背后发生了什么、哪些是异步的、braft 为什么这样设计。
2. **其次它也是我向别人展示学习深度的材料**。不是背给别人听的答案，而是"从概念一路挖到源码和工程实现"的完整思考过程。比如 Configraft 在 M5 里为什么不用 ReadIndex 而用 Leader Lease（因为 braft v1.1.2 没有 read_index API），这类取舍本身就是最好的证明。

怎么读：先通读概念（§2、§3）建立骨架，再用第 4 节的 counter 示例理解 braft 的工程框架，然后按 §5–§11 逐个机制吃透（每节都是"概念 → braft 实现 → Configraft 实现 → 问题"），最后用第 12 节把项目代码串起来。

## 2. 背景：共识与状态机复制

**问题**：分布式系统里多副本各自执行，怎么保证它们最终状态一致，而且已经确认的写不会丢？

**状态机复制（State Machine Replication, SMR）** 是解决这个问题的通用框架：只要每个副本以**相同顺序**执行**相同命令**，最终状态就一定一致。于是"如何让状态一致"被化简成"如何让所有副本按相同顺序收到相同日志"——这恰恰是共识算法负责的部分。

```
客户端 ──(command)──> 共识模块(日志) ──apply──> 状态机(应用层)
                         │ 复制
                  其他副本的共识模块 ──apply──> 各自状态机
```

**Raft 的定位**：共识算法的一种，最大的卖点是**可理解性**。它把共识问题拆成三个相对独立的子问题：

1. **领导者选举** —— 谁的日志被当作"事实"；
2. **日志复制** —— 把命令复制到所有节点；
3. **安全性** —— 保证前两条不会产生错误结果。

**braft 的定位**：百度开源的 C++ 版 Raft，基于 brpc/bthread。它把"共识引擎"做成库，业务方只需要实现 `StateMachine`。理解 braft 关键是一句话：**算法在库里，状态在我写的 StateMachine 里，两者通过接口解耦。** 这句话我在读 counter 示例后体会特别深，见第 4 节。

## 3. 核心概念与术语

| 术语                   | 含义                                      | 一句话直觉                      |
| ---------------------- | ----------------------------------------- | ------------------------------- |
| Term(任期)             | 单调递增的整数，每开始一次选举 +1          | 全局"代际"，用来区分新旧 leader |
| Leader                 | 唯一能接受写请求、主导日志复制的节点      | 日志的"唯一作者"                |
| Follower               | 被动接收日志、回应投票                    | 大多数时间都是它                |
| Candidate              | 选举中的自荐者                            | 超时后短暂出现的角色            |
| Log Entry              | 一条日志记录 (index, term, command)       | 命令 + 编号                     |
| commitIndex            | 已确认可以执行到哪条日志                  | "落定"水位线                    |
| lastApplied            | 状态机已应用到哪条                        | "执行"水位线                    |
| matchIndex / nextIndex | leader 记录的每个 follower 的日志同步进度 | 对账用的两个指针                |
| Quorum(多数派)         | 超过半数的节点集合                        | 决策门槛，任意两个多数派必有交集 |

**两条核心不变量**：

- 同一 index 的日志只属于一个 term（日志只有追加和截断，没有中间插队）；
- 一旦某条日志 commit，它和它之前的所有日志最终都会被执行（commit 单调性）。

Raft 把服务器状态分成三类：**持久化状态**（`currentTerm`、`votedFor`、`log[]`，响应 RPC 前必须先落盘）、**服务器易失状态**（`commitIndex`、`lastApplied`）、**leader 易失状态**（`nextIndex[]`、`matchIndex[]`）。两个 RPC：**AppendEntries**（日志复制 + 心跳，带 `prevLogIndex/prevLogTerm` 做一致性检查、`leaderCommit` 推进提交）和 **RequestVote**（选举，带 `lastLogIndex/lastLogTerm` 做日志新旧判断）。具体字段含义我在读源码时再逐个对照，这里先建立直觉。

## 4. braft 框架精读：counter 示例入门 ⭐

**这是我学 braft 的入口。** braft 自带 `example/counter`（一个分布式计数器：`fetch_add` 通过 Raft 复制到所有副本，`get` 读当前值）。它只有 `server.cpp`、`client.cpp` 两个文件，却完整展示了业务方接入 braft 的三件事：**实现 `StateMachine`、`node->apply` 提交、`RouteTable` 路由**。我把这一节当作 braft 的"框架说明书"，后面 §5–§11 讨论具体算法机制时，就回这里找代码落点。

### 4.1 总体架构：算法在库里，状态在我写的 StateMachine 里

我读 counter 时先画了这张依赖图：

```
业务代码(你的应用)
   │ 实现 StateMachine 接口
   ▼
braft::StateMachine(我写的类)
   ▲ 回调:on_apply / on_snapshot_save / on_leader_start / on_leader_stop ...
   │
braft::Node(共识引擎, node.cpp)
   │   日志(LogManager) · 元数据(RaftMeta) · 快照(Snapshot)
   │   复制(Replicator) · 选举 · 投票(BallotBox)
   ▼
brpc / bthread(网络与线程模型)
```

**核心结论**：共识逻辑（日志复制、选举、commit 判定）全在 braft 内部；我作为业务方只做两件事——把要复制的指令通过 `node->apply(task)` 交出去，然后在 `on_apply` 回调里把已提交的日志应用到自己的状态上。`Node` 和 `StateMachine` 靠接口解耦，我甚至可以不知道 leader 怎么选举也能把 counter 跑起来——但要真正理解，就得逐层往下挖。

### 4.2 server.cpp 精读：StateMachine + Node + apply

counter 的 `Counter` 类（`third_party/src/braft/example/counter/server.cpp:63`）同时充当了 `CounterService`（brpc 服务）和 `braft::StateMachine`。我拆开看它的三个关键角色：

**① 启动节点：`start()`（server.cpp:75）**

```cpp
butil::EndPoint addr(butil::my_ip(), FLAGS_port);
braft::NodeOptions node_options;
node_options.initial_conf.parse_from(FLAGS_conf);      // 初始集群配置
node_options.election_timeout_ms = FLAGS_election_timeout_ms;
node_options.fsm = this;                                // 把 StateMachine 交给 Node
node_options.snapshot_interval_s = FLAGS_snapshot_interval;
std::string prefix = "local://" + FLAGS_data_path;
node_options.log_uri = prefix + "/log";                 // 日志/元数据/快照分开存
node_options.raft_meta_uri = prefix + "/raft_meta";
node_options.snapshot_uri = prefix + "/snapshot";
braft::Node* node = new braft::Node(FLAGS_group, braft::PeerId(addr));
node->init(node_options);
```

我注意到三点：`fsm = this` 把状态机交给 Node（`node_owns_fsm = false` 表示析构不用 Node 管）；日志、元数据、快照三份存储可以分开配置，这为后面"不同存储介质"留了余地；`init` 之后 Node 才开始参与选举。

**② 提交写请求：`fetch_add`（server.cpp:102）——这就是 `node->apply` 的典型用法**

```cpp
// 序列化请求到 IOBuf
butil::IOBuf log;
butil::IOBufAsZeroCopyOutputStream wrapper(&log);
request->SerializeToZeroCopyStream(&wrapper);
// 构造一个 braft::Task 提交给复制组
braft::Task task;
task.data = &log;
task.done = new FetchAddClosure(this, request, response, done_guard.release());
if (FLAGS_check_term) {
    task.expected_term = term;   // 防 ABA，见 §7
}
_node->apply(task);   // 注意：异步！
```

注释里写得很直白：`_value` 不能在这个函数里直接改，否则会和组内其他节点不一致。**所有状态变更必须走日志**——这是 SMR 的核心约束，我在项目里也严格守着这条。

**③ 消费已提交日志：`on_apply`（server.cpp:183）——状态机真正改状态的地方**

```cpp
void on_apply(braft::Iterator& iter) {
    for (; iter.valid(); iter.next()) {
        braft::AsyncClosureGuard closure_guard(iter.done());  // 让 done 异步执行，不阻塞状态机
        if (iter.done()) {
            // 本节点提交的请求：直接从 closure 里取结果，免去再次解析
            FetchAddClosure* c = dynamic_cast<FetchAddClosure*>(iter.done());
            response = c->response();
            detal_value = c->request()->value();
        } else {
            // Follower 上复制来的日志：必须重新从 iter.data() 解析
            ...
        }
        _value.fetch_add(detal_value);   // 唯一的修改点
    }
}
```

这里我悟到两点：**`iter.done()` 只在"本节点发起的请求"上非空**（从 Follower 复制来的日志 done 为空，只能重新解析）；**`on_apply` 是所有副本唯一的改状态入口**，天然串行。

**④ leader 状态与重定向：`on_leader_start/on_leader_stop` + `redirect`（server.cpp:172/284）**

```cpp
void on_leader_start(int64_t term) { _leader_term.store(term); }
void on_leader_stop(const butil::Status& status) { _leader_term.store(-1); }
bool is_leader() const { return _leader_term.load() > 0; }
void redirect(CounterResponse* response) {
    response->set_success(false);
    braft::PeerId leader = _node->leader_id();
    if (!leader.is_empty()) response->set_redirect(leader.to_string());
}
```

`_leader_term > 0` 就认为是 leader，只有 leader 才 `apply`；非 leader 收到写请求就回 `redirect`（携带 leader 地址），让客户端重定向。读请求 `get()` 也只在 `is_leader()` 时才直接读本地 `_value`。

### 4.3 client.cpp 精读：RouteTable 与重定向

客户端（`client.cpp`）不知道谁是 leader，靠 `RouteTable` 维护 group → leader 的映射，流程是：

```cpp
// ① 启动时把集群配置注册进 RouteTable
braft::rtb::update_configuration(FLAGS_group, FLAGS_conf);

// ② 每次发请求前，从 RouteTable 选 leader
if (braft::rtb::select_leader(FLAGS_group, &leader) != 0) {
    // 不知道 leader，就让 RouteTable 去"问"（refresh_leader 会探测各 peer）
    braft::rtb::refresh_leader(FLAGS_group, FLAGS_timeout_ms);
    continue;
}

// ③ 发给 leader
stub.fetch_add(&cntl, &request, &response, NULL);

// ④ 失败或返回 redirect 时更新路由表，下一轮再试
if (!response.success() && response.has_redirect()) {
    braft::rtb::update_leader(FLAGS_group, response.redirect());
}
```

**这就是"客户端如何跟随 leader 变更"的答案**：靠重定向信息驱动 RouteTable 自愈，客户端本身不需要感知选举。我在 Configraft 的客户端和混沌测试里都复用了这个思路。

## 5. 领导者选举 ⭐

### 5.1 概念：选举流程

Raft 选举的本质：**让节点在"没人告诉我谁是 leader"时主动自荐，靠多数派认可拿到唯一领导权**。流程概括为：

1. Follower 在 `election_timeout` 内没收到 leader 心跳 → 认为 leader 失联。
2. 自增 term、转为 Candidate，先投自己一票。
3. 并行向所有节点发 `RequestVote`。
4. 拿到**多数派**投票 → 成为 leader，立即广播心跳（空 AppendEntries）确立权威。
5. 超时未获胜 → term 再 +1，重试（随机超时避免"选票分裂"死循环）。
6. 收到任期 **>= 自己 term** 的 leader 心跳 → 退回 Follower。

三个关键点：**随机化超时**（论文 150–300ms，避免所有节点同时超时导致选票分裂）、**一任期最多一票**（votedFor 落盘，保证同任期至多一个 leader）、**日志新者优先**（候选人日志不比投票者旧才投，保证已提交日志不丢）。

### 5.2 braft 实现

- 选举在主流程在 `src/braft/node.cpp` + `election.cpp`，`election_timeout_ms` 控制超时（counter 里配了 5000ms），实际触发带随机抖动。
- **角色感知在回调**：counter 里我不需要自己写选举逻辑，`on_leader_start(term)` 被调用即代表"我成为该 term 的 leader"，`on_leader_stop` 代表让位——braft 把"何时当选"封装在库里，业务方只感知结果。
- **pre_vote（预投票）**：braft 可配置。它解决的是**网络分区里的节点 term 不断自增、分区恢复后干扰正常 leader** 的问题——节点先问"如果我发起选举你们支持吗"，拿到多数派预支持后才真正自增 term 竞选，避免 term 失控。

### 5.3 Configraft 落地：用选举保证"单写者"

Configraft 用选举解决配置中心的**单写者问题**：同一时刻只有一个 leader 接受写，天然避免多个节点并发写配置导致的冲突。落地在 `src/raft/`：

- `ConfigraftStateMachine` 用原子变量 `leader_term_` 记录当前任期（`src/raft/state_machine.cpp:61` `on_leader_start` 置 term、`on_leader_stop` 置 -1），`is_leader()` 判断。
- `RaftNode::Apply`（`src/raft/raft_node.cpp:130`）开头就 `IsLeader()` 校验，非 leader 返回 `NOT_LEADER` + `leader_id`，客户端据此重定向——**这套交互与 counter 的 redirect 完全同构，只是语义变成了错误码**。
- `RaftNode::Role()` 把 braft 的 `NodeStatus.state` 翻译成 leader/candidate/follower，供健康检查 `/healthz` 暴露。

**问题：为什么只有 leader 能写，而不是"多数派同意就写"？** 因为如果每个节点都能直接写本地，两个写者可能把互相冲突的命令发到不同日志位置，状态机无法收敛。Raft 的答案是：**先选出一个 leader，让它当日志的"唯一作者"，其余节点只负责复制和执行**——把"多方协商一致"化简为"一方作主 + 多方跟随"，正确性论证全部转移到 leader 选举上。这也是为什么选举安全（同任期至多一个 leader）是整套机制的地基。

## 6. 日志复制 ⭐

### 6.1 概念：日志复制流程

日志复制的目标：**把 leader 日志里的命令，按相同顺序复制到所有节点并最终执行**。流程概括为：

1. Leader 把命令追加为本地日志 entry (index, term, cmd)。
2. 并行向所有 follower 发 AppendEntries。
3. **多数派落盘成功 → 判定该 entry 已 commit**。
4. Leader 推进 commitIndex，在后续 AppendEntries 里捎带给所有节点。
5. 各节点把 <= commitIndex 的日志逐条 apply 到状态机。
6. Leader 回复客户端成功（**只有 commit 后才算成功**）。

配套机制：**nextIndex/matchIndex** 是 leader 记录每个 follower 复制进度的对账指针；**批量 + 流水线（pipelining）** 允许多个 AppendEntries 同时在途提升吞吐。

### 6.2 braft 实现

- **提交入口**：`node->apply(task)`。背后链路（我在 4.2 已见代码）：把 task 封装成 log entry → 写入本地日志 → Replicator 广播 → 多数派落盘 → BallotBox 判定 commit → 触发 `on_apply` → `iter.done()` 被 `Run()`。**能否成功是异步的**，不能从 `apply` 的返回值同步拿到。
- **批量与流水线**：`src/braft/replicator.cpp`，`nextIndex` 从 leader `lastLogIndex+1` 起算，对账失败回退（见 §8）。
- **no-op 日志**：新 leader 上任后**第一件事是在日志里追加一条空 entry**。为什么必须有它，见下面的问题。

### 6.3 Configraft 落地：写请求走日志、串行落库

Configraft 的写路径（`src/raft/raft_node.cpp:130` `Apply`）把整个 RaftCmd 序列化后走 `node_->apply`：

```
KVServiceImpl::Put → node->Apply(RaftCmd)
  → RaftNode::Apply
      ├─ IsLeader()? 非 Leader → NOT_LEADER + leader_id
      ├─ RaftCmd 序列化为 IOBuf → 构造 braft::Task
      ├─ task.done = new ApplyClosure(out, wait_state)
      └─ node_->apply(task)          # 写日志 → 复制 → commit
  → on_apply 里 ApplyCmdToStore(store, cmd)   # 所有节点同一逻辑串行落库
  → ApplyClosure::WaitFor 唤醒，Apply() 返回
```

三个工程细节值得记：

1. **同步等待的封装**：braft 的 apply 是异步的，Configraft 用 `ApplyClosure` + `WaitState`（`src/raft/state_machine.h:24`）把回调包装成阻塞式 API，服务层拿到底层同步语义。等待状态必须用 `shared_ptr` 独立于 closure 存活——**braft 可能在 `apply()` 返回前就调用 `Run()` 并 `delete this`**，若等待端持有裸指针就是 UAF（`--raft_sync=false` 高吞吐压测会复现）。
2. **同步原语选 bthread 而非 std::**：`std::mutex/std::condition_variable` 的 wait 会**占住 pthread worker**，高并发写时 worker 耗尽、节点假死；bthread 的 wait 挂起 bthread、释放 worker。
3. **`task.expected_term` 防 ABA**：apply 时带上 `fsm_->leader_term()`，提交前若本节点已让位，braft 直接判定失败，避免把旧任期 leader 的指令写进新任期日志。

**问题：为什么新 leader 必须先写 no-op 日志？** 因为 Raft 规定**只有"当前任期的日志被复制到多数派"才能推进 commit**。新 leader 的日志里可能只有旧任期日志，如果不写 no-op，它永远无法提交任何日志，系统停摆。no-op 一旦被多数派复制并 commit，它之前的旧日志被**连带提交**（Log Matching）；同时 no-op commit 标志着领导权真正生效，之后才对外提供写服务。反例：若允许新 leader 直接提交"已复制到多数派"的旧任期日志，未来这条日志可能被另一个新 leader 覆盖，导致已返回成功的写丢失——no-op 把"能不能提交"推迟到"当前任期日志落定"之后，这个设计是整个安全性的枢纽（§7 的 commit 规则）。

## 7. 安全性：为什么这套机制是对的 ⭐

### 7.1 概念：五条不变量

Raft 用 5 条不变量保证正确性。**重点在"为什么"，不要背条文**：

1. **Election Safety**：每个 term 至多一个 leader → 由"一节点一票 + 多数派"保证。
2. **Leader Append-Only**：leader 从不删除或覆盖自己的日志，只追加 → 已 commit 的日志在 leader 上永远保留。
3. **Log Matching**：两条日志相同 index、相同 term，则它们之前的所有日志都相同 → 由 AppendEntries 的 `prevLogTerm` 校验保证。
4. **Leader Completeness**：已 commit 的日志一定出现在之后所有 leader 的日志里 → 由"日志最新者才能当选"（选举限制）保证。
5. **State Machine Safety**：节点把 index i 的日志 apply 到状态机后，其他节点不会在 i 上 apply 不同命令 → 由 3 + 4 推出。

**Commit 规则**（重中之重）：一个 entry 可判定 committed，当且仅当 **被多数派复制** 且 **是 leader 当前任期的日志**。旧任期日志不能靠复制数直接提交，只能随当前任期某条日志的提交被"连带提交"。

### 7.2 braft 实现

- **Leader Append-Only 由框架保证**：braft 的 `LogManager` 只支持追加和按 leader 日志截断 follower，业务方无法从 `on_apply` 反向改日志。
- **Log Matching 由 AppendEntries 的 `prevLogTerm` 检查保证**：follower 校验失败就拒绝，见 §8。
- **expected_term 的防 ABA 语义**：counter 里 `FLAGS_check_term` 默认打开（server.cpp:130），`task.expected_term = term`。它防的是这个问题：一个请求在 apply 排队期间 leader 发生变更（如任期 5→6→5），如果没有 expected_term，这个"旧任期请求"可能在新任期被当成合法任务执行。带任期戳后，任务提交时若任期不匹配就判失败。

### 7.3 Configraft 落地：`expected_term` 防 ABA + 串行 apply

- **expected_term**：`RaftNode::Apply` 里 `const int64_t term = fsm_->leader_term(); if (term > 0) task.expected_term = term;`（`src/raft/raft_node.cpp:150`）。leader 刚当选时 term 可能为 -1，此时不设 expected_term（还没到防 ABA 的阶段）。
- **串行 apply**：`ConfigraftStateMachine::on_apply`（`src/raft/state_machine.cpp:16`）是所有副本**唯一的改状态入口**，天然单线程。`ApplyCmdToStore` 的每次写都是 LevelDB 原子 `WriteBatch`（全局 revision 与数据同批提交，`src/store/store.h:47`），因此 leader 变更后日志重放（同一日志被 apply 多次）也只是"写同样的值、推进同样的 revision"，**幂等、可重复**——这是 State Machine Safety 在工程上的落实。

**问题：为什么旧任期日志不能按复制数直接提交？** 经典反例（论文 Figure 8）：某旧任期日志曾被复制到多数派，但随后的 leader 变更可能让它被覆盖。若提前提交它并回复客户端成功，这条"成功"的写就丢了。no-op 正是为了打破这个死局（§6 的问题里已展开）。**一句话：复制到多数派是"当时成立"，commit 要求"未来不会变"，后者只有当前任期 leader 能保证。**

## 8. 日志不一致的处理 ⭐

### 8.1 概念：冲突处理流程

**不一致从哪来**：网络分区、旧 leader 的未 commit 日志等，导致 follower 日志可能比 leader 多（脏日志）、少，或与 leader 冲突。**Raft 的哲学：leader 是权威，冲突以 leader 为准。**

处理流程：

1. Leader 从 `nextIndex[f]` 处发 AppendEntries，带上 `prevLogIndex/prevLogTerm`。
2. Follower 校验 `prevLogIndex` 处日志的 term 是否匹配：不匹配 → 拒绝并返回冲突信息，leader 回退 `nextIndex[f]` 再试；匹配 → 接受，并截断冲突点之后的日志，覆盖为 leader 的日志。
3. 反复回退直到找到双方一致的**公共前缀**，此后跟随 leader。

**关键性质**：由于 Log Matching，冲突点之后的日志全部删除重建即可，不影响已 commit 的部分（已 commit 的日志必然在公共前缀内）。

### 8.2 braft 实现

braft 同样基于"AppendEntries 拒绝 + nextIndex 回退"，但拒绝时会返回**更精确的冲突 index/term**，一次跳过一段，避免逐条试错——这是论文"逐条回退"的工程优化。落点在 `src/braft/node.cpp` 处理 AppendEntries 响应的分支、`src/braft/replicator.cpp` 的 `_next_index` 推进逻辑。

### 8.3 Configraft 落地：落后节点自动追赶

Configraft **没有为日志不一致写任何额外代码**——这正是 braft 的价值：落后节点重启后，leader 的 Replicator 自动用 AppendEntries 把它补齐。我在混沌测试（`scripts/chaos_test.sh`）里 kill 一个 follower 再重启，观察它重新加入集群后数据追平，验证的就是这个机制。

**问题：follower 日志和 leader 冲突，为什么删除冲突段不会误删已提交数据？** 因为**已提交的日志必然同时存在于 leader 和 follower 的公共前缀里**。由 Leader Completeness（已提交日志在新 leader 日志里）和 Log Matching（公共前缀一致）共同保证：leader 能当选就说明它日志最新，follower 与 leader 的差异只可能发生在"双方都没提交"的区域，删除重建是安全的。我初学时常担心"会不会把已提交的砍了"，想通这一点就释然了。

## 9. 线性一致读 ReadIndex ⭐

### 9.1 概念：为什么读也需要共识

**问题**：leader 直接读本地状态机，可能读到**过期数据**——因为读的这一刻它可能已经被分区，不再是法定 leader。要求读操作也满足**线性一致（Linearizable）**：读到的必须包含此前所有已确认的写。

**为什么不直接走日志复制？** 写走一遍日志、读也走一遍日志，延迟和吞吐都不可接受。Raft 论文 §6.4 提出 **ReadIndex** 四步：

1. Leader 记录当前 `commitIndex` 作为 `readIndex`（此刻"已确认到哪"）。
2. 通过一次心跳/广播**确认自己仍是 leader**（多数派响应 = 领导权还在）；若期间出现更高 term，本次读中止。
3. 等待状态机 `lastApplied >= readIndex`（确保已执行到读点之前的所有写）。
4. 在本地状态机上执行读，返回结果。

**为什么正确**：第 2 步保证读时刻 leader 身份有效；第 1+3 步保证读到的状态包含 `readIndex` 之前的全部写。

**租约读（ReadOnlyLeaseBased）**：若在 `election_timeout` 内没收到更高 term 的请求，可以认为领导权仍有"租约"，跳过第 2 步的心跳，读延迟更低。**风险：依赖时钟，时钟偏移过大时租约可能失真。**

### 9.2 braft 实现

- braft 提供 `Node::read_index(Closure* done, int64_t* index)`，回调在状态机 apply 到该 index 后被触发，业务方在回调里执行读。
- counter 的 `get()` 其实只做了最简版本：`is_leader()` 为真就直接读本地 `_value`（server.cpp:138）。它没有 ReadIndex 也没有租约，严格来说只能算"尽力而为的强一致读"——因为 `is_leader()` 只说明"我过去是 leader"，不能证明"此刻仍是法定 leader"。我在项目里没有抄这个简化做法，而是用了更严格的 lease 方案（见下）。

### 9.3 Configraft 落地：braft 无 ReadIndex，用 Leader Lease（M5）

Configraft 用的 braft **v1.1.2 没有 `read_index` API**，所以我用 **braft 内置的 leader lease** 实现线性一致读（`src/raft/raft_node.cpp:166`，注释把正确性论证写得很完整）：

```
WaitLeaderLease()   → braft::LEASE_VALID
    ⟹ 本任期首条配置日志(= no-op)已提交
    ⟹ commit 覆盖先前任期的所有已提交写
    ⟹ (Leader Completeness) 本地状态机已含所有已提交写
WaitAppliedCatchUp() → applied >= committed(补齐异步 apply 的窗口)
    → 本地读 Store
```

关键论证：**lease 有效 ⟹ 本任期首条配置日志（即 no-op）已提交 ⟹ commit 覆盖先前任期所有已提交写 ⟹ 本地状态机已含全部已提交写**。对比 ReadIndex 四步，这里**用"lease 有效"代替了第 2 步"心跳确认仍为 leader"**——`election_timeout` 内未收到更高 term 请求，就认为领导权仍在租约内。

工程细节：

- 两个等待都带超时（`kLeaseWaitMs=1000`、`kCatchupWaitMs=1000`），内部用 `bthread_usleep` 自旋，不占 pthread worker。
- 失败分支有讲究：`WaitLeaderLease` 失败时，若本节点是 leader 但 lease 未就绪（NOT_READY 超时 / LEASE_DISABLED），返回 `INTERNAL`（"leader not ready, retry"）——**不能填自己的 `leader_id`** 让客户端重定向到自己，否则死循环；非 leader 才返回 `NOT_LEADER + leader_id`。
- 读路径降级：`?serializable=true` 允许 Follower 本地读（监控用，可容忍轻微过期）。

> 本节的完整设计论证与代码注释见 [m5.md](m5.md)（M5 里程碑：CAS 原子更新 + Leader Lease 线性一致读）。

**问题：租约读为什么安全？风险在哪？** 安全的直觉是"lease 有效 ⟹ 我仍是法定 leader ⟹ 本地状态是完整的"。但 lease 依赖一个假设：**其他节点在 lease 有效期内不会选出新 leader**——这靠"election_timeout 内没有比我更新的 term"来近似，而它最终依赖时钟。如果本节点时钟偏慢，lease 可能"假到期"（保守，只会拒读不会错读）；如果其他节点时钟偏快，它们可能在新 leader 选出时我的 lease 还"假有效"（危险）。工程上常见做法是给 lease 留余量，etcd 后来干脆改用 ReadIndex/投票租约来摆脱纯时钟依赖——**这就是我在 Configraft 里没追求极致低延迟、而选择接受 lease 语义并写好文档注释的原因**。

## 10. 快照与落后节点追赶 ⭐

### 10.1 概念：为什么要快照

**问题**：日志无限增长，重启要重放全部历史，启动慢、占磁盘。解决：**定期把状态机状态固化（snapshot），丢弃此前的日志**。快照内容 = 状态机在 index i 处的完整状态 + 元数据（最后包含的日志 index/term）。

落后节点追赶两条路：

- 所需日志**还在** → 用 AppendEntries 补齐；
- 日志**已被裁剪**（`nextIndex` 小于最早日志）→ 改用 **InstallSnapshot** RPC：分块发送 → follower 逐块落盘 → 全部收到后加载进状态机 → 丢弃旧日志，从快照点继续复制。

### 10.2 braft 实现：counter 的快照是标准范式

counter 的快照代码（server.cpp:230）几乎就是 braft 快照的标准模板，我读它时对照着 `src/braft/snapshot.cpp`：

```cpp
static void *save_snapshot(void* arg) {
    // 序列化状态机到文件
    Snapshot s; s.set_value(sa->value);
    braft::ProtoBufFile pb_file(snapshot_path);
    pb_file.save(&s, true);
    sa->writer->add_file("data");   // 快照 = 一组文件，把文件登记进 writer
}
void on_snapshot_save(braft::SnapshotWriter* writer, braft::Closure* done) {
    // 存盘较慢，开独立 bthread 避免阻塞状态机
    bthread_start_urgent(&tid, NULL, save_snapshot, arg);
}
int on_snapshot_load(braft::SnapshotReader* reader) {
    // Leader 不加载快照
    CHECK(!is_leader());
    // 从 reader 读出文件，替换状态机
    braft::ProtoBufFile pb_file(reader->get_path() + "/data");
    pb_file.load(&s);
    _value.store(s.value());
    return 0;
}
```

三个要点：**快照导出放独立 bthread**（不阻塞状态机）、**快照只含已 commit 状态**（序列化的是应用完日志后的状态机）、**Leader 不加载快照**（`on_snapshot_load` 只发生在落后节点身上）。

### 10.3 Configraft 落地：快照支撑"新节点追赶"与"重启加速"

Configraft 的快照和 counter 同一模式（`src/raft/state_machine.cpp:92` `save_snapshot` / `:130` `on_snapshot_load`），但状态更复杂：`Store::ExportSnapshot` 在**同一个 LevelDB snapshot 下导出 revision + 主索引 + 配置版本索引**，保证 **revision 与数据原子一致**（否则跨节点 revision 编号发散）；`LoadSnapshot` 关闭旧 DB、重建目录、批量写回（`src/store/store.cpp`）。

项目里快照实现两个目的：

1. **在线加节点的追赶**：`AddPeer` 让新节点加入时，braft 会先让它通过快照/日志追平数据，再提交配置变更——快照让"从零开始的节点"不必重放全部历史。
2. **重启加速**：节点重启后直接从最近快照恢复，跳过冗长日志重放。

工程要点：`LoadSnapshot` 持**独占锁**重建 DB（`src/store/store.h:137` 的 `shared_mutex`）——这是 2026-08-20 代码审查发现的 HIGH 级 UAF：快照安装期间 `db_.reset()` 与并发读/Compaction 竞争。修复就是"快照重建持写锁，其余读写持读锁"（详见 [安全审查报告.md](安全审查报告.md) 第 1 项）。

**问题：为什么落后节点能用快照追赶上？** 因为快照包含到 index i 为止的**全部已提交状态**，天然覆盖它缺失的所有日志，不需要日志也在。注意快照只能包含**已 commit** 的状态（快照推进同样遵守 commit 语义），否则会把未确认的写固化进去，破坏安全性。

## 11. 成员变更

### 11.1 概念：为什么成员变更难

**难点**：加/删节点改变的是"多数派"集合。若直接切换配置，新老配置的多数派可能**不相交** → 同时出现两个 leader（破坏选举安全）。

Raft 论文方案：

- **单节点变更**：一次只加/减一个节点。此时新老配置的多数派必然相交，无需额外机制，工程上最常用。
- **Joint Consensus（联合共识）**：一次变更多个节点时，先进入新老配置**共治**阶段（决策需新老两个多数派都通过），再切换到纯新配置。

### 11.2 braft 实现

`Node::add_peer / remove_peer / change_peers`。braft 通常让新节点先以 **learner（学习者）** 身份加入——**只复制日志、不参与投票**，追赶到接近最新后再提升为正式成员，避免空转节点拖慢集群。相关代码在 `src/braft/configuration.cpp`。

### 11.3 Configraft 落地：在线扩缩容

Configraft 用成员变更实现**在线扩缩容**——加节点分担压力、减节点缩容，全程不停服（M6 里程碑）。`RaftNode::AddPeer/RemovePeer`（`src/raft/raft_node.cpp:344`）把 braft 的异步回调包成同步 API：

- 先 `IsLeader()` 校验（仅 Leader 可变更）→ 解析 `PeerId` → `node_->add_peer(...)` → 阻塞在 bthread cv 等待（100ms 粒度轮询实现超时）。
- **超时 ≠ 失败**：`kConfChangeTimeoutSec = 60` 超时后返回的 message 明确提示"change may still be in progress, check health later"——add_peer 内部要先等新节点追平数据（快照/日志，见 §10），这个动作无法撤销。
- **移除 Leader 自身**：braft 会在配置提交后让位（`ELEADERREMOVED`），`remove_peer` 返回成功但本节点随即变成 follower。

**问题：为什么一次只变一个节点最安全？** 因为**两个配置的多数派只要有一个共同节点，就不会同时出现两个法定 leader**。一次只加/减一个节点时，新老配置的多数派至少共享 1 个节点（数学上必然相交），所以不需要 joint consensus 的复杂协议。而如果一次从 3 节点直接换成 5 节点（两个完全不同的集合），新旧多数派可能完全没有交集——旧配置的两个节点 + 新配置的三个节点各自选主，双 leader 就出现了。learner 机制再补一刀：让新节点先不投票，从源头上避免"新配置里半数是空转节点"的选举风险。

## 12. Configraft 项目中的 Raft 落地

这一节把整个项目串起来。Configraft 是一个**基于 Raft 的分布式配置中心**——"集中管理、实时生效、可回滚"的配置服务，是 etcd 在 C++ 技术栈下的精简子集。Raft 在项目里解决的核心问题只有一个：**多节点上的配置数据如何保证强一致**。

### 12.1 项目架构：ConfigNode 抽象 + 双实现

我读项目代码时最先注意到 `src/raft/node.h` 的 `ConfigNode` 抽象接口（`Apply/Get/GetConfig/Watch/Compaction/AddPeer/RemovePeer/...`），它有两个实现：

```
src/raft/node.h           ConfigNode 抽象接口
   ├── src/raft/raft_node.cpp   RaftNode：集群模式，内部持有 braft::Node + 复制状态机
   └── src/raft/local_node.cpp  LocalNode：单机模式，绕过 Raft 直接落库
src/raft/state_machine.cpp      ConfigraftStateMachine：braft::StateMachine 的实现
src/store/store.cpp             Store：LevelDB 封装 + MVCC(状态机背后真正的"状态")
```

服务层（`src/server/`）只面向 `ConfigNode` 编程——**单机/集群切换对上层透明**。`src/server/server.cpp` 按 `--node` 是否为空决定实例化哪个实现；store 由 server 统一打开后移交节点（避免 LevelDB LOCK 重复占用）。`RaftNode::Init`（`src/raft/raft_node.cpp:72`）把 store、fsm、Node 串起来，`NodeOptions.fsm = fsm_.get()`，日志/元数据/快照分别指向 `data_dir/raft/{log,raft_meta,snapshot}`，`snapshot_interval_s = 30`。

**为什么保留 LocalNode 单机模式？** 两种模式共用 `ApplyCmdToStore`（`src/store/store_ops.cpp:122`），**状态变更逻辑只有一份**——这正是"同一复制状态机"的代码体现。LocalNode 是 M1 先跑通全功能的垫脚石，也天然是压测/教学时排除 Raft 干扰的对照组。

### 12.2 各机制的用途对照

| Raft 机制 | Configraft 用它实现 | 关键落点 |
|---|---|---|
| 选举（§5） | 保证单写者：同一时刻只有 leader 接受写 | `IsLeader()` 校验 + `NOT_LEADER` 重定向 |
| 日志复制（§6） | 强一致写：RaftCmd 复制到多数派后 on_apply 串行落库 | `RaftNode::Apply` + `ApplyClosure` |
| 安全性（§7） | 已确认的写不丢：expected_term 防 ABA + 幂等重放 | `task.expected_term` + WriteBatch 原子提交 |
| 日志不一致（§8） | 落后/重启节点自动追平，无需人工干预 | 依赖 braft Replicator，混沌测试验证 |
| 线性一致读（§9） | 强一致读：Leader Lease 保证读到最新已提交写 | `WaitLeaderLease` + `WaitAppliedCatchUp` |
| 快照（§10） | 在线加节点追赶 + 重启加速 | `ExportSnapshot`/`LoadSnapshot` |
| 成员变更（§11） | 在线扩缩容，不停服 | `AddPeer`/`RemovePeer` 同步封装 |
| MVCC/Watch | 项目在复制状态机之上自建：revision 版本链 + Watch 长轮询 | `src/store/` + `src/watch/` |

最后一块拼图是 MVCC 与 Watch：**每写一次，全局 revision +1**，历史版本保留供回滚；Watch 长轮询从复制状态机 apply 产生的事件流里读变更、按 revision 断点续传。这两块虽然不属于 Raft 算法本身，但完全建立在"复制状态机串行 apply"之上——没有 Raft 保证的串行执行，revision 的"读-改-写"就无法原子。

### 12.3 代码地图

```
src/raft/node.h             ConfigNode 抽象接口
src/raft/raft_node.cpp      RaftNode：Apply/读路径/成员变更/元信息（braft 接入层）
src/raft/state_machine.cpp  ConfigraftStateMachine：on_apply/快照（复制状态机）
src/raft/local_node.cpp     LocalNode：单机模式（同步执行 + 写锁串行化）
src/store/store.cpp         Store：LevelDB + MVCC + Compaction + 快照导入导出
src/store/store_ops.cpp     ApplyCmdToStore：RaftCmd 分发到 Store 各写方法
src/watch/watch_hub.cpp     WatchHub：长轮询分发（订阅/重放/背压）
src/server/*                brpc 服务（KV/Config/Watch/Admin/Dashboard）
```

## 13. 学习路线与资源

按我的学习顺序走，顺序不能乱：

1. **论文**：《In Search of an Understandable Consensus Algorithm》（Ongaro & Ousterhout），中文翻译版 + 英文对照读。边读边在纸上画 term / 日志结构。
2. **交互演示**：raft.github.io 动图。亲手模拟选举、日志复制，直到能**预判下一步**再点下一步。
3. **braft 官方文档**（baidu.github.io/braft）：overview → server → client → cli → replication。
4. **示例精读**：回到本文第 4 节，跑一遍 `example/counter` 的 server 和 client，打断点看一次 apply 的全链路。
5. **源码精读**：配合第 4 节与 §5–§11 的"braft 实现"，只读关键路径，不逐行。
6. **项目代码**：对照第 12 节代码地图，把各节的"问题"在 Configraft 源码里找到对应落点，再回到本节。

**自检标准**：能不看资料，把每节末尾的"问题"用自己的话讲清楚，并能画出第 4.1 节的架构图与第 12.3 节的代码地图 → 这一轮过关。

------

*笔记维护记录：2026-08-13 创建；2026-08-25 重构为第一人称学习笔记，按"概念 → braft 源码精读 → Configraft 落地 → 问题"组织，新增 counter 示例精读，删除面试速查表。*
