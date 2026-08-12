# M4 Watch：基于 HTTP 长轮询的实时变更推送

> 配套：`开发计划.md` M4 章节。本文记录设计决策与实现要点，是对面试官"你是不是只会调库"的应答素材。

## 一句话定位

配置中心的灵魂是"**改了立刻生效**"：客户端不轮询、不查库，而是**长轮询**订阅某个 key（或全部），配置一变更就收到带 `revision` 的有序事件流；断网/重连后携带 `revision` 续传，**不丢、不重**。

## 为什么长轮询，而非 gRPC 流 / WebSocket？

| 方案 | 结论 |
|---|---|
| **HTTP 长轮询** | ✅ 实现简单、天然无状态、超时后自动重连、可被任意负载均衡；事件本来就低频，等待成本低 |
| gRPC 流式 RPC | brpc 的流式 RPC 需要额外处理流控/半关闭/连接复用语义，对"低频事件 + 高频重连"场景过重 |
| WebSocket / 推送 | 需要维持长连接与心跳，服务端状态复杂；长轮询的服务端只需"挂起一个请求" |

**本质**：Watch 是**低频、强一致锚点**的场景——客户端能容忍最长 `timeout_ms` 的等待，但不能容忍丢事件。长轮询把"推"简化为"客户端主动拉 + 服务端阻塞到有新数据"，用 `revision` 做对账锚点，是最匹配的。

## 整体架构与一次数据流

```
[Client]  GET /v1/watch/limit?from_revision=100&timeout_ms=30000
              │
              ▼
[WatchServiceImpl] ──► [ConfigNode::Watch] ──► [WatchHub] ──┐
                                                  │          │
   ① 重放历史: Store.ReplayEvents(100, now]          │  注册 Waiter（key 过滤+缓冲）
     （LevelDB v/ 前缀，断点续传基石）               ▼
   ② 无事件则挂起 bthread 等待                   [Waiter 队列]
                                                  ▲
   ③ on_apply 广播: 状态机 apply 一条日志 ──►  Broadcast(事件)  ──► 匹配的 Waiter
     （Leader/Follower 都广播）                                   push + notify_all
```

1. **历史重放**（断点续传）：MVCC 已经把每次写入持久化在 `v/{rev:16hex}/{key}`，**事件无需额外存储**——直接扫描该区间即可还原事件流，重启/换节点后不丢。
2. **实时事件**：`on_apply` 串行 apply 每条日志后，把该指令产生的事件广播给所有匹配的活跃 Waiter。
3. **两路合一**：先注册（进入广播可见集）再重放历史，关闭"已提交历史"与"新事件"之间的窗口；两者以 `revision` 无缝衔接。

## 断点续传：revision 锚点

- **请求** `from_revision`：返回 `(from_revision, now]` 开区间的事件，升序。
- **响应** `current_revision`：**最后一条事件的 revision**（无事件时是当前集群 revision）。客户端下一轮 `from_revision = current_revision`。
- **为什么锚点取"最后事件 rev"而非集群最新 rev？** 若返回的事件只到 105、集群已到 107，锚点填 107 会让客户端从 107 续传而**漏掉 106/107**。
- **`from_revision = 0`**：表示"从现在开始看"，不重放历史（新订阅者的默认语义）。

### 事件从哪里来？（关键：零额外持久化）

所有写操作（Put/Delete/Publish/Rollback/BatchPut）在 `on_apply` 中都产生事件，同时写入 `v/` 历史。于是：
- **历史事件** = `ReplayEvents` 扫描 `v/` 前缀，按 `kv.deleted()` 区分 PUT/DELETE；
- **实时事件** = `on_apply` 广播（本节点提交的日志和复制来的日志**都广播**，所以 Watch 可落在任意节点）。

### Compacted：历史被回收怎么办？

Compaction 回收旧版本后，`(from_revision, now]` 可能有洞。判定：

```
compact_rev = max(所有被 Compaction 删除的 v/ 记录 revision)
from_revision < compact_rev  ⟹  返回 COMPACTED（客户端被迫从 current_revision 重建）
```

**为什么这个判定是精确的？** 所有被删记录 rev ≤ compact_rev，故 `from ≥ compact_rev` ⟺ 重放区间内没有洞 ⟺ 重放完整。它与删除操作放进**同一个 LevelDB WriteBatch**，读侧"先建迭代器快照、后读 compact_rev"，杜绝"读到不完整重放却没报 COMPACTED"的竞态。

## 背压：慢消费者处理

每个 Waiter 有事件缓冲上限（1024 条）。`Broadcast` 时若缓冲满：
- 置 `overflow`、清空缓冲、断开该连接；
- 客户端收到 `INTERNAL` + `current_revision`，用该 revision 重连，经**历史重放**补齐。

**核心收益**：慢消费者**绝不阻塞 on_apply**（串行 apply 是 Raft 正确性前提）。缓冲把"生产-消费"解耦：事件一进缓冲就安全，即使存储随后回收也不丢。

## 并发设计（实现要点）

- **锁序单向** `hub_mu_ → waiter->mu`：`Broadcast` 先拷出 weak_ptr 快照放锁，再逐个 `lock()` + 推事件 + `notify_all`，杜绝 ABBA 死锁。
- **Waiter 生命周期**：调用方（长轮询 bthread）持 `shared_ptr`，hub 只存 `weak_ptr`；**先 Unregister 再释放强引用**，广播侧 `lock()` 失败即跳过——绝无 UAF。
- **无丢唤醒**：先 push 事件（持 `waiter->mu`）后 `notify_all`；等待循环每次醒来重查谓词，`wait_until` 仅对 `ETIMEDOUT` 特判退出，其余视为伪唤醒。
- **必须用 bthread 同步**（`bthread::Mutex`/`ConditionVariable`）做长等待：`wait_until` 挂起的是 bthread、释放 pthread worker。若用 `std::condition_variable`，4 个并发长轮询就会占满 `num_threads=4` 的 worker 把服务饿死。
- **Watch 不强制 Leader**：每节点本地事件流随副本 apply 推进，Follower 也能服务 Watch（实测：Follower 上的 watch 能收到 Leader 提交后复制过来的事件）。

## 与 etcd Watch 的对照

| 维度 | etcd | configraft |
|---|---|---|
| 协议 | gRPC 流式 Watch | HTTP 长轮询 |
| 断点续传 | `watch(key, rev)` + compact_rev | `from_revision` + compact_rev |
| 历史事件 | 由 mvcc 存储回放 | 由 MVCC `v/` 前缀回放 |
| 慢消费者 | eventBuf 溢出返回 error | 缓冲上限 → overflow 断开重连 |
| 一致性 | watch 可在任意节点，rev 对齐 | 同（不强制 Leader） |

## 实测验证（2026-08-12）

单机 + 3 节点集群均验证通过：

```bash
# 断点续传：from_revision=1 重放 foo 的历史（limit 的 rev 3 被 key 过滤）
curl "127.0.0.1:8100/v1/watch/foo?from_revision=1&timeout_ms=2000"
# {"current_revision":2,"events":[{"revision":2,"key":"foo","value":"bar2","type":"PUT"}]}

# 实时长轮询：挂起 8s 的连接，写入后 1s 内收到事件（而非等满超时）
curl "127.0.0.1:8100/v1/watch/foo?from_revision=3&timeout_ms=8000"   # 后台挂起
curl -X POST 127.0.0.1:8100/v1/kv/foo -d '{"value":"bar3"}'          # 写入
# {"current_revision":4,"events":[{"revision":4,"key":"foo","value":"bar3","type":"PUT"}]}

# 监听全部 key + DELETE 类型 + 多事件顺序
curl "127.0.0.1:8100/v1/watch?from_revision=3&timeout_ms=2000"
# rev 4 foo PUT, rev 5 m1 PUT, rev 6 m2 PUT, rev 7 foo DELETE

# 集群：Follower(8001) 上的 watch 收到 Leader(8003) Publish 的事件
# 历史被 Compaction 回收后，旧 from_revision 返回 code=5 (COMPACTED)
```

## 简历素材

> **"实现基于 HTTP 长轮询的 Watch 实时推送，以 revision 断点续传保证事件有序不丢，含慢消费者背压处理。"**

## 面试题（自测）

1. 为什么长轮询而非 gRPC 流 / WebSocket？
2. 断点续传怎么保证不丢不重？锚点为什么用"最后事件 rev"而不是集群最新 rev？
3. 事件从哪里来？为什么不需要额外持久化事件？
4. 慢消费者怎么处理？为什么不能让 on_apply 被阻塞？
5. 事件顺序怎么保证？广播为什么用 `notify` 在 push 之后？
6. `compact_rev` 判定为什么是精确的？
7. 为什么 Waiter 要用 bthread 同步而非 std 同步？长轮询会饿死服务吗？
8. Watch 为什么不强制落在 Leader？Follower 上 Watch 的语义是什么？
