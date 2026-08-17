# 常见问题 FAQ

## 设计决策

### 为什么基于 braft 而不自研 Raft？
Raft 算法是时间黑洞，自研容易在选举/日志一致性上踩坑且难验证。选择在百度工业级 braft 之上做应用层（MVCC/版本/Watch/CAS/成员变更），深度通过**精读 braft 源码**获得，项目价值聚焦"配置中心的业务语义"而非重复造轮子。这符合项目边界：**复制状态机应用层**是我们的深度，Raft 算法本身用成熟库。

### 为什么选 LevelDB 而非 RocksDB？
LevelDB 代码精炼（~1 万行），适合精读 LSM-Tree 原理；RocksDB 是 LevelDB 的超集，功能多但复杂度高。配置中心的单机存储需求 LevelDB 足够，且 LevelDB 单线程写与 Raft 串行 apply 天然匹配。

### Watch 为什么用 HTTP 长轮询而非 gRPC 流 / WebSocket？
brpc 的 gRPC 流式 RPC 支持有限（流控/背压复杂），WebSocket 引入额外协议栈。HTTP 长轮询简单可靠：客户端携带 `from_revision` 轮询，服务端有事件立即返回、无事件阻塞至超时；revision 既是续传锚点又是断点续传的语义核心。与 etcd 的 v2 方案同源。

### 为什么回滚产生新版本而不删除历史？
Raft 日志是 append-only，删除历史会破坏日志与状态机的对应关系。回滚 = 读目标版本值写回产生**新版本**（etcd 语义），历史版本仍可查。副作用是存储增长，由 Compaction 定期回收解决。

### 线性一致读为什么用 leader lease 而非 ReadIndex？
braft v1.1.2 无 read_index API（源码确认）。改用内置 leader lease：`LEASE_VALID ⟹ 本任期首条配置日志（=no-op）已提交 ⟹ commit 覆盖先前任期已提交写 ⟹ 本地读线性一致`，再叠加 `applied≥commit` 屏障补齐异步 apply。读路径=WaitLeaderLease + WaitAppliedCatchUp，代价是 leader 变更后新 leader 需一个心跳窗口才能提供读。

### CAS 原子性怎么保证？
CAS 作为一条 Raft 指令走 on_apply 串行"比较-写入"，读-比较-写在同一串行点原子完成；失败不消耗 revision（不产生无谓事件）。并发 CAS 恰一成功由单节点 braft 组测试验证。

### 成员变更怎么保证安全？
基于 braft `add_peer/remove_peer`：新节点**先追平数据（快照/日志）再进配置**，防止落后节点以缺失日志参与投票破坏安全性；移除 Leader 自身时 braft 自动让位重新选主。单成员变更跳过 joint consensus（braft 兼容 legacy 单阶段）。

### 为什么健康检查用 `/healthz` 而不是 `/health`？
brpc 1.17 内置 HealthService 已占用 `/health`（restful 映射首段注册为 service 短名会与其冲突，Server 启动失败）。`/healthz` 是 k8s liveness 惯例，语义一致。

## 使用

### 如何一键启动 3 节点集群？
```bash
# 本机进程集群（端口 8001-8003）
./scripts/run_cluster.sh

# Docker Compose 一键集群（固定 IP 172.28.0.11-13）
docker compose up -d --build
```

### 如何在线扩容？
新节点以空配置启动（`--peers ""`），在 leader 上执行：
```bash
curl -X POST -d '{"peer":"127.0.0.1:8004"}' http://<leader>:8001/addpeer
```
新节点自动经快照/日志追平数据后加入。

### 压测数据如何？
3 节点集群（4 核 VM）：gRPC 写 QPS ~2.9k（默认 fsync，P99 5ms）；关闭 fsync 后 ~6.5k（牺牲 Raft 持久性，仅演示）。HTTP 读 ~110k QPS。详见 [docs/m7.md](docs/m7.md)。

### 为什么默认开 fsync 后写吞吐下降？
`FLAGS_raft_sync=true` 每次写日志 fsync（Raft 持久性保证：节点崩溃不丢已提交日志），是写路径最大开销。这是"性能 vs 持久性"的经典权衡（etcd 同样面对），生产默认开。

### 有哪些已知局限？
- `raft_sync=false` 牺牲持久性（崩溃可能丢已提交日志），仅演示用。
- 线性一致读依赖 leader lease，leader 刚当选时需等待 lease 就绪（短窗口）。
- Watch 落在追赶中的节点时会先等追平（≤5s）或返回重试，避免重放旧事件。
