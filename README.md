<div align="center">

# Configraft

**基于 Raft 共识的分布式配置中心 —— 强一致 · 实时生效 · 可回滚**

C++17 · braft · brpc · LevelDB · Protobuf · Docker

[![CI](https://github.com/ranbai7/configraft/actions/workflows/ci.yml/badge.svg)](https://github.com/ranbai7/configraft/actions/workflows/ci.yml)
[![Language](https://img.shields.io/badge/C%2B%2B-17-blue)](#)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](#)

</div>

---

## 简介

Configraft 是一个轻量级、**强一致**的分布式键值存储系统，以**分布式配置中心**的形态对外提供服务。它基于百度的工业级 C++ Raft 实现 [braft](https://github.com/baidu/braft) 保证多节点数据一致，使用 [LevelDB](https://github.com/google/leveldb) 作为单机存储引擎，并由 [brpc](https://github.com/apache/brpc) 在**同一端口**提供 **gRPC + HTTP** 双协议接口与 Raft 内部通信。

设计目标是为微服务提供"**集中管理、实时生效、可一键回滚**"的配置服务，是 etcd 在 C++ 技术栈下的一个精简子集实现。

### 解决什么问题

在微服务架构中，配置散落在各服务的本地文件或环境变量里，修改配置往往需要重启服务或手动分发，低效且易错。Configraft 提供：

- **集中管理**：所有配置项统一存储、统一发布。
- **实时生效**：配置变更后通过 Watch 长轮询立即推送到所有订阅方，**零停机生效**。
- **强一致**：借助 Raft 共识，配置发布不因节点宕机而丢失或不一致；读取默认线性一致。
- **安全回滚**：每次修改保留历史版本（MVCC），可随时一键回滚到任意历史版本。

---

## 特性

- ✅ **基础 KV**：`Put / Get / Delete / BatchPut`（gRPC + HTTP）
- ✅ **配置管理**：多版本发布 `Publish`、历史查询 `GetConfig(key, version)`、一键回滚 `Rollback`
- ✅ **实时监听**：HTTP 长轮询 `Watch`，按全局 revision 有序推送、断点续传、慢消费者背压断开
- ✅ **原子操作**：`CompareAndSwap`（CAS），基于 Raft 串行 apply 天然原子
- ✅ **高可用**：3 节点集群容忍 1 节点宕机，Leader 自动选举，故障数秒内恢复
- ✅ **在线扩缩容**：动态 `AddPeer / RemovePeer` 成员变更，新节点经快照追赶加入
- ✅ **MVCC + Compaction**：全局单调 revision 版本模型，自动回收过期版本，存储不无限膨胀
- ✅ **强一致读**：Leader Lease 线性一致读，杜绝读到过期配置（braft 无 ReadIndex，采用 Lease 方案）
- ✅ **可视化管理**：内置 Web Dashboard，节点状态 / 操作面板 / Watch 事件流一屏尽览
- ✅ **运维友好**：健康检查、Docker 一键启动 3 节点集群、一键停止脚本
- ✅ **混沌可验证**：kill Leader / 网络分区 / 节点重启故障演练脚本

---

## 架构

```
┌───────────────────────────────────────────────────────────────┐
│                    客户端 / SDK（跨语言）                        │
│   gRPC(HTTP/2)   HTTP(RESTful)   HTTP(长轮询 Watch)   Dashboard│
└───────────────┬───────────────────────────────────────────────┘
                │              brpc 单端口多协议
┌───────────────▼───────────────────────────────────────────────┐
│             API / 服务层（brpc Server）                         │
│  KVService  ConfigService  WatchService  AdminService  Dashboard│
└───────────────┬───────────────────────────────────────────────┘
┌───────────────▼───────────────────────────────────────────────┐
│             Raft 层（braft）                                    │
│  选主 / 日志复制 / Leader Lease 读 / 成员变更 / Snapshot / 故障转移│
└───────────────┬───────────────────────────────────────────────┘
┌───────────────▼───────────────────────────────────────────────┐
│             复制状态机（串行 on_apply）                         │
│  MVCC 写入 · 版本管理 · CAS · Watch 事件 · Compaction          │
└───────────────┬───────────────────────────────────────────────┘
┌───────────────▼───────────────────────────────────────────────┐
│             存储层（LevelDB，每节点一份）                        │
│  KV 数据(MVCC 多版本) · revision 索引 · 墓碑 · Raft 元数据       │
└───────────────────────────────────────────────────────────────┘
```

### 数据流

- **写**：客户端 → Leader API → `node->apply(task)` → 多数派确认并写日志 → commit → `on_apply` 串行写入 LevelDB（产生新 revision 与 Watch 事件）→ 返回。
- **读（线性一致）**：客户端 → Leader API → 校验 Leader Lease 有效 → 等 applied ≥ commit → 从本地状态机读取 → 返回。
- **Watch**：客户端携带 `from_revision` 发起长轮询 → 挂起 → 变更事件到达后立即推送 → 用新 revision 再次请求。

> **Leader Lease 线性一致读**：braft 1.1.2 未提供 ReadIndex API，本项目利用 braft 内置 leader lease 实现线性一致读。Lease 有效 ⟹ Leader 已提交配置条目（no-op）⟹ commit 覆盖先前任期的所有已提交写 ⟹ 本地读结果与任一历史 Leader 的线性顺序一致。实现详见 [docs/m5.md](docs/m5.md)。

---

## 技术栈

| 层级 | 选型 |
|------|------|
| 语言 | C++17 |
| 构建 | CMake + [scripts/build_deps.sh](scripts/build_deps.sh)（源码构建第三方依赖） |
| 一致性协议 | [braft](https://github.com/baidu/braft)（工业级 C++ Raft，v1.1.2） |
| 网络框架 | [brpc](https://github.com/apache/brpc)（v1.17，单端口 HTTP + gRPC + Raft 内部通信） |
| 存储引擎 | [LevelDB](https://github.com/google/leveldb) |
| 序列化 | Protobuf3（protobuf 3.21.12） |
| 测试 | GoogleTest（store / watch / raft_cas）+ 混沌脚本 |
| 压测 | ghz（gRPC）/ wrk（HTTP），[scripts/benchmark.sh](scripts/benchmark.sh) |
| CI/CD | GitHub Actions（构建 + 单元测试，缓存第三方依赖） |
| 部署 | Docker / Docker Compose |

---

## 快速开始（Docker）

### 前置条件

- Docker + Docker Compose v2

### 一键启动 3 节点集群

```bash
git clone https://github.com/ranbai7/configraft.git
cd configraft
docker compose up -d --build
```

启动后三个节点分别暴露宿主端口 `8001 / 8002 / 8003`。写请求请打到 **Leader**（通常为 `node1`，可通过健康检查确认）。

### 健康检查

```bash
# 返回节点角色 / term / commit / applied 与 leader 地址
curl http://127.0.0.1:8001/healthz
# {"message":"ok","role":"leader","term":1,"commit_index":2,"applied_index":2,"leader_id":"172.28.0.11:8001:0"}
```

### HTTP 读写配置

```bash
# 写入（发布配置，自动递增版本）
curl -X POST http://127.0.0.1:8001/v1/kv/gateway.rate_limit \
  -d '{"value":"2000"}'

# 读取（默认线性一致）
curl http://127.0.0.1:8001/v1/kv/gateway.rate_limit

# 回滚到历史版本（写回旧值，产生新版本）
curl -X POST http://127.0.0.1:8001/v1/config/gateway.rate_limit/rollback \
  -d '{"target_version":1}'
```

### Watch 实时监听

```bash
# 监听全部 key 的变更（长轮询，收到变更即返回，然后携带新 revision 重新请求）
curl "http://127.0.0.1:8001/v1/watch?from_revision=0"

# 监听单个 key（/v1/watch/{key}）；from_revision 为续传锚点
curl "http://127.0.0.1:8001/v1/watch/gateway.rate_limit?from_revision=0"
```

> `from_revision = 0` 表示"从现在开始看"（不重放历史）；重放历史请携带 `> 0` 的 revision 锚点。

### gRPC 调用

```bash
grpcurl -proto proto/configraft.proto -plaintext 127.0.0.1:8001 \
  configraft.v1.KVService/Get -d '{"key":"gateway.rate_limit"}'

# 压测（ghz）
ghz --insecure --proto proto/configraft.proto \
  --call configraft.v1.KVService/Put \
  -d '{"key":"k","value":"v"}' -z 10s -c 100 \
  127.0.0.1:8001
```

### Web Dashboard

浏览器打开 `http://127.0.0.1:8001/dashboard`，可查看集群节点状态卡片、执行 KV / Config / CAS / 成员操作，并实时观察 Watch 事件流（配置变更 → 推送 → 订阅端）。详见 [docs/Dashboard操作指南.md](docs/Dashboard操作指南.md)。

---

## 核心 API

### KV

| 方法 | HTTP 路径 | 说明 |
|------|-----------|------|
| `Put(key, value)` | `POST /v1/kv/{key}` | 写入（body: `{"value":"..."}`） |
| `Get(key)` | `GET /v1/kv/{key}` | 读取，默认线性一致；`?serializable=true` 允许 Follower 读 |
| `Delete(key)` | `DELETE /v1/kv/{key}` | 删除（写 tombstone） |
| `BatchPut(entries)` | `POST /v1/kv/batch` | 批量写入（body: `{"entries":[{"key","value"},...]}`） |
| `CompareAndSwap(key, expect, value)` | `POST /v1/kv/{key}/cas` | 条件更新（body: `{"expect","value"}`），原子 |

### 配置管理

| 方法 | HTTP 路径 | 说明 |
|------|-----------|------|
| `Publish(key, value)` | `POST /v1/config/{key}` | 发布新版本 |
| `GetConfig(key, version)` | `GET /v1/config/{key}?version=N` | 读指定版本；version 缺省/≤0 为最新 |
| `Rollback(key, target_version)` | `POST /v1/config/{key}/rollback` | 一键回滚（写回旧值，产生新版本） |

### 监听与运维

| 方法 | HTTP 路径 | 说明 |
|------|-----------|------|
| `Watch(key, from_revision)` | `GET /v1/watch`（全部）/ `GET /v1/watch/{key}` | 长轮询变更推送，断点续传 |
| `GetHealth()` | `GET /healthz` | 节点角色 / term / commit / applied |
| `AddPeer(peer)` | `POST /addpeer` | 在线加节点（body: `{"peer":"ip:port"}`） |
| `RemovePeer(peer)` | `POST /removepeer` | 在线移除节点 |

> **错误码**：`0` OK · `1` KEY_NOT_FOUND · `2` CAS_FAILED · `3` NOT_LEADER（响应含 leader 地址，客户端应重定向）· `4` VERSION_NOT_FOUND · `5` COMPACTED（历史已回收，用 current_revision 续传）· `6` INVALID_ARGUMENT · `100` INTERNAL。
>
> gRPC 方法路径：`configraft.v1.KVService/{Put,Get,Delete,BatchPut,CompareAndSwap}`、`configraft.v1.ConfigService/{Publish,GetConfig,Rollback}`、`configraft.v1.WatchService/Watch`。AdminService 三方法（`GetHealth/AddPeer/RemovePeer`）因 brpc restful 映射占用 gRPC 路径，仅提供 HTTP（见 [docs/m6.md](docs/m6.md)）。

---

## 一致性保证

| 操作 | 一致性 | 实现 |
|------|--------|------|
| 所有写操作 | **线性一致** | Raft 多数派确认后 `on_apply` 串行落库 |
| `Get`（默认） | **线性一致** | Leader Lease 校验 + applied ≥ commit 后本地读 |
| `Get`（`serializable=true`） | 可容忍轻微过期 | Follower 本地读，用于监控 |
| `Watch` | **有序、不丢、可续传** | 事件来自 Raft apply 序列，按全局 revision 排序；历史回收时返回 `COMPACTED` + 新锚点 |

---

## 目录结构

```
configraft/
├── CMakeLists.txt
├── Dockerfile / docker-compose.yml    # 3 节点集群一键启动
├── proto/configraft.proto             # gRPC / Protobuf 协议定义
├── src/
│   ├── server/                        # brpc 服务（KV/Config/Watch/Admin/Dashboard + CORS）
│   ├── raft/                          # ConfigNode 抽象、RaftNode/LocalNode、复制状态机、快照
│   ├── store/                         # LevelDB 封装、MVCC、Compaction、快照导入导出
│   ├── watch/                         # WatchHub 长轮询分发（订阅/重放/背压）
│   └── common/                        # storekey 编码、状态码、日志
├── client/                            # 示例客户端（client_main.cpp）
├── scripts/                           # build_deps / run_cluster / chaos_test / benchmark
├── tests/                             # GoogleTest（store / watch / raft_cas）
├── web/                               # Dashboard 前端静态资源（原生 JS）
└── docs/                              # 架构 / 里程碑 / 压测 / 安全审查等文档
```

---

## 本地开发

### 1. 构建第三方依赖（brpc / braft / protobuf）

```bash
bash scripts/build_deps.sh
```

脚本幂等构建到 `third_party/install`（约 148MB），含项目定制补丁（braft 构建修复等）。

### 2. 构建与测试

```bash
cmake -B build
cmake --build build -j4
ctest --test-dir build          # 3 个测试：store / watch / raft_cas
```

### 3. 运行

```bash
# 单机模式（LocalNode，无 Raft，用于功能调试）
./build/configraft_server --port 8100 --data_dir /tmp/cfg-m1

# 3 节点集群（端口 8001-8003）
bash scripts/run_cluster.sh
bash scripts/run_cluster.sh stop      # 一条命令停止集群
```

> 单机模式不启动 Raft，可快速验证 KV/Config/Watch 全部功能；集群模式才提供一致性保证与故障转移。

---

## 性能与验证

### 压测数据（2026-08 云服务器实测）

> **测试环境**：阿里云云主机 · 8 vCPU / 14G 内存 / NVMe SSD · Ubuntu 22.04 · 多节点进程同机（本地环回）· ghz（gRPC）/ wrk（HTTP）· 每项 10s。

| 场景（每项 10s） | 单机 | 3 节点 · 默认持久化 | 3 节点 · 关 fsync | 5 节点 · 默认持久化 | 5 节点 · 关 fsync |
|---|---|---|---|---|---|
| gRPC 写 Put · 8 并发 | 28.7k / 0.64ms | 1.8k / 10.3ms | 15.9k / 1.4ms | 1.2k / 11.0ms | 13.0k / 1.7ms |
| gRPC 写 Put · 64 并发 | 41.8k / 3.1ms | 10.0k / 11.6ms | 28.2k / 4.7ms | 7.4k / 14.7ms | 24.4k / 5.8ms |
| gRPC 写 Put · 128 并发 | 38.1k / 6.1ms | 16.0k / 13.0ms | 31.0k / 7.1ms | 12.6k / 18.9ms | 27.3k / 8.8ms |
| gRPC 读 Get · 64 并发 | 38.6k / 3.6ms | 45.2k / 2.8ms | 42.5k / 3.3ms | 44.1k / 3.0ms | 40.5k / 4.1ms |
| HTTP 读 GET · 100 连接 | 259k / 0.44ms† | 275k / 0.48ms | 271k / 0.48ms | 267k / 0.50ms | 263k / 0.51ms |
| HTTP 写 POST · 100 连接 | 102k / 47ms† | 17.9k / 12.6ms | 75.4k / 4.2ms | 13.0k / 15.5ms | 50.4k / 1.33s† |

> ① 数据为 2026-08-20 阿里云实测（8 vCPU / 14G / NVMe SSD / Ubuntu 22.04，多节点进程同机环回），为**审查加固后（commit e5d737b 加锁）当前代码**实测；加锁前后同环境对比差异全部在 ±5% 内（详见 [docs/性能压测报告.md](docs/性能压测报告.md) §6）。
> ② `†` 单机 fsync=true 组 / 5 节点关 fsync 组的 HTTP P99 受同机压测 CPU 资源竞争影响偏高（读路径与 fsync 无关），QPS 仍稳定；同项在另一 fsync 配置下恢复正常。

**关键结论**：

1. **读远高于写**：HTTP 读 25–30 万 QPS vs 写 1.2k–31k——符合配置中心"读多写少"业务形态。
2. **Raft 复制规模代价**：5 节点写吞吐比 3 节点低 15–32%（多数派 2/3 → 3/5），读路径几乎不受影响（Leader Lease 本地读）；代价换来容错升级（3 节点容忍 1 故障、5 节点容忍 2 故障）。
3. **fsync 是写路径最大开销**：默认持久化 vs 关 fsync 相差 8–10 倍——"强一致 + 崩溃不丢已提交日志"的固有代价（`--raft_sync=false` 仅压测/演示用）。
4. **写路径峰值**（8 核 CPU 上限）：单机约 38k、3 节点关 fsync 约 31k、5 节点关 fsync 约 27k QPS（c64→c128 已趋饱和）。

**审查加固后复测（2026-08-20，锁开销验证）**：安全审查（commit e5d737b）引入 Store 共享互斥量与单机写锁后，在同环境（8 核阿里云）复测完整矩阵：**集群写路径 QPS 差异全部在 ±5% 内，读路径完全不受影响**——共享锁修复快照 UAF 的加固零性能代价。对比表与原始证据（环境指纹 + 二进制 md5 + 逐项原始输出）见 [docs/性能压测报告.md](docs/性能压测报告.md) §6。

> 完整数据与复现方式见 [docs/性能压测报告.md](docs/性能压测报告.md)；`bash scripts/benchmark.sh --all --dur 10` 可一键复跑完整矩阵。

### 验证

- **混沌测试**：`scripts/chaos_test.sh` 执行 kill Leader / kill Follower / 网络分区 / 节点重启演练，验证写线性一致、读一致、Watch 有序不丢、集群自动恢复。
- **单测**：`ctest --test-dir build` 覆盖 MVCC 语义、Watch 有序/背压、CAS 并发原子性（含"失败不消耗 revision"断言）。
- **性能分析**：写路径热点（fsync）与优化过程详见 [docs/m7.md](docs/m7.md)。

---

## 代码审查与安全加固

2026-08-20 完成一次**全项目代码审查**（Raft 核心 / 服务层 / 存储 / 前端 4 个维度并行），发现并修复 **9 项**问题（commit `e5d737b`），主要包括：

- **快照并发 UAF**（HIGH）：`LoadSnapshot` 重建 LevelDB 时与并发读/Compaction 无同步 → 引入共享互斥量保护存储生命周期
- **单机模式写并发**（HIGH）：`LocalNode` 并发写导致全局 revision 重复 → 写路径串行化
- **通配 CORS**（HIGH）：允许任意网页跨站读写 → 收紧为仅同主机跨端口放行
- 快照 revision 原子性、快照含配置版本索引、层级 key 历史碰撞、写失败原子提交、key 校验、关闭 brpc 内置服务面板等

完整发现清单（含未修项与设计取舍）见 [docs/安全审查报告.md](docs/安全审查报告.md)。

---

## 安全模型

> 当前版本**未内置鉴权**，面向受信任内网 / 单机演示场景。请勿将端口直接暴露到不可信网络；生产部署请在网关注入鉴权或做网络隔离。

已采取的缓解措施：

- **CORS 收紧**：`Access-Control-Allow-Origin` 仅回显同主机来源，阻断恶意网页跨站读写（配置 / 成员 / 数据）。
- **关闭内置服务面板**：brpc `/status` `/vars` 等内部监控页已禁用，避免泄露运行参数。
- **参数校验**：拒绝空 key（`INVALID_ARGUMENT`）。

---

## 文档

| 文档 | 内容 |
|------|------|
| [docs/开发计划.md](docs/开发计划.md) | 里程碑规划、简历素材与面试题 |
| [docs/m5.md](docs/m5.md) / [docs/m6.md](docs/m6.md) / [docs/m7.md](docs/m7.md) | M5 线性读与 CAS、M6 成员变更、M7 性能优化 |
| [docs/watch.md](docs/watch.md) / [docs/LevelDB.md](docs/LevelDB.md) / [docs/raft一致性算法.md](docs/raft一致性算法.md) | 核心机制深入 |
| [docs/Dashboard操作指南.md](docs/Dashboard操作指南.md) | Dashboard 逐功能操作与现象解释 |
| [docs/演示脚本.md](docs/演示脚本.md) | 完整功能演示清单 |
| [docs/性能压测报告.md](docs/性能压测报告.md) | 云服务器压测原始数据与分析 |
| [docs/安全审查报告.md](docs/安全审查报告.md) | 全项目代码审查：已修复项 + 未修项 + 设计取舍 |
| [docs/面试准备.md](docs/面试准备.md) | 简历描述 + 30 分钟讲解稿 + 高频面试题 |
| [FAQ.md](FAQ.md) / [CONTRIBUTING.md](CONTRIBUTING.md) | 常见问题 / 贡献指南 |

---

## 已完成里程碑（M0–M8）

| 里程碑 | 内容 | 状态 |
|--------|------|------|
| M0 | 项目骨架：CMake / 单元测试框架 / 代码规范 | ✅ |
| M1 | 单机 KV：brpc + LevelDB + Protobuf（`LocalNode`） | ✅ |
| M2 | Raft 接入：braft 3 节点写入一致 + 自动选主 | ✅ |
| M3 | MVCC 版本模型：`Publish` / `GetConfig` / `Rollback` + Compaction | ✅ |
| M4 | Watch 长轮询 + revision 断点续传 + 背压 | ✅ |
| M5 | CAS 原子操作 + Leader Lease 线性一致读 | ✅ |
| M6 | 成员变更（AddPeer/RemovePeer）+ Docker 一键集群 + 健康检查 | ✅ |
| M7 | 性能优化（bthread 原语修复、fsync 热点分析）+ 混沌测试 + 压测 | ✅ |
| M8 | 全项目代码审查与安全加固（9 项修复） | ✅ |

**后续方向**：接口鉴权（令牌 / IP 白名单）、HTTP 路由层集成测试、apply 幂等（崩溃重放防护）、快照安装与读并发压测。

---

## 致谢

- [braft](https://github.com/baidu/braft) —— 工业级 C++ Raft 实现
- [brpc](https://github.com/apache/brpc) —— 高性能多协议 RPC 框架
- [LevelDB](https://github.com/google/leveldb) —— 嵌入式 LSM-Tree 存储引擎
- [etcd](https://github.com/etcd-io/etcd) —— 分布式 KV/配置中心设计参照

## License

[Apache License 2.0](LICENSE)
