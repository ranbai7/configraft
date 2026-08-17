<div align="center">

# Configraft

**一个基于 Raft 共识的分布式配置中心 —— 强一致 · 实时生效 · 可回滚**

C++17 · braft · brpc · LevelDB · Protobuf · Docker

[![CI](https://github.com/ranbai7/configraft/actions/workflows/ci.yml/badge.svg)](https://github.com/ranbai7/configraft/actions/workflows/ci.yml)
[![Language](https://img.shields.io/badge/C%2B%2B-17-blue)](#)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](#)

</div>

---

## 简介

Configraft 是一个轻量级、**强一致**的分布式键值存储系统，以**分布式配置中心**的形态对外提供服务。它基于百度的工业级 C++ Raft 实现 [braft](https://github.com/baidu/braft) 保证多节点数据一致，使用 [LevelDB](https://github.com/google/leveldb) 作为单机存储引擎，并由 [brpc](https://github.com/apache/brpc) 在同一端口提供 **gRPC + HTTP** 双协议接口。

设计目标是为微服务提供"**集中管理、实时生效、可一键回滚**"的配置服务，是 etcd 在 C++ 技术栈下的一个精简子集实现。

### 解决什么问题

在微服务架构中，配置散落在各服务的本地文件或环境变量里，修改配置往往需要重启服务或手动分发，低效且易错。Configraft 提供：

- **集中管理**：所有配置项统一存储、统一发布。
- **实时生效**：配置变更后通过 Watch 长轮询立即推送到所有订阅方，**零停机生效**。
- **强一致**：借助 Raft 共识，配置发布不因节点宕机而丢失或不一致；读取默认线性一致。
- **安全回滚**：每次修改保留历史版本，可随时一键回滚到任意历史版本。

---

## 特性

- ✅ **基础 KV**：`Put / Get / Delete / BatchPut`（gRPC + HTTP）
- ✅ **配置管理**：多版本发布 `Publish`、历史查询 `GetConfig(key, version)`、一键回滚 `Rollback`
- ✅ **实时监听**：HTTP 长轮询 `Watch`，按全局 revision 有序推送、断点续传
- ✅ **原子操作**：`CompareAndSwap`（CAS），基于 Raft 串行 apply 天然原子
- ✅ **高可用**：3 节点集群容忍 1 节点宕机，Leader 自动选举，故障数秒内恢复
- ✅ **在线扩缩容**：动态 `AddPeer / RemovePeer` 成员变更，新节点经快照追赶加入
- ✅ **MVCC + Compaction**：全局单调 revision 版本模型，自动回收过期版本，存储不无限膨胀
- ✅ **强一致读**：ReadIndex 线性一致读，杜绝读到过期配置
- ✅ **运维友好**：健康检查、brpc 内嵌监控面板、Docker 一键启动 3 节点集群
- ✅ **混沌可验证**：kill Leader / 网络分区 / 节点重启故障演练脚本

---

## 架构

```
┌───────────────────────────────────────────────────────────────┐
│                     客户端 / SDK（跨语言）                      │
│    gRPC(HTTP/2, unary)   HTTP(RESTful)   HTTP(长轮询 Watch)    │
└───────────────┬───────────────────────────────────────────────┘
                │              brpc 单端口多协议
┌───────────────▼───────────────────────────────────────────────┐
│              API / 服务层（brpc Server）                       │
│   KVService    ConfigService    WatchService    AdminService  │
└───────────────┬───────────────────────────────────────────────┘
┌───────────────▼───────────────────────────────────────────────┐
│              Raft 层（braft）                                  │
│   选主 / 日志复制 / ReadIndex / 成员变更 / Snapshot / 故障转移   │
└───────────────┬───────────────────────────────────────────────┘
┌───────────────▼───────────────────────────────────────────────┐
│              复制状态机（串行 on_apply）                        │
│   MVCC 写入 · 版本管理 · CAS · Watch 事件 · Compaction          │
└───────────────┬───────────────────────────────────────────────┘
┌───────────────▼───────────────────────────────────────────────┐
│              存储层（LevelDB，每节点一份）                       │
│   KV 数据(MVCC 多版本) · revision 索引 · 墓碑 · Raft 元数据      │
└───────────────────────────────────────────────────────────────┘
```

### 数据流

- **写**：客户端 → Leader API → `node->apply(task)` → 多数派确认并写日志 → commit → `on_apply` 串行写入 LevelDB（产生新 revision 与 Watch 事件）→ 返回。
- **读**：客户端 → Leader API → ReadIndex 确认 → 从本地状态机读取 → 返回（线性一致）。
- **Watch**：客户端携带 `from_revision` 发起长轮询 → 挂起 → 变更事件到达后立即推送 → 用新 revision 再次请求。

---

## 技术栈

| 层级 | 选型 |
|------|------|
| 语言 | C++17 |
| 构建 | CMake + vcpkg |
| 一致性协议 | [braft](https://github.com/baidu/braft)（工业级 C++ Raft） |
| 网络框架 | [brpc](https://github.com/apache/brpc)（单端口 HTTP + gRPC + Raft 内部通信） |
| 存储引擎 | [LevelDB](https://github.com/google/leveldb) |
| 序列化 | Protobuf3 |
| 部署 | Docker / Docker Compose |
| 测试 | GoogleTest + 混沌脚本 |
| 压测 | ghz（gRPC）/ wrk（HTTP） |
| 性能分析 | Linux perf + 火焰图 |
| CI/CD | GitHub Actions |

---

## 快速开始

### 前提

- Docker / Docker Compose（推荐，已固化 brpc/braft/protobuf 版本）

### 一键启动 3 节点集群

```bash
git clone https://github.com/ranbai7/configraft.git
cd configraft
docker compose up -d
```

集群就绪后，访问任一节点（示例端口 `8000`）：

### HTTP 写入并查询配置

```bash
# 写入（发布配置，自动递增版本）
curl -X POST http://localhost:8000/v1/kv/gateway.rate_limit \
  -d '{"value":"2000"}'

# 读取（默认线性一致）
curl http://localhost:8000/v1/kv/gateway.rate_limit

# 回滚到历史版本
curl -X POST http://localhost:8000/v1/config/gateway.rate_limit/rollback \
  -d '{"target_version":1}'

# 健康检查
curl http://localhost:8000/health
```

### gRPC 调用（grpcurl / ghz）

```bash
# grpcurl 调用（提供 proto 文件）
grpcurl -proto configraft.proto -plaintext localhost:8000 \
  configraft.v1.KVService/Get \
  -d '{"key":"gateway.rate_limit"}'

# ghz 压测
ghz --insecure --proto configraft.proto \
  --call configraft.v1.KVService/Put \
  -d '{"key":"k","value":"v"}' -n 100000 -c 100 \
  localhost:8000
```

### Watch 实时监听

```bash
# 长轮询，携带 from_revision，收到变更即返回（然后携带新 revision 重新请求）
curl "http://localhost:8000/v1/watch?key=gateway.rate_limit&from_revision=0"
```

---

## 核心 API

### KV

| 方法 | HTTP | 说明 |
|------|------|------|
| `Put(key, value)` | `POST /v1/kv/{key}` | 写入 |
| `Get(key)` | `GET /v1/kv/{key}` | 读取（线性一致） |
| `Delete(key)` | `DELETE /v1/kv/{key}` | 删除 |
| `BatchPut(entries)` | `POST /v1/kv/batch` | 批量写入 |
| `CompareAndSwap(key, expect, value)` | `POST /v1/kv/{key}/cas` | 条件更新，原子 |

### 配置管理

| 方法 | HTTP | 说明 |
|------|------|------|
| `Publish(key, value)` | `POST /v1/config/{key}` | 发布新版本 |
| `GetConfig(key, version)` | `GET /v1/config/{key}` | 读指定/最新版本 |
| `Rollback(key, target_version)` | `POST /v1/config/{key}/rollback` | 一键回滚（写回旧值，产生新版本） |

### 监听与运维

| 方法 | HTTP | 说明 |
|------|------|------|
| `Watch(key, from_revision)` | `GET /v1/watch` | 长轮询变更推送，断点续传 |
| `GetHealth()` | `GET /health` | 节点角色、commit/apply index |
| `AddPeer / RemovePeer` | `POST /v1/admin/peer` | 在线成员变更 |

---

## 一致性保证

| 操作 | 一致性 | 实现 |
|------|--------|------|
| 所有写操作 | **线性一致** | Raft 多数派确认后 apply |
| `Get`（默认） | **线性一致** | Leader + ReadIndex |
| `Get`（`serializable`） | 可容忍轻微过期 | Follower 本地读，用于监控 |
| `Watch` | **有序、不丢、可续传** | 事件来自 Raft apply 序列，按 revision 排序 |

---

## 目录结构

```
configraft/
├── CMakeLists.txt
├── docker-compose.yml
├── proto/                # gRPC / Protobuf 协议定义
├── src/
│   ├── server/           # brpc 服务（KV/Config/Watch/Admin）
│   ├── raft/             # braft StateMachine、快照、客户端路由
│   ├── store/            # LevelDB 封装、MVCC、Compaction
│   └── common/           # 工具、日志、配置
├── client/               # 示例客户端与 SDK
├── scripts/              # 混沌测试、压测、部署脚本
├── tests/                # GoogleTest 单元 / 集成测试
└── docker/               # 构建镜像
```

---

## 本地开发

```bash
# 依赖：brpc、braft、leveldb、protobuf（建议 vcpkg 安装固定版本）
git clone https://github.com/ranbai7/configraft.git
cd configraft
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j

# 运行单元测试
ctest --test-dir build

# 本地启动单节点（开发模式）
./build/configraft_server --node node1 --peers 127.0.0.1:8001,127.0.0.1:8002,127.0.0.1:8003
```

---

## 性能与验证

- **压测**：`scripts/benchmark.sh` 使用 ghz / wrk 输出 QPS 与 P99 时延报告。
- **混沌验证**：`scripts/chaos/` 提供 kill Leader、kill Follower、网络分区、节点重启演练，验证写线性一致、读一致、Watch 有序不丢、集群自动恢复。
- **性能分析**：`scripts/profile.sh` 生成 perf + 火焰图，定位写路径 / 序列化 / 内存拷贝热点。

---

## Roadmap

- [ ] M1 单机 KV 骨架（brpc + LevelDB + Protobuf）
- [ ] M2 braft 接入：3 节点写入一致 + 自动选主
- [ ] M3 MVCC 版本模型：Publish / GetConfig / Rollback + Compaction
- [ ] M4 Watch 长轮询 + revision 断点续传
- [ ] M5 CAS 与 ReadIndex 线性一致读
- [ ] M6 成员变更 + Docker 一键集群 + 健康检查
- [ ] M7 压测、混沌测试、perf 火焰图与文档

---

## 致谢

- [braft](https://github.com/baidu/braft) —— 工业级 C++ Raft 实现
- [brpc](https://github.com/apache/brpc) —— 高性能多协议 RPC 框架
- [LevelDB](https://github.com/google/leveldb) —— 嵌入式 LSM-Tree 存储引擎
- [etcd](https://github.com/etcd-io/etcd) —— 分布式 KV/配置中心设计参照

## License

[Apache License 2.0](LICENSE)
