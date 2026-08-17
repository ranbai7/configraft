# 贡献指南

欢迎对 Configraft 贡献代码、文档或 issue。这是一个用于学习与面试展示的分布式配置中心项目，任何改进都欢迎。

## 项目简介

Configraft 是基于 braft / brpc / LevelDB 的 C++17 分布式配置中心：强一致、实时生效、可回滚。功能包括 MVCC 多版本、Watch 长轮询、CAS 原子更新、线性一致读、在线成员变更、Docker 一键部署。

## 环境与构建

```bash
# 1. 构建第三方依赖（protobuf/brpc/braft/gtest 源码构建，幂等可重复）
./scripts/build_deps.sh

# 2. 构建项目（configraft_server / configraft_client）
cmake -B build
cmake --build build -j$(nproc)

# 3. 运行全部单元测试
ctest --test-dir build --output-on-failure
```

- 依赖版本锁定见 `scripts/build_deps.sh`（protobuf 3.21.12 / brpc 1.17.0 / braft v1.1.2 / gtest 1.11）。
- 已配置 GitHub Actions 持续集成（`.github/workflows/ci.yml`），push 后自动构建 + 测试。

## 目录结构

```
proto/configraft.proto   协议定义（KV/Config/Watch/Admin + RaftCmd 内部指令）
src/
  common/                存储 key 编码（MVCC 布局）、状态码、日志
  store/                 LevelDB 封装 + MVCC 操作（Put/Get/历史/回滚/Compaction）
  raft/                  ConfigNode 抽象 + LocalNode(单机) / RaftNode(braft 集群)
  server/                brpc 服务实现（KV/Config/Watch/Admin，gRPC + REST 双协议）
  watch/                 WatchHub 事件分发中枢（长轮询 + 背压）
  main/                  服务入口（gflags 参数）
client/                  示例客户端
tests/                   GoogleTest 单元/集成测试
scripts/                 依赖构建、集群启动、混沌测试脚本
docs/                    开发计划与各里程碑设计笔记
```

## 开发流程

项目按里程碑（M0–M7）逐阶段开发，见 [docs/开发计划.md](docs/开发计划.md)。每个里程碑：**代码 → 验收标准 → 简历素材 → 面试题** 四个产出。

## 提交规范

- Commit message 前缀：`feat:` / `fix:` / `docs:` / `ci:` / `refactor:` / `test:`。
- 每个里程碑建议一个 commit 并打 tag（现有 `v0.1.0`–`v0.7.0`）。
- 新增/修改行为须附对应测试，保证 `ctest` 全绿。

## 代码风格

- C++17，RAII / 智能指针优先。
- 业务代码在 `namespace configraft`，协议类型 `using namespace v1`。
- 注释用中文，重点写"为什么这么设计"（面向面试的可讲性）。
- 写路径必须经 Raft 串行 apply（`ApplyCmdToStore`），禁止在 on_apply 之外改状态。
