# Configraft 服务镜像 —— 基于本地构建产物（先本地构建：cmake --build build -j）
#
# 设计说明：
#   - configraft_server 动态链接了 third_party/install/lib/libbraft.so（自构建），
#     故镜像需一并携带该 .so，其余系统库（leveldb/ssl/z/snappy/gflags）由
#     ubuntu:22.04 官方仓库提供，与本地构建环境版本一致。
#   - 多阶段在镜像内完整构建依赖需要 30-60 分钟且依赖 GitHub 下载，这里选择
#     COPY 本地产物，保证 `docker compose up` 快速可复现。
# docker.io 在本网络不可达，用 daocloud 加速器（代理 docker hub）
FROM docker.m.daocloud.io/library/ubuntu:22.04

# 运行所需系统动态库 + 健康检查用 curl
RUN apt-get update && apt-get install -y --no-install-recommends \
        curl \
        libssl3 \
        libleveldb1d \
        libsnappy1v5 \
        libgflags2.2 \
        libz1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY build/configraft_server /app/configraft_server
COPY third_party/install/lib/libbraft.so /app/lib/libbraft.so

ENV LD_LIBRARY_PATH=/app/lib

# Raft 内部通信与 gRPC/HTTP 同端口
EXPOSE 8001 8002 8003

ENTRYPOINT ["/app/configraft_server"]
