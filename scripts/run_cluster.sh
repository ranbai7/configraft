#!/usr/bin/env bash
# 本机一键启动/停止 3 节点 Configraft 集群（Raft 复制验证用）。
#
# 用法:
#   scripts/run_cluster.sh              # 启动 3 节点集群（端口 8001-8003，数据 /tmp/cfg-cluster）
#   scripts/run_cluster.sh [BASE_DIR]   # 自定义数据目录启动
#   scripts/run_cluster.sh stop         # 一条命令停止集群（kill 所有 configraft 节点进程）
set -euo pipefail

# stop 参数：停止所有 configraft 节点进程。
# 用 pgrep -x configraft_serv 精确匹配进程名（内核截断为 15 字符），避免
# pgrep -f 在部分终端环境（如 Claude Code）误匹配到 shell wrapper。
if [ "${1:-}" = "stop" ]; then
    pids="$(pgrep -x configraft_serv || true)"
    if [ -z "$pids" ]; then
        echo "没有运行中的 configraft 节点进程。"
        exit 0
    fi
    echo "停止 configraft 节点: $pids"
    # shellcheck disable=SC2086
    kill $pids 2>/dev/null || true
    sleep 1
    for pid in $pids; do
        if kill -0 "$pid" 2>/dev/null; then
            echo "  pid=$pid 未正常退出，kill -9 强杀"
            kill -9 "$pid" 2>/dev/null || true
        fi
    done
    sleep 1
    if pgrep -x configraft_serv >/dev/null; then
        echo "警告：仍有进程未退出："
        pgrep -ax configraft_serv || true
        exit 1
    fi
    echo "已全部停止（configraft_serv 无残留）。"
    exit 0
fi

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="$REPO_ROOT/build/configraft_server"
BASE_DIR="${1:-/tmp/cfg-cluster}"
PEERS="127.0.0.1:8001,127.0.0.1:8002,127.0.0.1:8003"

if [ ! -x "$BIN" ]; then
    echo "未找到 $BIN，请先编译: cmake --build build -j"
    exit 1
fi

# 清理旧进程与数据（保证每次演示从干净状态开始）。
# 注意：进程名被内核截断为 configraft_serv（15 字符），pgrep -x 全名匹配不到，
# 须用 -f 匹配完整命令行。
for pid in $(pgrep -f "build/configraft_server" || true); do
    kill "$pid" 2>/dev/null || true
done
sleep 1
rm -rf "$BASE_DIR"
mkdir -p "$BASE_DIR"

for i in 1 2 3; do
    port=$((8000 + i))
    dir="$BASE_DIR/node$i"
    mkdir -p "$dir"
    "$BIN" \
        --port "$port" \
        --node "node$i" \
        --peers "$PEERS" \
        --data_dir "$dir" \
        --election_timeout_ms 1000 \
        > "$dir/server.log" 2>&1 &
    echo "node$i -> 127.0.0.1:$port (data=$dir)"
done

echo "等待集群选举..."
sleep 3

echo
echo "=== 端口监听 ==="
ss -tlnp 2>/dev/null | grep -E ":800[123]" | awk '{print $4}' || true

echo
echo "=== 各节点日志关键行 ==="
for i in 1 2 3; do
    echo "--- node$i ---"
    grep -E "become leader|start following|raft node .* started|Configuration of this group|listening" \
        "$BASE_DIR/node$i/server.log" | tail -2 || echo "(无关键日志)"
done

echo
echo "=== 集群 ready。写请求请打到 leader 节点。==="
echo "   查看 leader:  grep 'become leader' $BASE_DIR/node*/server.log"
