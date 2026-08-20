#!/usr/bin/env bash
# Configraft 性能压测：自动跑测试矩阵并汇总 QPS / P99。
#
# 用法:
#   scripts/benchmark.sh                    # 对【当前运行中】的集群跑默认矩阵
#   scripts/benchmark.sh --all              # 完整矩阵：fsync 开/关 × 单机/集群，自动启停集群
#   scripts/benchmark.sh --nodes 5          # 集群节点数（默认 3；--all 时有效）
#   scripts/benchmark.sh --dur 10           # 每轮压测时长（秒，默认 10）
#   scripts/benchmark.sh --mode cluster     # 只跑指定模式（single|cluster）
#   scripts/benchmark.sh --raft-sync false  # 只跑指定 fsync 配置（true|false）
#
# 依赖: ghz（gRPC）、wrk（HTTP）、curl。缺工具时跳过对应项目并提示。
# 输出: 汇总表（场景 | QPS | P99）。写入 README 性能模块的数据源。
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="$REPO_ROOT/build/configraft_server"
DUR=10
ALL=0
NODES=3
MODES=""
SYNC_MODES=""

usage() {
    sed -n '2,12p' "$0"
    exit 1
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --all) ALL=1 ;;
        --nodes) NODES="${2:-3}"; shift ;;
        --dur) DUR="${2:-10}"; shift ;;
        --mode) MODES="${2:-}"; shift ;;
        --raft-sync) SYNC_MODES="${2:-}"; shift ;;
        *) usage ;;
    esac
    shift
done

if [[ "$ALL" = 1 ]]; then
    MODES="${MODES:-single cluster}"
    SYNC_MODES="${SYNC_MODES:-true false}"
else
    MODES="${MODES:-cluster}"
    SYNC_MODES="${SYNC_MODES:-$( [ -f /tmp/cfg-bench-fsync ] && cat /tmp/cfg-bench-fsync || echo true )}"
fi

# 工具检查
GHZ="$(command -v ~/go/bin/ghz >/dev/null 2>&1 && echo ~/go/bin/ghz || command -v ghz || echo '')"
WRK="$(command -v wrk || echo '')"

# ---------------- 集群启停 ----------------
stop_all() {
    for pid in $(pgrep -x configraft_serv || true); do kill "$pid" 2>/dev/null || true; done
    sleep 1
}

start_cluster() {
    local mode="$1" sync="$2"
    stop_all
    echo "[env] mode=$mode raft_sync=$sync ..."
    if [[ "$mode" = "single" ]]; then
        rm -rf /tmp/cfg-bench-single && mkdir -p /tmp/cfg-bench-single
        "$BIN" --port 8100 --data_dir /tmp/cfg-bench-single \
               --raft_sync="$sync" > /tmp/cfg-bench-single.log 2>&1 &
        sleep 2
        BENCH_LEADER=8100
    else
        # 动态生成 peers（如 5 节点: 127.0.0.1:8001..8005）
        local peers=""
        for i in $(seq 1 "$NODES"); do
            peers="${peers}${peers:+,}127.0.0.1:$((8000 + i))"
        done
        rm -rf /tmp/cfg-bench-cluster && mkdir -p /tmp/cfg-bench-cluster
        for i in $(seq 1 "$NODES"); do
            local port=$((8000 + i))
            "$BIN" --port "$port" --node "node$i" --peers "$peers" \
                   --data_dir "/tmp/cfg-bench-cluster/node$i" \
                   --election_timeout_ms 1000 --raft_sync="$sync" \
                   > "/tmp/cfg-bench-cluster/node$i.log" 2>&1 &
        done
        # 节点越多选举越慢，给足时间
        sleep $((2 + NODES))
        BENCH_LEADER=0
        for i in $(seq 1 "$NODES"); do
            if curl -s "http://127.0.0.1:800$i/healthz" | grep -q '"role":"leader"'; then
                BENCH_LEADER=$((8000 + i)); break
            fi
        done
    fi
    [[ "$BENCH_LEADER" != 0 ]] || { echo "!! 集群未选出 leader"; exit 1; }
    echo "[env] leader=127.0.0.1:$BENCH_LEADER"
}

# ---------------- 压测与解析 ----------------
# 输出一行: 名字 QPS P99
run_ghz() {  # name call data concurrency
    local name="$1" call="$2" data="$3" c="$4"
    local out qps p99
    # set +e：grep 无匹配时不让 set -e 终止整个压测
    set +e
    out="$("$GHZ" --insecure --proto "$REPO_ROOT/proto/configraft.proto" \
        --call "$call" -d "$data" -c "$c" -z "${DUR}s" \
        "127.0.0.1:$BENCH_LEADER" 2>&1)"
    qps="$(echo "$out" | grep -oP 'Requests/sec:\s*\K[\d.]+' | head -1)"
    p99="$(echo "$out" | grep -oP '99 % in \K[\d.]+' | head -1)"
    set -e
    printf '%-14s c%-4s %s\n' "$name" "$c" "$(fmt "$qps" "$p99")"
}

run_wrk() {  # name url [lua]
    local name="$1" url="$2" lua="${3:-}"
    local args=(-t 4 -c 100 -d "${DUR}s" -L)
    [[ -n "$lua" ]] && args+=(-s "$lua")
    local out qps p99_raw val p99
    set +e
    out="$("$WRK" "${args[@]}" "$url" 2>&1)"
    qps="$(echo "$out" | grep -oP 'Requests/sec:\s*\K[\d.]+' | head -1)"
    # wrk 百分位形如 "     99%    467.00us" / "1.42ms"，值可能带 us/ms/s 单位。
    # 提取"数值+单位"，统一换算成毫秒（与 ghz 的 ms 口径一致）。
    p99_raw="$(echo "$out" | grep -oE '^\s*99% *[0-9.]+(us|ms|s)' | head -1)"
    # 取最后一个数字串（"99%" 里的 99 不是值，用 tail 取真正的延迟值）
    val="$(echo "$p99_raw" | grep -oE '[0-9.]+' | tail -1)"
    case "$p99_raw" in
        *us) p99="$(awk "BEGIN{printf \"%.2f\", ${val:-0}/1000}")" ;;
        *ms) p99="$val" ;;
        *s)  p99="$(awk "BEGIN{printf \"%.2f\", ${val:-0}*1000}")" ;;
        *)   p99="$val" ;;
    esac
    set -e
    printf '%-14s      %s\n' "$name" "$(fmt "$qps" "$p99")"
}

fmt() {  # 输出 "QPS  QPS  P99 P99ms"
    local qps="${1:-?}" p99="${2:-?}"
    printf 'QPS=%-9s P99=%sms' "$qps" "$p99"
}

run_matrix() {
    local mode="$1" sync="$2"
    echo
    echo "================ ${mode} / raft_sync=$sync / ${DUR}s ================"
    local label="[${mode}]"

    # 写入一个测试 key
    curl -s -X POST "http://127.0.0.1:$BENCH_LEADER/v1/kv/bench.key" -d '{"value":"b"}' >/dev/null

    if [[ -n "$GHZ" ]]; then
        for c in 8 32 64 128; do
            run_ghz "$label gRPC Put"  configraft.v1.KVService.Put '{"key":"bench.key","value":"bench-value"}' "$c"
        done
        for c in 32 64; do
            run_ghz "$label gRPC Get"  configraft.v1.KVService.Get '{"key":"bench.key"}' "$c"
        done
    else
        echo "$label ghz 未安装，跳过 gRPC 项"
    fi

    if [[ -n "$WRK" ]]; then
        run_wrk "$label HTTP GET"  "http://127.0.0.1:$BENCH_LEADER/v1/kv/bench.key"
        printf 'wrk.method = "POST"\nwrk.headers["Content-Type"]="application/json"\nwrk.body = "{\\"value\\":\\"bench-value\\"}"\n' > /tmp/bench_post.lua
        run_wrk "$label HTTP POST" "http://127.0.0.1:$BENCH_LEADER/v1/kv/bench.key" /tmp/bench_post.lua
    else
        echo "$label wrk 未安装，跳过 HTTP 项"
    fi
}

# ---------------- 主流程 ----------------
echo "工具: ghz=${GHZ:-缺失}  wrk=${WRK:-缺失}  时长=${DUR}s"
echo "矩阵: nodes=$NODES mode=[$MODES] raft_sync=[$SYNC_MODES]"

for mode in $MODES; do
    for sync in $SYNC_MODES; do
        if [[ "$ALL" = 1 ]]; then
            start_cluster "$mode" "$sync"
        else
            BENCH_LEADER=0
            if [[ "$mode" = "cluster" ]]; then
                for i in $(seq 1 "$NODES"); do
                    if curl -s "http://127.0.0.1:800$i/healthz" | grep -q '"role":"leader"'; then
                        BENCH_LEADER=$((8000 + i)); break
                    fi
                done
            else
                BENCH_LEADER=8100
            fi
            [[ "$BENCH_LEADER" != 0 ]] || { echo "!! 请先启动集群（bash scripts/run_cluster.sh）"; exit 1; }
        fi
        echo "$sync" > /tmp/cfg-bench-fsync
        run_matrix "$mode" "$sync"
    done
done

# 仅 --all 模式自动清理临时集群；对"当前集群"压测后保留现场。
if [[ "$ALL" = 1 ]]; then
    stop_all
fi
echo
echo "=== 压测完成。数据可写入 README「性能与验证」章节（标注测试环境）。 ==="
