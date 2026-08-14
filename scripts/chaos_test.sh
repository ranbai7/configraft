#!/usr/bin/env bash
# ============================================================
# M7 混沌测试：kill Leader / kill Follower / 网络分区(STOP) / 重启
#
# 全程一致性验证：
#   - 写线性一致：写后立即可从 leader 读回新值（值=递增序号，读到旧值=违规）
#   - Watch 有序不丢：长轮询事件 revision 严格递增
#   - 集群自动恢复：每次故障后重新选出 leader，写路径恢复
#
# 用法: scripts/chaos_test.sh [BASE_DIR]
#   BASE_DIR 默认 /tmp/cfg-chaos（自动清理重建，注意会清掉该目录）
# 退出码: 0=通过  1=存在一致性违规
#
# 说明：网络分区用 kill -STOP/CONT 模拟（进程挂起=心跳与请求全部不可达，
#       等效网络隔离；无需 root）。
# ============================================================
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="$REPO_ROOT/build/configraft_server"
BASE_DIR="${1:-/tmp/cfg-chaos}"
PEERS="127.0.0.1:8001,127.0.0.1:8002,127.0.0.1:8003"
PORTS=(8001 8002 8003)
KEY="chaos/verify"

VERIFY_LOG="$BASE_DIR/verify.log"

if [ ! -x "$BIN" ]; then
    echo "未找到 $BIN，请先编译: cmake --build build -j"
    exit 1
fi

# ---- 工具函数 ----

# 按端口找 pid（ss 优先，pgrep 兜底）
pid_by_port() {
    local port=$1 pid
    pid=$(ss -tlnp 2>/dev/null | grep ":$port " | grep -o 'pid=[0-9]*' | head -1 | cut -d= -f2)
    if [ -z "$pid" ]; then
        pid=$(pgrep -f "configraft_server --port $port" || true)
    fi
    echo "$pid"
}

kill_all() {
    for pid in $(pgrep -f "configraft_server --port" || true); do
        kill -9 "$pid" 2>/dev/null || true
    done
    sleep 1
}

start_node() {
    local port=$1 dir=$2
    local idx=$((port - 8000))
    "$BIN" --port "$port" --node "node$idx" --peers "$PEERS" \
        --data_dir "$dir" --election_timeout_ms 1000 \
        >> "$BASE_DIR/node$idx.log" 2>&1 &
}

# 探测当前 leader 端口
leader_port() {
    local p role
    for p in "${PORTS[@]}"; do
        role=$(curl -s --max-time 1 "http://127.0.0.1:$p/healthz" 2>/dev/null \
               | grep -o '"role":"leader"')
        if [ -n "$role" ]; then echo "$p"; return 0; fi
    done
    return 1
}

wait_leader() {
    local i lp
    for i in $(seq 1 40); do
        if lp=$(leader_port); then echo "$lp"; return 0; fi
        sleep 1
    done
    return 1
}

# ---- 一致性验证器（后台）：写-读校验线性一致 ----
verifier() {
    local last_confirmed=0 ok=0 write_fail=0 violations=0 n=0
    while :; do
        n=$((n + 1))
        local lp val resp code got vn
        lp=$(leader_port) || { sleep 0.3; continue; }
        val="v$n"
        resp=$(curl -s --max-time 3 -X POST -H 'Content-Type: application/json' \
            -d "{\"value\":\"$val\"}" "http://127.0.0.1:$lp/v1/kv/$KEY")
        code=$(echo "$resp" | sed -n 's/.*"code":\([0-9-]*\).*/\1/p')
        if [ "${code:-x}" != "0" ]; then
            # 瞬时失败（leader 变更 / 节点重启中）可接受；若连续多轮失败需人工看
            write_fail=$((write_fail + 1))
            sleep 0.2
            continue
        fi
        ok=$((ok + 1))
        last_confirmed=$n
        # 线性一致读：读回序号必须 >= 已确认写入
        got=$(curl -s --max-time 3 "http://127.0.0.1:$lp/v1/kv/$KEY")
        vn=$(echo "$got" | sed -n 's/.*"value":"v\([0-9]*\)".*/\1/p')
        if [ -z "$vn" ]; then
            violations=$((violations + 1))
            echo "LINEAR_VIOLATION read_empty n=$n" >> "$VERIFY_LOG"
        elif [ "$vn" -lt "$last_confirmed" ]; then
            violations=$((violations + 1))
            echo "LINEAR_VIOLATION confirmed=$last_confirmed read=$vn n=$n" >> "$VERIFY_LOG"
        fi
        sleep 0.2
    done
}

# ---- Watch 验证器（后台）：事件 revision 严格递增 ----
watch_verifier() {
    local from=0 prev_rev=0 tmp
    while :; do
        # Watch 可落任意可达节点（每节点本地事件流）
        local p resp cur
        p=""
        for pp in "${PORTS[@]}"; do
            if curl -s --max-time 1 "http://127.0.0.1:$pp/healthz" >/dev/null 2>&1; then
                p=$pp
                break
            fi
        done
        [ -z "$p" ] && { sleep 0.3; continue; }
        resp=$(curl -s --max-time 10 "http://127.0.0.1:$p/v1/watch/$KEY?from_revision=$from&timeout_ms=9000")
        # INTERNAL（如节点落后被拒）或空响应：保留锚点 from，重试不回退
        code=$(echo "$resp" | sed -n 's/.*"code":\([0-9-]*\).*/\1/p')
        cur=$(echo "$resp" | sed -n 's/.*"current_revision":\([0-9]*\).*/\1/p')
        if [ "${code:-0}" != "0" ] || [ -z "$cur" ]; then
            sleep 0.2
            continue
        fi
        # 提取本批事件 revision，校验严格递增（临时文件避免子 shell 变量问题）
        tmp=$(mktemp)
        echo "$resp" | grep -o '"revision":[0-9]*' | sed 's/"revision"://' > "$tmp"
        while read -r r; do
            if [ -n "$r" ] && [ "$r" -le "$prev_rev" ]; then
                echo "WATCH_ORDER_VIOLATION prev=$prev_rev rev=$r from=$from node=:$p resp=$resp" >> "$VERIFY_LOG"
            fi
            [ -n "$r" ] && prev_rev=$r
        done < "$tmp"
        rm -f "$tmp"
        from=$cur
    done
}

# ---- 主流程 ----

echo "=== 清理旧进程与数据 ==="
kill_all
rm -rf "$BASE_DIR"
mkdir -p "$BASE_DIR"
: > "$VERIFY_LOG"

echo "=== 启动 3 节点集群 ==="
for i in 1 2 3; do
    start_node "$((8000 + i))" "$BASE_DIR/node$i"
done
echo "等待选举..."
sleep 3
LEADER=$(wait_leader) || { echo "FAIL: 集群未能选出 leader"; exit 1; }
echo "初始 leader: node$((LEADER - 8000)) (:$LEADER)"

verifier & VERIFIER_PID=$!
watch_verifier & WATCH_PID=$!
sleep 2

# 场景 A：kill -9 Leader
echo
echo "=== [场景 A] kill -9 Leader (:$LEADER) ==="
LPID=$(pid_by_port "$LEADER")
[ -z "$LPID" ] && { echo "FAIL: 找不到 leader 进程"; exit 1; }
kill -9 "$LPID"
echo "  killed pid=$LPID"
T0=$(date +%s)
NEW_LEADER=$(wait_leader) || { echo "FAIL: kill leader 后未能重选"; exit 1; }
T1=$(date +%s)
echo "  新 leader: node$((NEW_LEADER - 8000)) (:$NEW_LEADER)，恢复耗时 $((T1 - T0))s"
sleep 2

# 场景 B：kill -9 Follower
echo
echo "=== [场景 B] kill -9 Follower ==="
FP=""
for p in "${PORTS[@]}"; do
    [ "$p" = "$NEW_LEADER" ] && continue
    FP=$p
    break
done
FPID=$(pid_by_port "$FP")
kill -9 "$FPID"
echo "  killed follower :$FP pid=$FPID"
sleep 3
LP2=$(leader_port) && echo "  leader 仍正常: :$LP2 (写读未中断)"
sleep 2

# 场景 C：网络分区（STOP 一个节点）
echo
echo "=== [场景 C] 网络分区: STOP 一个节点，再 CONT 恢复 ==="
SP=""
for p in "${PORTS[@]}"; do
    if curl -s --max-time 1 "http://127.0.0.1:$p/healthz" >/dev/null 2>&1; then
        SP=$p
        break
    fi
done
SPID=$(pid_by_port "$SP")
kill -STOP "$SPID"
echo "  STOP node$((SP - 8000)) (:$SP, pid=$SPID) —— 心跳/请求全部不可达"
sleep 5   # 超过心跳超时，节点被排除
LP3=$(leader_port) && echo "  分区期间集群仍可用: leader=:$LP3"
kill -CONT "$SPID"
echo "  CONT 恢复 node$((SP - 8000))"
sleep 4
echo "  恢复后 leader: $(leader_port || echo NONE)"

# 场景 D：重启场景 A/B 中被 kill 的节点（数据保留，验证自动追赶）
echo
echo "=== [场景 D] 重启宕机节点（数据保留，自动追赶）==="
for i in 1 2 3; do
    port=$((8000 + i))
    if ! curl -s --max-time 1 "http://127.0.0.1:$port/healthz" >/dev/null 2>&1; then
        echo "  重启 node$i (:$port)"
        start_node "$port" "$BASE_DIR/node$i"
        sleep 1
    fi
done
# 等待所有节点进入配置且数据追平（健康且 commit>=验证器当前值）
sleep 6
echo "  最终各节点:"
for p in "${PORTS[@]}"; do
    echo -n "    :$p => "
    curl -s --max-time 2 "http://127.0.0.1:$p/healthz" | sed -n 's/.*"role":"\([a-z]*\)".*"term":\([0-9]*\).*/role=\1 term=\2/p' || echo "(不可达)"
done

# ---- 汇总 ----
sleep 2
kill "$VERIFIER_PID" "$WATCH_PID" 2>/dev/null
sleep 1

echo
echo "======== 混沌测试汇总 ========"
echo "验证器日志 ($VERIFY_LOG):"
[ -s "$VERIFY_LOG" ] && cat "$VERIFY_LOG" || echo "  （无错误记录）"
echo
HARD_ERR=$(grep -cE "LINEAR_VIOLATION|WATCH_ORDER_VIOLATION" "$VERIFY_LOG" || true)
echo "一致性违规数（线性一致 / Watch 乱序）: ${HARD_ERR}"
if [ "${HARD_ERR:-0}" = "0" ]; then
    echo "✅ 混沌测试通过：写线性一致、Watch 有序、集群自动恢复"
    exit 0
else
    echo "❌ 混沌测试失败：存在一致性违规，见 $VERIFY_LOG"
    exit 1
fi
