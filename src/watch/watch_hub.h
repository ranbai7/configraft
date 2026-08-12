#pragma once
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "bthread/condition_variable.h"
#include "bthread/mutex.h"
#include "configraft.pb.h"

namespace configraft {

struct WatchResult;  // raft/node.h 中定义（避免循环包含）
class Store;

// 一次长轮询等待会话。
//
// 生命周期约定（防 UAF 的核心）：调用方（阻塞在长轮询里的那个 bthread）持有
// shared_ptr，WatchHub 集合里只存 weak_ptr。Waiter 的生存期由调用方掌控——
// Watch 返回前先 Unregister（在 hub 锁下移除 weak_ptr）再丢弃强引用；广播侧
// Broadcast 持 weak_ptr 快照后逐个 lock()，失败即跳过，绝不产生悬垂访问。
struct Waiter {
    bthread::Mutex mu;                 // 保护 events / overflow（Broadcast 与等待循环竞争）
    bthread::ConditionVariable cv;
    std::string key_filter;            // 空串 = 监听全部 key
    std::deque<v1::WatchEvent> events; // 事件缓冲（上限 max_events_per_waiter）
    bool overflow = false;             // 背压标记：缓冲满被断开

    // 事件是否匹配本 waiter 的 key 过滤。须持 waiter->mu 时调用。
    bool Matches(const v1::WatchEvent& e) const {
        return key_filter.empty() || e.key() == key_filter;
    }
};

// 单 Waiter 事件缓冲上限（背压阈值）。
static constexpr size_t kMaxWatchEvents = 1024;

// WatchHub：每进程一个的事件分发中枢，解耦"事件产生（on_apply 串行广播）"
// 与"事件消费（长轮询 bthread 阻塞等待）"。
//
// 广播侧（写路径）：状态机 on_apply / LocalNode::Apply 串行 apply 一条指令后
// 调用 Broadcast()，把该指令产生的事件推给所有匹配的活跃 Waiter。同步广播、
// 持锁不跨长操作，保证事件顺序 == 日志 apply 顺序。
//
// 消费侧（读路径）：每个 HTTP 长轮询请求调用 Watch()，先在 Store 重放历史
// （断点续传，事件无需额外持久化），再注册 Waiter 阻塞等待新事件 / 超时。
//
// 背压：Waiter 缓冲满即标记 overflow 并清空断开，让客户端以 current_revision
// 重连重放，绝不让慢消费者阻塞 on_apply。
class WatchHub {
public:
    explicit WatchHub(size_t max_events_per_waiter = kMaxWatchEvents);
    ~WatchHub();

    WatchHub(const WatchHub&) = delete;
    WatchHub& operator=(const WatchHub&) = delete;

    // 广播事件给所有匹配的活跃 Waiter。on_apply / LocalNode::Apply 调用。
    void Broadcast(const std::vector<v1::WatchEvent>& events);

    // 长轮询主逻辑（阻塞当前 bthread，不占 pthread worker）：
    //   1. from_revision > 0 时先重放 (from, current] 历史（Register 先于 Replay，
    //      关闭"已提交历史"与"新事件广播"之间的窗口，保证无洞）；
    //   2. 无事件则阻塞等待 Broadcast / 超时 / canceled / server_deadline；
    //   3. 历史已被 Compaction 回收 → 返回 false 且 code=COMPACTED；
    //      缓冲溢出（慢消费者）→ 返回 false 且 code=INTERNAL(overflow)。
    // 返回 true 表示 out 已填充（事件或超时空结果）；false 表示错误（COMPACTED/overflow）。
    //   server_deadline_us：服务器侧 RPC 截止（epoch 微秒），<=0 表示无。
    //   canceled：可选，非空时在等待循环中轮询（连接断开时提前退出并释放 waiter）。
    bool Watch(const std::string& key, int64_t from_revision, int64_t timeout_ms,
               int64_t server_deadline_us, const std::function<bool()>* canceled,
               Store* store, WatchResult* out);

private:
    void Register(const std::shared_ptr<Waiter>& w);    // hub 存 weak_ptr
    void Unregister(const std::shared_ptr<Waiter>& w);  // 移除指向 w 的 weak_ptr

    bthread::Mutex mu_;  // 保护 waiters_（仅注册/注销/快照拷贝，临界区极短）
    std::vector<std::weak_ptr<Waiter>> waiters_;
    size_t max_events_per_waiter_;
};

}  // namespace configraft
