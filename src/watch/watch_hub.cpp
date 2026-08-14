#include "watch/watch_hub.h"

#include <algorithm>
#include <cerrno>
#include <utility>

#include "bthread/bthread.h"
#include "butil/time.h"
#include "raft/node.h"
#include "store/store.h"

namespace configraft {

using namespace v1;  // Code/WatchEvent 定义在 configraft.v1 包

WatchHub::WatchHub(size_t max_events_per_waiter)
    : max_events_per_waiter_(max_events_per_waiter > 0 ? max_events_per_waiter
                                                       : kMaxWatchEvents) {}

WatchHub::~WatchHub() {
    // 活跃 Waiter 的强引用由阻塞在 Watch() 中的 bthread 持有；本对象析构时
    // brpc Server 已停（等待在途 RPC 完成），无在途 Watch 调用。残留 weak_ptr 无需处理。
}

void WatchHub::Register(const std::shared_ptr<Waiter>& w) {
    std::lock_guard<bthread::Mutex> lock(mu_);
    waiters_.push_back(w);
}

void WatchHub::Unregister(const std::shared_ptr<Waiter>& w) {
    std::lock_guard<bthread::Mutex> lock(mu_);
    // 线性删除：活跃 watcher 数量级小（配置中心场景），可接受。
    // 若未来 watcher 量级大，再换 per-key 索引或引用计数。
    for (auto it = waiters_.begin(); it != waiters_.end();) {
        if (it->lock().get() == w.get()) {
            it = waiters_.erase(it);
        } else {
            ++it;
        }
    }
}

void WatchHub::Broadcast(const std::vector<WatchEvent>& events) {
    if (events.empty()) {
        return;
    }
    // 快照式遍历：先在 mu_ 下拷贝 weak_ptr 列表并立即放锁，再逐个处理。
    // 锁序恒为 mu_ → waiter->mu，禁止任何路径反向加锁（否则与等待循环 ABBA 死锁）。
    std::vector<std::weak_ptr<Waiter>> snapshot;
    {
        std::lock_guard<bthread::Mutex> lock(mu_);
        snapshot = waiters_;
    }
    for (const auto& w : snapshot) {
        std::shared_ptr<Waiter> sp = w.lock();  // 失败 = 已注销/销毁，跳过
        if (!sp) {
            continue;
        }
        std::lock_guard<bthread::Mutex> lock(sp->mu);
        for (const auto& e : events) {
            if (!sp->Matches(e)) {
                continue;
            }
            if (sp->events.size() >= max_events_per_waiter_) {
                // 背压：慢消费者缓冲溢出 → 断开，让客户端以 current_revision 重连重放。
                // 清空缓冲避免返回不完整事件；不阻塞 on_apply。
                sp->overflow = true;
                sp->events.clear();
                break;
            }
            sp->events.push_back(e);
        }
        sp->cv.notify_all();  // 先改状态（events/overflow）后 notify，谓词循环保证无丢唤醒
    }
}

namespace {

// server_deadline_us（epoch 微秒）转绝对 timespec。
timespec FromEpochUs(int64_t us) {
    timespec ts;
    ts.tv_sec = us / 1000000;
    ts.tv_nsec = static_cast<long>(us % 1000000) * 1000;
    return ts;
}

bool TimespecLess(const timespec& a, const timespec& b) {
    return a.tv_sec < b.tv_sec || (a.tv_sec == b.tv_sec && a.tv_nsec < b.tv_nsec);
}

}  // namespace

bool WatchHub::Watch(const std::string& key, int64_t from_revision, int64_t timeout_ms,
                     int64_t server_deadline_us, const std::function<bool()>* canceled,
                     Store* store, WatchResult* out) {
    out->code = Code::OK;
    out->message.clear();
    out->events.clear();
    out->current_revision = 0;

    if (from_revision < 0) {
        from_revision = 0;
    }
    timeout_ms = std::clamp<int64_t>(timeout_ms, 1, 60000);

    // 1) 快速判定：from_revision 之前的历史已被 Compaction 回收 → 断点续传不可继续。
    //    compact_rev = 被删 v/ 记录的最大 revision，故 from >= compact_rev ⟺ 重放区间无洞。
    if (from_revision > 0 && store->CompactRev() > from_revision) {
        out->code = Code::COMPACTED;
        out->message = "requested revision has been compacted, resume from current_revision";
        out->current_revision = store->CurrentRevision();
        return false;
    }

    // 1.5) 节点落后于客户端续传锚点（重启/快照追赶中）：先等本节点追平，避免追赶
    //      期间本地重新 apply 旧日志、广播出客户端已消费的旧事件（M7 混沌测试发现：
    //      from=N 连上落后节点会收到 revision<=N 的重复事件，破坏"不丢不重"）。
    //      追赶通常毫秒级（日志/快照复制）；5s 未追平（如大快照加载）返回 INTERNAL，
    //      客户端保留锚点重试——**绝不返回低于 from_revision 的 current_revision**。
    if (from_revision > 0) {
        int64_t catchup_deadline_us = butil::gettimeofday_us() + 5000000;
        if (server_deadline_us > 0 && server_deadline_us < catchup_deadline_us) {
            catchup_deadline_us = server_deadline_us;
        }
        while (store->CurrentRevision() < from_revision) {
            if (canceled && (*canceled)()) {
                return true;  // 客户端断开：返回空结果
            }
            if (butil::gettimeofday_us() >= catchup_deadline_us) {
                out->code = Code::INTERNAL;
                out->message = "local node behind requested revision, retry";
                out->current_revision = store->CurrentRevision();
                return false;
            }
            bthread_usleep(5000);  // 挂起当前 bthread，不占 pthread worker
        }
    }

    // 2) 先注册后重放：关闭"历史重放"与"广播新事件"之间的窗口。
    //    注册后到达的写走广播；注册前已提交的写走重放。两个集合不相交且并集完备。
    //    from_revision == 0 表示"从现在开始看"，不重放历史（注册前已提交的事件天然不返回）。
    auto waiter = std::make_shared<Waiter>();
    waiter->key_filter = key;
    Register(waiter);

    if (from_revision > 0) {
        const int64_t cur = store->CurrentRevision();
        std::vector<WatchEvent> replay;
        int32_t rcode = Code::OK;
        if (store->ReplayEvents(from_revision, cur, key,
                                static_cast<int>(max_events_per_waiter_), &replay,
                                &rcode)) {
            if (!replay.empty()) {
                Unregister(waiter);
                out->events = std::move(replay);
                // 锚点 = 最后一条事件的 revision（绝不用 store current，否则续传漏事件）
                out->current_revision = out->events.back().revision();
                return true;
            }
        } else if (rcode == Code::COMPACTED) {
            Unregister(waiter);
            out->code = Code::COMPACTED;
            out->message =
                "requested revision has been compacted, resume from current_revision";
            out->current_revision = store->CurrentRevision();
            return false;
        }
    }

    // 3) 长轮询等待。deadline = min(now + timeout, server_deadline)。
    timespec deadline = butil::microseconds_from_now(timeout_ms * 1000LL);
    if (server_deadline_us > 0) {
        const timespec sd = FromEpochUs(server_deadline_us);
        if (TimespecLess(sd, deadline)) {
            deadline = sd;
        }
    }

    {
        std::unique_lock<bthread::Mutex> lock(waiter->mu);
        while (waiter->events.empty() && !waiter->overflow) {
            if (canceled && (*canceled)()) {
                break;  // 客户端断开/服务器关闭连接：及时释放 waiter
            }
            // wait_until 在 bthread 上挂起本 bthread、释放 pthread worker；
            // 仅 ETIMEDOUT 视为超时退出，其余返回值视为伪唤醒，回到循环重查 predicate。
            if (waiter->cv.wait_until(lock, deadline) == ETIMEDOUT) {
                break;
            }
        }
    }

    // 先注销（hub 集合不再引用本 waiter），再丢弃唯一强引用。Wait 线程是强引用持有者，
    // 注销先于释放 → 广播侧即使持旧快照也只会 lock() 失败跳过，无 UAF。
    Unregister(waiter);

    if (waiter->overflow) {
        out->code = Code::INTERNAL;
        out->message =
            "watch buffer overflow (slow consumer), reconnect with current_revision";
        out->current_revision = store->CurrentRevision();
        return false;
    }
    if (!waiter->events.empty()) {
        out->events.reserve(waiter->events.size());
        for (auto& e : waiter->events) {
            out->events.push_back(std::move(e));  // deque → vector（WatchResult 契约）
        }
        out->current_revision = out->events.back().revision();
        return true;
    }
    // 超时 / 取消：返回空结果 + 当前 revision，客户端立即以该 revision 重连（保持订阅不中断）。
    out->current_revision = store->CurrentRevision();
    return true;
}

}  // namespace configraft
