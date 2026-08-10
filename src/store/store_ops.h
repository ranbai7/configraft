#pragma once

#include "configraft.pb.h"
#include "raft/node.h"
#include "store/store.h"

namespace configraft {

using namespace v1;  // 协议类型（RaftCmd/Code 等）定义在 configraft.v1 包

// 把 RaftCmd 应用到 Store 的共享写逻辑。
// 单机模式（LocalNode）与集群模式（StateMachine::on_apply）共用同一份实现，
// 保证"状态只在 on_apply 中修改"的一致性约定不因模式切换而漂移。
//
// 注意：必须在串行上下文调用（LocalNode 的 Apply 本身串行；集群下由 braft
// on_apply 串行执行）。
void ApplyCmdToStore(Store* store, const RaftCmd& cmd, ApplyResult* out);

}  // namespace configraft
