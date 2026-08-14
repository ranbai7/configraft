#include <gflags/gflags.h>

#include <string>

#include "braft/raft.h"
#include "common/log.h"
#include "server/server.h"

namespace braft {
DECLARE_bool(raft_enable_leader_lease);
}  // namespace braft

DEFINE_int32(port, 8000, "listen port");
DEFINE_string(listen_ip, "127.0.0.1", "listen ip (also the raft peer address)");
DEFINE_string(data_dir, "data", "leveldb data directory");
DEFINE_string(node, "", "node name, empty for single-node mode (M2: e.g. node1)");
DEFINE_string(peers, "",
              "raft peers, comma-separated (M2: e.g. 127.0.0.1:8001,127.0.0.1:8002,127.0.0.1:8003)");
DEFINE_int32(election_timeout_ms, 1000, "raft election timeout in ms");

int main(int argc, char* argv[]) {
    google::ParseCommandLineFlags(&argc, &argv, true);

    // M5 线性一致读依赖 braft leader lease。在 Parse 之后强制开启：若在 Parse 前设置，
    // 用户命令行传 --raft_enable_leader_lease=false 会被 Parse 覆盖，静默退回"leader 直接读"
    // 的不安全模式。所有 peer 必须一致开启（follower 才拒绝向旧 leader 投票）。
    braft::FLAGS_raft_enable_leader_lease = true;

    configraft::ServerOptions opts;
    opts.port = FLAGS_port;
    opts.listen_ip = FLAGS_listen_ip;
    opts.data_dir = FLAGS_data_dir;
    opts.node_name = FLAGS_node;
    opts.peers = FLAGS_peers;
    opts.election_timeout_ms = FLAGS_election_timeout_ms;

    configraft::ConfigraftServer server;
    std::string err;
    if (!server.Init(opts, &err)) {
        LOG(ERROR) << "init failed: " << err;
        return 1;
    }
    LOG(INFO) << "server ready, press Ctrl+C to quit";
    server.RunUntilAskedToQuit();
    return 0;
}
