#include <gflags/gflags.h>

#include <string>

#include "common/log.h"
#include "server/server.h"

DEFINE_int32(port, 8000, "listen port");
DEFINE_string(listen_ip, "127.0.0.1", "listen ip (also the raft peer address)");
DEFINE_string(data_dir, "data", "leveldb data directory");
DEFINE_string(node, "", "node name, empty for single-node mode (M2: e.g. node1)");
DEFINE_string(peers, "",
              "raft peers, comma-separated (M2: e.g. 127.0.0.1:8001,127.0.0.1:8002,127.0.0.1:8003)");
DEFINE_int32(election_timeout_ms, 1000, "raft election timeout in ms");

int main(int argc, char* argv[]) {
    google::ParseCommandLineFlags(&argc, &argv, true);

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
