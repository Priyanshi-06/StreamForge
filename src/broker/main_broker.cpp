// minikafka-broker: starts a broker in the (possibly single-node) cluster.
//
// Usage: minikafka-broker.exe <dataDir> <tcpPort>
//                              [--broker-id=N] [--cluster=id:host:port,...]
//
// --broker-id and --cluster must be given together (or not at all, which
// defaults to a single-node cluster where this broker is id 0) - every
// broker in the cluster must be started with the SAME --cluster list for
// them to agree on partition leadership. See broker/cluster_config.h.

#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>

#include "broker/broker_server.h"
#include "common/net/socket.h"
#include "common/util/logger.h"

int main(int argc, char** argv) {
    minikafka::Socket::globalInit();

    std::string dataDir = "data";
    uint16_t port = 9092;
    bool haveBrokerId = false;
    int32_t brokerId = 0;
    bool haveCluster = false;
    std::string clusterArg;

    std::vector<std::string> positional;
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg.rfind("--broker-id=", 0) == 0) {
            brokerId = std::atoi(arg.substr(12).c_str());
            haveBrokerId = true;
        } else if (arg.rfind("--cluster=", 0) == 0) {
            clusterArg = arg.substr(10);
            haveCluster = true;
        } else {
            positional.push_back(arg);
        }
    }

    if (positional.size() > 0) dataDir = positional[0];
    if (positional.size() > 1) port = static_cast<uint16_t>(std::atoi(positional[1].c_str()));

    if (haveBrokerId != haveCluster) {
        Logger::error("--broker-id and --cluster must be given together (or neither, for a "
                      "single-node cluster).");
        return 1;
    }

    try {
        minikafka::ClusterConfig cluster =
            haveCluster ? minikafka::ClusterConfig::parse(brokerId, clusterArg)
                        : minikafka::ClusterConfig::singleNode("127.0.0.1", port);

        minikafka::BrokerServer server(dataDir, port, cluster);
        server.run();
    } catch (const std::exception& e) {
        Logger::error(std::string("Broker error: ") + e.what());
        return 1;
    }
    return 0;
}
