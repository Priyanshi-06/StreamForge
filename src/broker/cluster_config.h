#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace minikafka {

struct BrokerAddress {
    int32_t brokerId;
    std::string host;
    uint16_t port;
};

// Static, identical-across-all-brokers cluster membership, parsed from a
// "--cluster=id:host:port,id:host:port,..." style argument. Sorted by
// brokerId ascending at construction, so two ClusterConfigs built from
// the same broker set - regardless of what order operators typed them in
// - always agree on assignReplicas() results without any coordination.
class ClusterConfig {
public:
    // Degenerate single-broker cluster: this process is broker id 0 and
    // the only member. Used as the default so single-broker operation
    // (today's behavior) needs no special-cased code path anywhere else.
    static ClusterConfig singleNode(std::string host, uint16_t port);

    // Parses "id:host:port,id:host:port,...". Throws std::runtime_error
    // on a malformed entry, a duplicate broker id, or if selfBrokerId
    // isn't present in the list - fail fast rather than start in a
    // broken, half-configured state.
    static ClusterConfig parse(int32_t selfBrokerId, const std::string& clusterArg);

    int32_t selfBrokerId() const { return selfBrokerId_; }
    const std::vector<BrokerAddress>& brokers() const { return brokers_; }  // sorted by brokerId
    std::vector<int32_t> sortedBrokerIds() const;

    // nullptr if brokerId isn't a member of this cluster.
    const BrokerAddress* find(int32_t brokerId) const;

private:
    ClusterConfig(int32_t selfBrokerId, std::vector<BrokerAddress> brokers);

    int32_t selfBrokerId_;
    std::vector<BrokerAddress> brokers_;  // sorted by brokerId ascending
};

}  // namespace minikafka
