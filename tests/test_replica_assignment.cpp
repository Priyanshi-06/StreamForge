// Tests for the pure assignReplicas() function and ClusterConfig parsing
// - the foundation every broker relies on to independently agree on
// partition leadership without any coordination protocol.

#include <cassert>
#include <iostream>
#include <stdexcept>

#include "broker/cluster_config.h"
#include "broker/replica_assignment.h"

using namespace minikafka;

static void testDeterminismAndLeaderIsIndexZero() {
    std::vector<int32_t> brokers = {0, 1, 2};

    auto a = assignReplicas(0, brokers, 2);
    auto b = assignReplicas(0, brokers, 2);
    assert(a == b && "same inputs must always produce the same output");
    assert(a.size() == 2);
    assert(a[0] == 0 && "partition 0 should start at broker 0");

    std::cout << "[PASS] assignReplicas is deterministic, leader is index 0\n";
}

static void testRoundRobinAcrossPartitions() {
    std::vector<int32_t> brokers = {0, 1, 2};

    assert(assignReplicas(0, brokers, 1)[0] == 0);
    assert(assignReplicas(1, brokers, 1)[0] == 1);
    assert(assignReplicas(2, brokers, 1)[0] == 2);
    assert(assignReplicas(3, brokers, 1)[0] == 0);  // wraps around

    // RF=2: each partition's replica set should be 2 consecutive (wrapping) brokers.
    auto r0 = assignReplicas(0, brokers, 2);
    assert((r0 == std::vector<int32_t>{0, 1}));
    auto r2 = assignReplicas(2, brokers, 2);
    assert((r2 == std::vector<int32_t>{2, 0}));  // wraps

    std::cout << "[PASS] assignReplicas round-robins across partitions\n";
}

static void testReplicationFactorClamped() {
    std::vector<int32_t> brokers = {0, 1};

    auto result = assignReplicas(0, brokers, 5);
    assert(result.size() == 2 && "RF exceeding broker count must clamp, not crash or pad");

    std::cout << "[PASS] assignReplicas clamps RF to the broker count\n";
}

static void testEmptyBrokerList() {
    std::vector<int32_t> empty;
    auto result = assignReplicas(0, empty, 2);
    assert(result.empty());

    std::cout << "[PASS] assignReplicas returns empty for an empty broker list\n";
}

static void testClusterConfigOrderIndependence() {
    // Two ClusterConfigs built from the SAME broker set, typed in a
    // different order, must produce identical sorted ids - this is what
    // actually justifies "no coordination needed between brokers."
    ClusterConfig a = ClusterConfig::parse(0, "0:host0:9000,1:host1:9001,2:host2:9002");
    ClusterConfig b = ClusterConfig::parse(1, "2:host2:9002,0:host0:9000,1:host1:9001");

    assert(a.sortedBrokerIds() == b.sortedBrokerIds());
    assert((a.sortedBrokerIds() == std::vector<int32_t>{0, 1, 2}));

    assert(a.selfBrokerId() == 0);
    assert(b.selfBrokerId() == 1);

    const BrokerAddress* addr = a.find(1);
    assert(addr != nullptr);
    assert(addr->host == "host1");
    assert(addr->port == 9001);

    assert(a.find(99) == nullptr);

    std::cout << "[PASS] ClusterConfig is order-independent and looks up brokers correctly\n";
}

static void testClusterConfigValidation() {
    bool threw = false;
    try {
        ClusterConfig::parse(0, "0:host0:9000,0:host1:9001");  // duplicate id
    } catch (const std::runtime_error&) {
        threw = true;
    }
    assert(threw && "duplicate broker id must throw");

    threw = false;
    try {
        ClusterConfig::parse(5, "0:host0:9000,1:host1:9001");  // self not present
    } catch (const std::runtime_error&) {
        threw = true;
    }
    assert(threw && "selfBrokerId not in the cluster must throw");

    threw = false;
    try {
        ClusterConfig::parse(0, "not-a-valid-entry");
    } catch (const std::runtime_error&) {
        threw = true;
    }
    assert(threw && "malformed entry must throw");

    std::cout << "[PASS] ClusterConfig fails fast on misconfiguration\n";
}

static void testSingleNodeDefault() {
    ClusterConfig c = ClusterConfig::singleNode("127.0.0.1", 9092);
    assert(c.selfBrokerId() == 0);
    assert(c.sortedBrokerIds().size() == 1);
    assert(c.sortedBrokerIds()[0] == 0);

    auto replicas = assignReplicas(0, c.sortedBrokerIds(), 2);
    assert(replicas.size() == 1 && replicas[0] == 0);

    std::cout << "[PASS] ClusterConfig::singleNode behaves as a degenerate 1-node cluster\n";
}

int main() {
    testDeterminismAndLeaderIsIndexZero();
    testRoundRobinAcrossPartitions();
    testReplicationFactorClamped();
    testEmptyBrokerList();
    testClusterConfigOrderIndependence();
    testClusterConfigValidation();
    testSingleNodeDefault();
    std::cout << "All replica assignment tests passed.\n";
    return 0;
}
