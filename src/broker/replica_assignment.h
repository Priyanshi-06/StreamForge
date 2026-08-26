#pragma once

#include <cstdint>
#include <vector>

namespace minikafka {

// Deterministic, pure partition -> replica broker-id assignment. Every
// broker in the cluster computes this independently and always agrees,
// as long as they're given the same sortedBrokerIds and replicationFactor
// - no coordination protocol needed. Round-robin starting at
// partitionIndex within sortedBrokerIds (callers must supply it already
// sorted ascending - see ClusterConfig, which guarantees this regardless
// of the order operators typed brokers in). Returns
// min(replicationFactor, sortedBrokerIds.size()) distinct broker ids
// (clamped rather than erroring if RF exceeds the broker count); index
// [0] of the result is the leader. Empty if sortedBrokerIds is empty.
std::vector<int32_t> assignReplicas(int32_t partitionIndex,
                                     const std::vector<int32_t>& sortedBrokerIds,
                                     int32_t replicationFactor);

}  // namespace minikafka
