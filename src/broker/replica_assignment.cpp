#include "broker/replica_assignment.h"

#include <algorithm>

namespace minikafka {

std::vector<int32_t> assignReplicas(int32_t partitionIndex,
                                     const std::vector<int32_t>& sortedBrokerIds,
                                     int32_t replicationFactor) {
    std::vector<int32_t> result;

    int32_t n = static_cast<int32_t>(sortedBrokerIds.size());
    if (n == 0) return result;

    int32_t count = std::min(std::max(replicationFactor, 1), n);
    int32_t start = ((partitionIndex % n) + n) % n;  // defensive against a negative index

    for (int32_t i = 0; i < count; ++i) {
        result.push_back(sortedBrokerIds[(start + i) % n]);
    }
    return result;
}

}  // namespace minikafka
