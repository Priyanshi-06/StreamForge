#include "broker/topic.h"

#include "broker/replica_assignment.h"

namespace minikafka {

Topic::Topic(std::string name, int32_t numPartitions, int32_t replicationFactor,
             std::filesystem::path dataDir)
    : name_(std::move(name)),
      numPartitions_(numPartitions),
      replicationFactor_(replicationFactor),
      dataDir_(std::move(dataDir)) {}

void Topic::open(const ClusterConfig& cluster) {
    selfBrokerId_ = cluster.selfBrokerId();

    partitions_.clear();
    partitions_.resize(numPartitions_);
    replicaAssignment_.clear();
    replicaAssignment_.resize(numPartitions_);

    std::vector<int32_t> sortedIds = cluster.sortedBrokerIds();

    for (int32_t i = 0; i < numPartitions_; ++i) {
        replicaAssignment_[i] = assignReplicas(i, sortedIds, replicationFactor_);

        bool hostedHere = false;
        for (int32_t brokerId : replicaAssignment_[i]) {
            if (brokerId == selfBrokerId_) {
                hostedHere = true;
                break;
            }
        }
        if (!hostedHere) continue;  // stays nullptr - metadata-only on this broker

        std::filesystem::path partitionDir = dataDir_ / (name_ + "-" + std::to_string(i));
        auto log = std::make_unique<PartitionLog>(partitionDir);
        log->open();
        partitions_[i] = std::move(log);
    }
}

void Topic::open() { open(ClusterConfig::singleNode("localhost", 0)); }

PartitionLog* Topic::partition(int32_t index) {
    if (index < 0 || index >= static_cast<int32_t>(partitions_.size())) return nullptr;
    return partitions_[index].get();
}

PartitionRole Topic::roleOf(int32_t index) const {
    if (index < 0 || index >= static_cast<int32_t>(replicaAssignment_.size())) {
        return PartitionRole::NotHosted;
    }
    const auto& replicas = replicaAssignment_[index];
    if (replicas.empty()) return PartitionRole::NotHosted;

    if (replicas[0] == selfBrokerId_) return PartitionRole::Leader;
    for (int32_t brokerId : replicas) {
        if (brokerId == selfBrokerId_) return PartitionRole::Follower;
    }
    return PartitionRole::NotHosted;
}

int32_t Topic::leaderBrokerId(int32_t index) const {
    if (index < 0 || index >= static_cast<int32_t>(replicaAssignment_.size())) return -1;
    if (replicaAssignment_[index].empty()) return -1;
    return replicaAssignment_[index][0];
}

std::vector<int32_t> Topic::replicaBrokerIds(int32_t index) const {
    if (index < 0 || index >= static_cast<int32_t>(replicaAssignment_.size())) return {};
    return replicaAssignment_[index];
}

}  // namespace minikafka
