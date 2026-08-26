#pragma once

#include <atomic>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include "broker/cluster_config.h"
#include "storage/partition_log.h"

namespace minikafka {

enum class PartitionRole { NotHosted, Leader, Follower };

// A topic: a name plus a fixed number of partitions, each backed by its
// own PartitionLog directory `<dataDir>/<name>-<index>/` - but only on
// the brokers actually assigned to host that partition (see
// broker/replica_assignment.h). A broker with no role in a given
// partition still knows about it (for Metadata/leader lookups) but
// doesn't open local storage for it.
class Topic {
public:
    Topic(std::string name, int32_t numPartitions, int32_t replicationFactor,
          std::filesystem::path dataDir);

    // Cluster-aware open: computes this broker's role (Leader/Follower/
    // NotHosted) for every partition via assignReplicas(), and only
    // opens/recovers local PartitionLog storage for partitions this
    // broker actually hosts.
    void open(const ClusterConfig& cluster);

    // Single-broker convenience overload: synthesizes a degenerate
    // 1-node cluster (this broker is id 0 and the only member), so every
    // partition is trivially Leader-and-locally-hosted here - byte-for-
    // byte the original single-broker behavior. Existing callers that
    // never heard of clusters keep working unchanged.
    void open();

    const std::string& name() const { return name_; }
    int32_t numPartitions() const { return numPartitions_; }
    int32_t replicationFactor() const { return replicationFactor_; }

    // Returns nullptr if index is out of range, OR if this broker doesn't
    // host that partition locally (PartitionRole::NotHosted).
    PartitionLog* partition(int32_t index);

    // Valid for ANY partition index in range, regardless of whether this
    // broker hosts it - it's just data computed by assignReplicas(), not
    // a property of local storage.
    PartitionRole roleOf(int32_t index) const;
    int32_t leaderBrokerId(int32_t index) const;         // -1 if index out of range
    std::vector<int32_t> replicaBrokerIds(int32_t index) const;  // index 0 = leader

    // Broker-side round-robin partition selection for produce requests
    // with no key. An atomic counter (rather than client-side state) so
    // it behaves correctly regardless of how many short-lived producer
    // CLI invocations are hitting this topic concurrently.
    int32_t nextRoundRobinPartition() {
        return static_cast<int32_t>(roundRobinCounter_.fetch_add(1, std::memory_order_relaxed) %
                                     static_cast<uint32_t>(numPartitions_));
    }

private:
    std::string name_;
    int32_t numPartitions_;
    int32_t replicationFactor_;
    std::filesystem::path dataDir_;
    std::vector<std::unique_ptr<PartitionLog>> partitions_;  // nullptr where NotHosted
    std::vector<std::vector<int32_t>> replicaAssignment_;    // per partition, index 0 = leader
    int32_t selfBrokerId_ = 0;                                // set by open(cluster)
    std::atomic<uint32_t> roundRobinCounter_{0};
};

}  // namespace minikafka
