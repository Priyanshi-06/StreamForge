#pragma once

#include <filesystem>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "broker/cluster_config.h"
#include "broker/topic.h"

namespace minikafka {

// In-memory name->Topic map, persisted to a flat `topics.meta` file
// (`name numPartitions replicationFactor` per line) under the data
// directory so topic configs survive a broker restart. On open(), also
// reconciles against on-disk `<name>-<index>` partition directories in
// case the meta file is missing or stale, so no partition data is
// silently orphaned.
class TopicRegistry {
public:
    // Cluster-aware constructor: topics opened by this registry only
    // create local partition storage for partitions this broker (per
    // `cluster`) actually leads or follows.
    TopicRegistry(std::filesystem::path dataDir, ClusterConfig cluster);

    // Single-broker convenience overload: synthesizes a degenerate
    // 1-node cluster, so every partition is hosted locally - byte-for-
    // byte the original single-broker behavior. Existing callers keep
    // working unchanged.
    explicit TopicRegistry(std::filesystem::path dataDir);

    const ClusterConfig& cluster() const { return cluster_; }

    void open();

    // Returns false (caller should report StatusCode::TopicExists) if a
    // topic with this name already exists.
    bool createTopic(const std::string& name, int32_t numPartitions, int32_t replicationFactor);

    // Returns nullptr if no such topic exists.
    Topic* getTopic(const std::string& name);

    struct TopicSummary {
        std::string name;
        int32_t numPartitions;
    };
    std::vector<TopicSummary> listTopics() const;

private:
    struct MetaEntry {
        std::string name;
        int32_t numPartitions;
        int32_t replicationFactor;
    };
    std::vector<MetaEntry> loadMetaFile() const;
    std::vector<MetaEntry> scanOnDiskTopics() const;
    void persistMeta() const;

    std::filesystem::path dataDir_;
    std::filesystem::path metaFilePath_;
    ClusterConfig cluster_;
    mutable std::mutex mutex_;
    std::unordered_map<std::string, std::unique_ptr<Topic>> topics_;
};

}  // namespace minikafka
