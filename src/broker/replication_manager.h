#pragma once

#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>

#include "broker/activity_log.h"
#include "broker/cluster_config.h"
#include "broker/replication_fetcher.h"
#include "broker/topic_registry.h"

namespace minikafka {

// Owned by BrokerServer. Starts/stops ReplicationFetcher threads for
// whatever partitions this broker currently follows. ensureFollowing()
// is idempotent (safe to call repeatedly, e.g. once per partition right
// after a topic is opened or announced) - it only starts a fetcher if
// this broker's role for that partition is actually Follower and one
// isn't already running.
class ReplicationManager {
public:
    ReplicationManager(TopicRegistry& registry, ActivityLog& activityLog);
    ~ReplicationManager();

    ReplicationManager(const ReplicationManager&) = delete;
    ReplicationManager& operator=(const ReplicationManager&) = delete;

    void ensureFollowing(const std::string& topicName, int32_t partition);

    // Calls ensureFollowing() for every partition of every currently-
    // known topic - used right after startup and after AnnounceTopic.
    void reconcileAll();

    void stopAll();

    // For the dashboard: -1 if not currently following this partition.
    struct FollowerLag {
        int64_t lastAppliedOffset;
        int64_t leaderHighWatermark;
    };
    std::optional<FollowerLag> lagFor(const std::string& topicName, int32_t partition) const;

private:
    TopicRegistry& registry_;
    ActivityLog& activityLog_;
    mutable std::mutex mutex_;
    std::unordered_map<std::string, std::unique_ptr<ReplicationFetcher>> fetchers_;
};

}  // namespace minikafka
