#include "broker/replication_manager.h"

#include "broker/topic.h"

namespace minikafka {

namespace {
std::string makeKey(const std::string& topic, int32_t partition) {
    return topic + '\x01' + std::to_string(partition);
}
}  // namespace

ReplicationManager::ReplicationManager(TopicRegistry& registry, ActivityLog& activityLog)
    : registry_(registry), activityLog_(activityLog) {}

ReplicationManager::~ReplicationManager() { stopAll(); }

void ReplicationManager::ensureFollowing(const std::string& topicName, int32_t partition) {
    Topic* topic = registry_.getTopic(topicName);
    if (!topic) return;
    if (topic->roleOf(partition) != PartitionRole::Follower) return;

    std::string key = makeKey(topicName, partition);
    std::lock_guard<std::mutex> lock(mutex_);
    if (fetchers_.count(key)) return;  // already following

    int32_t leaderId = topic->leaderBrokerId(partition);
    const BrokerAddress* leaderAddr = registry_.cluster().find(leaderId);
    if (!leaderAddr) return;  // shouldn't happen with a valid cluster config

    PartitionLog* localLog = topic->partition(partition);
    if (!localLog) return;  // shouldn't happen if role is Follower

    auto fetcher = std::make_unique<ReplicationFetcher>(topicName, partition, leaderAddr->host,
                                                          leaderAddr->port, *localLog, activityLog_);
    fetcher->start();
    fetchers_[key] = std::move(fetcher);
}

void ReplicationManager::reconcileAll() {
    for (const auto& summary : registry_.listTopics()) {
        Topic* topic = registry_.getTopic(summary.name);
        if (!topic) continue;
        for (int32_t i = 0; i < topic->numPartitions(); ++i) {
            ensureFollowing(summary.name, i);
        }
    }
}

void ReplicationManager::stopAll() {
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto& [key, fetcher] : fetchers_) {
        fetcher->stop();
    }
    fetchers_.clear();
}

std::optional<ReplicationManager::FollowerLag> ReplicationManager::lagFor(
    const std::string& topicName, int32_t partition) const {
    std::string key = makeKey(topicName, partition);
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = fetchers_.find(key);
    if (it == fetchers_.end()) return std::nullopt;
    return FollowerLag{it->second->lastAppliedOffset(), it->second->leaderHighWatermark()};
}

}  // namespace minikafka
