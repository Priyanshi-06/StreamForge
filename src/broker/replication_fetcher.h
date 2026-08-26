#pragma once

#include <atomic>
#include <cstdint>
#include <string>
#include <thread>

#include "broker/activity_log.h"
#include "storage/partition_log.h"

namespace minikafka {

// Runs on a background thread for as long as this broker is a follower
// of one specific partition: repeatedly issues a normal Fetch to the
// partition's leader starting at the local log's current nextOffset(),
// and applies whatever comes back via PartitionLog::appendReplicated().
// A follower is deliberately just an "internal consumer" of the leader -
// this reuses the existing client-facing Fetch protocol rather than a
// separate push RPC, so it inherits code that's already tested.
class ReplicationFetcher {
public:
    ReplicationFetcher(std::string topic, int32_t partition, std::string leaderHost,
                        uint16_t leaderPort, PartitionLog& localLog, ActivityLog& activityLog);
    ~ReplicationFetcher();

    ReplicationFetcher(const ReplicationFetcher&) = delete;
    ReplicationFetcher& operator=(const ReplicationFetcher&) = delete;

    void start();
    void stop();  // blocks until the background thread has exited

    // For the dashboard's lag display: how far this follower has applied
    // vs. what the leader reported as of the last successful fetch.
    int64_t lastAppliedOffset() const { return lastAppliedOffset_.load(); }
    int64_t leaderHighWatermark() const { return leaderHighWatermark_.load(); }

private:
    void run();

    std::string topic_;
    int32_t partition_;
    std::string leaderHost_;
    uint16_t leaderPort_;
    PartitionLog& localLog_;
    ActivityLog& activityLog_;

    std::atomic<bool> running_{false};
    std::atomic<int64_t> lastAppliedOffset_{0};
    std::atomic<int64_t> leaderHighWatermark_{0};
    std::thread thread_;
};

}  // namespace minikafka
