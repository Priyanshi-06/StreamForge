#pragma once

#include <chrono>
#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace minikafka {

// Lease-based ownership of a (group, topic, partition) by a single
// consumer at a time, so two consumers in the same group can't both
// process the same partition concurrently and race on offset commits.
//
// This is deliberately simpler than real Kafka's consumer-group
// rebalancing: a consumer explicitly claims the specific partition it
// wants (JoinGroup) rather than the broker automatically distributing a
// topic's partitions across whichever consumers happen to be in the
// group. There is no cross-partition redistribution when membership
// changes. A crashed consumer that stops sending Heartbeat simply has its
// lease expire, freeing the partition for the next claimant - purely
// in-memory, since group membership is inherently session-scoped and
// doesn't need to survive a broker restart the way committed offsets do.
class GroupCoordinator {
public:
    explicit GroupCoordinator(std::chrono::milliseconds leaseDuration = std::chrono::seconds(10));

    // Claims or refreshes the lease for consumerId. Succeeds (returns
    // true) if the partition is unowned, its lease has expired, or
    // consumerId is already the current owner. Fails if a different
    // consumerId holds a still-valid lease.
    bool claim(const std::string& group, const std::string& topic, int32_t partition,
               const std::string& consumerId);

    // Releases the lease if held by consumerId. No-op (not an error) if
    // consumerId doesn't currently hold it.
    void release(const std::string& group, const std::string& topic, int32_t partition,
                 const std::string& consumerId);

    // True if consumerId currently holds a non-expired lease.
    bool isOwner(const std::string& group, const std::string& topic, int32_t partition,
                 const std::string& consumerId) const;

    // Snapshot of one active (non-expired) lease, for the HTTP admin
    // dashboard to display.
    struct LeaseSnapshot {
        std::string group;
        std::string topic;
        int32_t partition;
        std::string consumerId;
        int64_t remainingMs;  // always > 0
    };

    // All currently non-expired leases, in no particular order.
    std::vector<LeaseSnapshot> listLeases() const;

    // True if no consumer currently holds a valid lease on this
    // (group, topic, partition), regardless of who might have held it
    // before it expired.
    bool isFree(const std::string& group, const std::string& topic, int32_t partition) const;

private:
    struct Lease {
        std::string group;
        std::string topic;
        int32_t partition;
        std::string ownerId;
        std::chrono::steady_clock::time_point expiresAt;
    };

    static std::string makeKey(const std::string& group, const std::string& topic,
                                int32_t partition);

    std::chrono::milliseconds leaseDuration_;
    mutable std::mutex mutex_;
    std::unordered_map<std::string, Lease> leases_;
};

}  // namespace minikafka
