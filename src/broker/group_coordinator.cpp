#include "broker/group_coordinator.h"

namespace minikafka {

GroupCoordinator::GroupCoordinator(std::chrono::milliseconds leaseDuration)
    : leaseDuration_(leaseDuration) {}

std::string GroupCoordinator::makeKey(const std::string& group, const std::string& topic,
                                       int32_t partition) {
    return group + '\x01' + topic + '\x01' + std::to_string(partition);
}

bool GroupCoordinator::claim(const std::string& group, const std::string& topic,
                              int32_t partition, const std::string& consumerId) {
    std::lock_guard<std::mutex> lock(mutex_);

    std::string key = makeKey(group, topic, partition);
    auto now = std::chrono::steady_clock::now();

    auto it = leases_.find(key);
    if (it != leases_.end() && it->second.ownerId != consumerId && it->second.expiresAt > now) {
        return false;  // actively held by someone else
    }

    leases_[key] = Lease{group, topic, partition, consumerId, now + leaseDuration_};
    return true;
}

void GroupCoordinator::release(const std::string& group, const std::string& topic,
                                int32_t partition, const std::string& consumerId) {
    std::lock_guard<std::mutex> lock(mutex_);

    std::string key = makeKey(group, topic, partition);
    auto it = leases_.find(key);
    if (it != leases_.end() && it->second.ownerId == consumerId) {
        leases_.erase(it);
    }
}

bool GroupCoordinator::isOwner(const std::string& group, const std::string& topic,
                                int32_t partition, const std::string& consumerId) const {
    std::lock_guard<std::mutex> lock(mutex_);

    std::string key = makeKey(group, topic, partition);
    auto it = leases_.find(key);
    if (it == leases_.end()) return false;
    if (it->second.ownerId != consumerId) return false;
    return it->second.expiresAt > std::chrono::steady_clock::now();
}

bool GroupCoordinator::isFree(const std::string& group, const std::string& topic,
                               int32_t partition) const {
    std::lock_guard<std::mutex> lock(mutex_);

    std::string key = makeKey(group, topic, partition);
    auto it = leases_.find(key);
    if (it == leases_.end()) return true;
    return it->second.expiresAt <= std::chrono::steady_clock::now();
}

std::vector<GroupCoordinator::LeaseSnapshot> GroupCoordinator::listLeases() const {
    std::lock_guard<std::mutex> lock(mutex_);

    std::vector<LeaseSnapshot> result;
    auto now = std::chrono::steady_clock::now();
    for (const auto& [key, lease] : leases_) {
        if (lease.expiresAt <= now) continue;  // expired - don't report stale leases
        auto remaining =
            std::chrono::duration_cast<std::chrono::milliseconds>(lease.expiresAt - now).count();
        result.push_back(LeaseSnapshot{lease.group, lease.topic, lease.partition, lease.ownerId,
                                        remaining});
    }
    return result;
}

}  // namespace minikafka
