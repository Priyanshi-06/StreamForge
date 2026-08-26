#pragma once

#include <deque>
#include <mutex>
#include <string>
#include <vector>

namespace minikafka {

// Thread-safe ring buffer of recent broker activity lines, so both the
// console (BrokerServer::logActivity) and the HTTP admin dashboard can
// show what's been happening without re-plumbing a second logging path.
class ActivityLog {
public:
    explicit ActivityLog(size_t maxEntries = 200);

    void add(const std::string& line);

    // Oldest first.
    std::vector<std::string> recent() const;

private:
    size_t maxEntries_;
    mutable std::mutex mutex_;
    std::deque<std::string> lines_;
};

}  // namespace minikafka
