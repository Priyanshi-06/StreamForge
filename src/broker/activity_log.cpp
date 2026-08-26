#include "broker/activity_log.h"

namespace minikafka {

ActivityLog::ActivityLog(size_t maxEntries) : maxEntries_(maxEntries) {}

void ActivityLog::add(const std::string& line) {
    std::lock_guard<std::mutex> lock(mutex_);
    lines_.push_back(line);
    while (lines_.size() > maxEntries_) {
        lines_.pop_front();
    }
}

std::vector<std::string> ActivityLog::recent() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return std::vector<std::string>(lines_.begin(), lines_.end());
}

}  // namespace minikafka
