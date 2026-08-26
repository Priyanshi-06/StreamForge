#include "broker/consumer_offsets.h"

#include <fstream>
#include <sstream>

namespace minikafka {

ConsumerOffsetStore::ConsumerOffsetStore(std::filesystem::path filePath)
    : filePath_(std::move(filePath)) {}

std::string ConsumerOffsetStore::makeKey(const std::string& group, const std::string& topic,
                                          int32_t partition) {
    // \x01 is a unit separator - not expected in group/topic names, avoids
    // ambiguity that plain-space concatenation could introduce.
    return group + '\x01' + topic + '\x01' + std::to_string(partition);
}

void ConsumerOffsetStore::open() {
    std::lock_guard<std::mutex> lock(mutex_);

    std::ifstream in(filePath_);
    if (!in) return;

    std::string line;
    while (std::getline(in, line)) {
        if (line.empty()) continue;
        std::istringstream iss(line);
        Entry e;
        if (iss >> e.group >> e.topic >> e.partition >> e.offset) {
            entries_[makeKey(e.group, e.topic, e.partition)] = e;
        }
    }
}

void ConsumerOffsetStore::commit(const std::string& group, const std::string& topic,
                                  int32_t partition, int64_t offset) {
    std::lock_guard<std::mutex> lock(mutex_);
    entries_[makeKey(group, topic, partition)] = Entry{group, topic, partition, offset};
    persist();
}

int64_t ConsumerOffsetStore::fetch(const std::string& group, const std::string& topic,
                                    int32_t partition) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = entries_.find(makeKey(group, topic, partition));
    return it == entries_.end() ? -1 : it->second.offset;
}

void ConsumerOffsetStore::persist() const {
    std::ofstream out(filePath_, std::ios::trunc);
    for (const auto& [key, e] : entries_) {
        out << e.group << ' ' << e.topic << ' ' << e.partition << ' ' << e.offset << '\n';
    }
}

}  // namespace minikafka
