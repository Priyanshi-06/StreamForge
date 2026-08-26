#pragma once

#include <cstdint>
#include <filesystem>
#include <mutex>
#include <string>
#include <unordered_map>

namespace minikafka {

// Broker-side consumer group offset tracking, persisted as a flat text
// file (`<group> <topic> <partition> <offset>` per line, rewritten in full
// on each commit). Deliberately not a `__consumer_offsets` log-topic -
// that's authentic to real Kafka but is meaningfully more machinery than
// this project needs; a flat file fully satisfies "resume after
// disconnect" for a single consumer per (group, topic, partition).
class ConsumerOffsetStore {
public:
    explicit ConsumerOffsetStore(std::filesystem::path filePath);

    void open();

    void commit(const std::string& group, const std::string& topic, int32_t partition,
                int64_t offset);

    // Returns -1 if no offset has been committed for this
    // (group, topic, partition) yet.
    int64_t fetch(const std::string& group, const std::string& topic, int32_t partition) const;

private:
    struct Entry {
        std::string group;
        std::string topic;
        int32_t partition;
        int64_t offset;
    };

    static std::string makeKey(const std::string& group, const std::string& topic,
                                int32_t partition);
    void persist() const;  // caller must hold mutex_

    std::filesystem::path filePath_;
    mutable std::mutex mutex_;
    std::unordered_map<std::string, Entry> entries_;
};

}  // namespace minikafka
