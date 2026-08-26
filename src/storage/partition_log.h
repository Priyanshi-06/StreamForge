#pragma once

#include <filesystem>
#include <memory>
#include <mutex>
#include <vector>

#include "storage/log_segment.h"
#include "storage/record.h"

namespace minikafka {

// Manages the ordered set of segments that make up one partition's log:
// segment rolling, offset assignment, append, and offset-based fetch.
// Thread-safe: guarded by a single mutex covering append + index access.
// Callers must not hold onto returned data across further calls while
// expecting exclusivity - fetch() copies records out before returning.
class PartitionLog {
public:
    explicit PartitionLog(std::filesystem::path dir, size_t segmentMaxBytes = 16 * 1024 * 1024);

    // Scans `dir` for existing segment files, opens and recovers each in
    // baseOffset order, and restores nextOffset() to resume where the
    // previous run left off. Creates the directory and an initial empty
    // segment if none exists.
    void open();

    // Assigns the next sequential offset, appends {key, value} with the
    // current wall-clock timestamp, and returns the assigned offset.
    int64_t append(const std::string& key, const std::string& value);

    // For follower replication only: appends r verbatim (its offset,
    // ORIGINAL timestamp, key, and value all taken as-is from the leader -
    // not regenerated) so a replica's bytes match the leader's exactly,
    // not just its offsets. r.offset must equal nextOffset() exactly;
    // returns false with no state change on a gap or duplicate. Callers
    // (ReplicationFetcher) should treat false as a fatal desync for this
    // partition - no auto-repair exists in this phase.
    bool appendReplicated(const Record& r);

    // Reads up to maxRecords starting at startOffset (clamped to the
    // earliest available offset if startOffset is before it). Returns an
    // empty vector if startOffset >= nextOffset() (consumer caught up).
    std::vector<Record> fetch(int64_t startOffset, int32_t maxRecords);

    int64_t nextOffset() const;

private:
    void rollIfNeeded();
    LogSegment* segmentContaining(int64_t offset) const;  // nullptr if offset out of range
    std::filesystem::path segmentFileName(int64_t baseOffset) const;

    std::filesystem::path dir_;
    size_t segmentMaxBytes_;
    std::vector<std::unique_ptr<LogSegment>> segments_;  // ascending baseOffset
    int64_t nextOffset_ = 0;
    mutable std::mutex mutex_;
};

}  // namespace minikafka
