#pragma once

#include <cstdio>
#include <filesystem>
#include <optional>
#include <utility>
#include <vector>

#include "storage/record.h"

namespace minikafka {

// A single append-only log file, identified by the offset of its first
// record (baseOffset). Not thread-safe on its own - PartitionLog owns the
// concurrency control.
class LogSegment {
public:
    LogSegment(std::filesystem::path filePath, int64_t baseOffset);
    ~LogSegment();

    LogSegment(const LogSegment&) = delete;
    LogSegment& operator=(const LogSegment&) = delete;

    // Opens (creating if needed) the backing file for read+write.
    void open();

    // Appends a record and returns the file position it was written at.
    // Caller is responsible for assigning a monotonically increasing
    // r.offset. Flushes to disk before returning.
    size_t append(const Record& r);

    // Reads the record starting at the exact file position `filePos`.
    // Returns nullopt if the position is out of range.
    std::optional<Record> readAt(size_t filePos);

    struct RecoveryResult {
        int64_t lastOffset = -1;  // -1 if segment is empty
        size_t validBytes = 0;    // file size after truncating any torn/corrupt tail
        std::vector<std::pair<int64_t, size_t>> index;  // offset -> filePos, ascending
    };

    // Scans the segment from the start, validating each record's CRC and
    // bounds. The first record that is torn (not enough bytes on disk to
    // hold its declared length) or fails CRC/bounds validation marks the
    // end of valid data: the file is truncated back to that point, since
    // in an append-only log only the tail can ever be partially written.
    // Must be called once after open(), before any append().
    RecoveryResult recover();

    // Forces the OS to persist pending writes to durable storage
    // (_commit on Windows, fsync on POSIX).
    void flush();

    // Binary search over the in-memory offset->filePos index (populated by
    // recover() and kept up to date by append()). Offsets are contiguous
    // and strictly increasing per segment, so this is an exact lookup.
    std::optional<size_t> findFilePos(int64_t offset) const;

    // -1 if the segment currently holds no records.
    int64_t lastOffset() const { return index_.empty() ? -1 : index_.back().first; }

    size_t sizeBytes() const { return writePos_; }
    int64_t baseOffset() const { return baseOffset_; }
    const std::filesystem::path& path() const { return path_; }

private:
    std::filesystem::path path_;
    int64_t baseOffset_;
    std::FILE* file_ = nullptr;
    size_t writePos_ = 0;  // current logical end-of-file; next append lands here
    std::vector<std::pair<int64_t, size_t>> index_;  // offset -> filePos, ascending
};

}  // namespace minikafka
