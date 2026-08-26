#include "storage/partition_log.h"

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <sstream>

namespace minikafka {

namespace {

int64_t currentTimeMs() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

// Segment filenames are 20-digit zero-padded base offsets, e.g.
// "00000000000000000000.log", matching Kafka's own convention.
bool parseBaseOffsetFromFilename(const std::string& stem, int64_t& outOffset) {
    if (stem.size() != 20) return false;
    for (char c : stem) {
        if (c < '0' || c > '9') return false;
    }
    outOffset = std::stoll(stem);
    return true;
}

}  // namespace

PartitionLog::PartitionLog(std::filesystem::path dir, size_t segmentMaxBytes)
    : dir_(std::move(dir)), segmentMaxBytes_(segmentMaxBytes) {}

std::filesystem::path PartitionLog::segmentFileName(int64_t baseOffset) const {
    char buf[32];
    std::snprintf(buf, sizeof(buf), "%020lld.log", static_cast<long long>(baseOffset));
    return dir_ / buf;
}

void PartitionLog::open() {
    std::lock_guard<std::mutex> lock(mutex_);

    std::filesystem::create_directories(dir_);

    std::vector<int64_t> baseOffsets;
    for (const auto& entry : std::filesystem::directory_iterator(dir_)) {
        if (!entry.is_regular_file()) continue;
        if (entry.path().extension() != ".log") continue;
        int64_t base;
        if (parseBaseOffsetFromFilename(entry.path().stem().string(), base)) {
            baseOffsets.push_back(base);
        }
    }
    std::sort(baseOffsets.begin(), baseOffsets.end());

    if (baseOffsets.empty()) {
        baseOffsets.push_back(0);
    }

    int64_t maxOffsetSeen = -1;
    for (int64_t base : baseOffsets) {
        auto seg = std::make_unique<LogSegment>(segmentFileName(base), base);
        seg->open();
        LogSegment::RecoveryResult result = seg->recover();
        if (result.lastOffset > maxOffsetSeen) {
            maxOffsetSeen = result.lastOffset;
        }
        segments_.push_back(std::move(seg));
    }

    nextOffset_ = maxOffsetSeen + 1;
}

void PartitionLog::rollIfNeeded() {
    if (!segments_.empty() && segments_.back()->sizeBytes() < segmentMaxBytes_) return;

    auto seg = std::make_unique<LogSegment>(segmentFileName(nextOffset_), nextOffset_);
    seg->open();
    seg->recover();  // new/empty file - trivial no-op, keeps state consistent
    segments_.push_back(std::move(seg));
}

int64_t PartitionLog::append(const std::string& key, const std::string& value) {
    std::lock_guard<std::mutex> lock(mutex_);

    rollIfNeeded();

    Record r;
    r.offset = nextOffset_;
    r.timestampMs = currentTimeMs();
    r.key = key;
    r.value = value;

    segments_.back()->append(r);
    return nextOffset_++;
}

bool PartitionLog::appendReplicated(const Record& r) {
    std::lock_guard<std::mutex> lock(mutex_);

    if (r.offset != nextOffset_) return false;  // gap or duplicate - fatal desync, don't repair

    rollIfNeeded();
    segments_.back()->append(r);
    ++nextOffset_;
    return true;
}

LogSegment* PartitionLog::segmentContaining(int64_t offset) const {
    auto it = std::upper_bound(
        segments_.begin(), segments_.end(), offset,
        [](int64_t value, const std::unique_ptr<LogSegment>& seg) { return value < seg->baseOffset(); });
    if (it == segments_.begin()) return nullptr;
    --it;
    return it->get();
}

std::vector<Record> PartitionLog::fetch(int64_t startOffset, int32_t maxRecords) {
    std::lock_guard<std::mutex> lock(mutex_);

    std::vector<Record> out;
    if (segments_.empty()) return out;

    int64_t offset = startOffset;
    if (offset < segments_.front()->baseOffset()) {
        offset = segments_.front()->baseOffset();
    }

    while (static_cast<int32_t>(out.size()) < maxRecords && offset < nextOffset_) {
        LogSegment* seg = segmentContaining(offset);
        if (!seg) break;
        auto posOpt = seg->findFilePos(offset);
        if (!posOpt) break;
        auto recOpt = seg->readAt(*posOpt);
        if (!recOpt) break;
        out.push_back(*recOpt);
        ++offset;
    }

    return out;
}

int64_t PartitionLog::nextOffset() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return nextOffset_;
}

}  // namespace minikafka
