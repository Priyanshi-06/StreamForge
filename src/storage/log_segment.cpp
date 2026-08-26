#include "storage/log_segment.h"

#include <algorithm>
#include <cstring>
#include <stdexcept>
#include <system_error>

#include "common/net/byteorder.h"

#ifdef _WIN32
#include <io.h>
#else
#include <unistd.h>
#endif

namespace minikafka {

LogSegment::LogSegment(std::filesystem::path filePath, int64_t baseOffset)
    : path_(std::move(filePath)), baseOffset_(baseOffset) {}

LogSegment::~LogSegment() {
    if (file_) {
        std::fclose(file_);
        file_ = nullptr;
    }
}

void LogSegment::open() {
    if (file_) return;

    if (!std::filesystem::exists(path_)) {
        file_ = std::fopen(path_.string().c_str(), "w+b");
    } else {
        file_ = std::fopen(path_.string().c_str(), "r+b");
    }
    if (!file_) {
        throw std::runtime_error("LogSegment: failed to open " + path_.string());
    }

    std::fseek(file_, 0, SEEK_END);
    long endPos = std::ftell(file_);
    writePos_ = (endPos > 0) ? static_cast<size_t>(endPos) : 0;
}

size_t LogSegment::append(const Record& r) {
    std::vector<uint8_t> encoded = encodeRecord(r);

    std::fseek(file_, static_cast<long>(writePos_), SEEK_SET);
    size_t written = std::fwrite(encoded.data(), 1, encoded.size(), file_);
    if (written != encoded.size()) {
        throw std::runtime_error("LogSegment: short write to " + path_.string());
    }
    flush();

    size_t pos = writePos_;
    index_.emplace_back(r.offset, pos);
    writePos_ += encoded.size();
    return pos;
}

std::optional<size_t> LogSegment::findFilePos(int64_t offset) const {
    auto it = std::lower_bound(
        index_.begin(), index_.end(), offset,
        [](const std::pair<int64_t, size_t>& entry, int64_t value) { return entry.first < value; });
    if (it == index_.end() || it->first != offset) return std::nullopt;
    return it->second;
}

std::optional<Record> LogSegment::readAt(size_t filePos) {
    if (filePos >= writePos_) return std::nullopt;

    std::fseek(file_, static_cast<long>(filePos), SEEK_SET);

    uint8_t header[4];
    if (std::fread(header, 1, 4, file_) != 4) return std::nullopt;
    uint32_t length = getU32BE(header);

    std::vector<uint8_t> full(4 + static_cast<size_t>(length));
    std::memcpy(full.data(), header, 4);
    if (length > 0) {
        size_t got = std::fread(full.data() + 4, 1, length, file_);
        if (got != length) return std::nullopt;
    }

    Record rec;
    size_t totalBytes;
    bool crcOk;
    if (!tryDecodeRecord(full.data(), full.size(), rec, totalBytes, crcOk)) return std::nullopt;
    if (!crcOk) return std::nullopt;
    return rec;
}

LogSegment::RecoveryResult LogSegment::recover() {
    RecoveryResult result;

    std::fseek(file_, 0, SEEK_END);
    long fileSizeLong = std::ftell(file_);
    size_t fileSize = (fileSizeLong > 0) ? static_cast<size_t>(fileSizeLong) : 0;

    std::vector<uint8_t> buf(fileSize);
    if (fileSize > 0) {
        std::fseek(file_, 0, SEEK_SET);
        size_t got = std::fread(buf.data(), 1, fileSize, file_);
        buf.resize(got);
    }

    size_t consumed = 0;
    while (consumed < buf.size()) {
        Record rec;
        size_t totalBytes;
        bool crcOk;
        bool ok = tryDecodeRecord(buf.data() + consumed, buf.size() - consumed, rec, totalBytes, crcOk);
        if (!ok || !crcOk) {
            break;  // torn write or corruption - everything from here is an invalid tail
        }
        result.index.emplace_back(rec.offset, consumed);
        result.lastOffset = rec.offset;
        consumed += totalBytes;
    }

    result.validBytes = consumed;

    if (consumed != buf.size()) {
        // Truncate back to the last confirmed-valid record boundary. Safe
        // because in an append-only log, only the tail can ever be torn.
        std::fclose(file_);
        file_ = nullptr;
        std::error_code ec;
        std::filesystem::resize_file(path_, consumed, ec);
        file_ = std::fopen(path_.string().c_str(), "r+b");
        if (!file_) {
            throw std::runtime_error("LogSegment: failed to reopen after truncation " +
                                      path_.string());
        }
    }

    writePos_ = consumed;
    index_ = result.index;
    return result;
}

void LogSegment::flush() {
    std::fflush(file_);
#ifdef _WIN32
    _commit(_fileno(file_));
#else
    fsync(fileno(file_));
#endif
}

}  // namespace minikafka
