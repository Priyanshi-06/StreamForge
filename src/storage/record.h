#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace minikafka {

// A single log record. `key`/`value` are nullopt to represent a null key
// or a tombstone (deleted) value respectively.
struct Record {
    int64_t offset = 0;
    int64_t timestampMs = 0;
    std::optional<std::string> key;
    std::optional<std::string> value;
};

// On-disk / wire encoding (identical for both):
//   [4 bytes]  length       BE u32  - bytes following this field
//   [4 bytes]  crc32        BE u32  - over offset..end of value
//   [8 bytes]  offset       BE i64
//   [8 bytes]  timestamp    BE i64
//   [4 bytes]  key length   BE i32  (-1 = null key)
//   [key bytes]
//   [4 bytes]  value length BE i32  (-1 = tombstone)
//   [value bytes]
std::vector<uint8_t> encodeRecord(const Record& r);

// Attempts to decode a record from `buf`, which has `available` bytes.
// Returns false if `available` is insufficient to even know the declared
// length, or insufficient to hold the full declared record (a torn read -
// the caller should treat this as "need more bytes" / end of valid data).
// On true, `outTotalBytes` is the number of bytes the record occupies
// (length field + body) and `crcOk` reports whether the CRC matched -
// callers doing crash recovery should treat crcOk == false as corruption
// and stop/truncate at this point, exactly like a torn read.
bool tryDecodeRecord(const uint8_t* buf, size_t available, Record& outRecord,
                      size_t& outTotalBytes, bool& crcOk);

}  // namespace minikafka
