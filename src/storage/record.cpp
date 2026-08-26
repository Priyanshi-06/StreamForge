#include "storage/record.h"

#include <cstring>

#include "common/net/byteorder.h"
#include "common/util/crc32.h"

namespace minikafka {

namespace {
// Minimum bytes in the body-after-crc region: offset(8) + timestamp(8) +
// keyLen(4) + valueLen(4), before any variable-length key/value bytes.
constexpr size_t kBodyAfterCrcFixedSize = 8 + 8 + 4 + 4;
}  // namespace

std::vector<uint8_t> encodeRecord(const Record& r) {
    const std::string& keyBytes = r.key.value_or(std::string());
    const std::string& valueBytes = r.value.value_or(std::string());
    int32_t keyLen = r.key.has_value() ? static_cast<int32_t>(keyBytes.size()) : -1;
    int32_t valueLen = r.value.has_value() ? static_cast<int32_t>(valueBytes.size()) : -1;

    size_t bodyAfterCrcSize = kBodyAfterCrcFixedSize + keyBytes.size() + valueBytes.size();

    std::vector<uint8_t> bodyAfterCrc(bodyAfterCrcSize);
    uint8_t* p = bodyAfterCrc.data();
    putI64BE(p, r.offset);
    p += 8;
    putI64BE(p, r.timestampMs);
    p += 8;
    putI32BE(p, keyLen);
    p += 4;
    if (!keyBytes.empty()) {
        std::memcpy(p, keyBytes.data(), keyBytes.size());
        p += keyBytes.size();
    }
    putI32BE(p, valueLen);
    p += 4;
    if (!valueBytes.empty()) {
        std::memcpy(p, valueBytes.data(), valueBytes.size());
        p += valueBytes.size();
    }

    uint32_t crc = crc32(bodyAfterCrc.data(), bodyAfterCrc.size());

    uint32_t length = static_cast<uint32_t>(4 + bodyAfterCrc.size());  // crc(4) + bodyAfterCrc
    std::vector<uint8_t> out(4 + length);
    putU32BE(out.data(), length);
    putU32BE(out.data() + 4, crc);
    std::memcpy(out.data() + 8, bodyAfterCrc.data(), bodyAfterCrc.size());

    return out;
}

bool tryDecodeRecord(const uint8_t* buf, size_t available, Record& outRecord,
                      size_t& outTotalBytes, bool& crcOk) {
    if (available < 4) return false;

    uint32_t length = getU32BE(buf);
    size_t totalRecordSize = 4 + static_cast<size_t>(length);
    if (available < totalRecordSize) return false;  // torn: declared length exceeds what we have

    if (length < 4 + kBodyAfterCrcFixedSize) return false;  // corrupt: body too small to be valid

    uint32_t storedCrc = getU32BE(buf + 4);
    const uint8_t* bodyAfterCrc = buf + 8;
    size_t bodyAfterCrcSize = length - 4;

    const uint8_t* p = bodyAfterCrc;
    int64_t offset = getI64BE(p);
    p += 8;
    int64_t timestampMs = getI64BE(p);
    p += 8;
    int32_t keyLen = getI32BE(p);
    p += 4;

    if (keyLen < -1) return false;  // corrupt
    size_t keyBytesLen = (keyLen == -1) ? 0 : static_cast<size_t>(keyLen);

    size_t consumedSoFar = 8 + 8 + 4;
    if (consumedSoFar + keyBytesLen + 4 > bodyAfterCrcSize) return false;  // corrupt: key overruns body

    const uint8_t* keyPtr = p;
    p += keyBytesLen;

    int32_t valueLen = getI32BE(p);
    p += 4;

    if (valueLen < -1) return false;  // corrupt
    size_t valueBytesLen = (valueLen == -1) ? 0 : static_cast<size_t>(valueLen);

    consumedSoFar = 8 + 8 + 4 + keyBytesLen + 4;
    if (consumedSoFar + valueBytesLen != bodyAfterCrcSize) return false;  // corrupt: size mismatch

    const uint8_t* valuePtr = p;

    uint32_t actualCrc = crc32(bodyAfterCrc, bodyAfterCrcSize);
    crcOk = (actualCrc == storedCrc);

    outRecord.offset = offset;
    outRecord.timestampMs = timestampMs;
    outRecord.key = (keyLen == -1) ? std::nullopt
                                    : std::make_optional(std::string(
                                          reinterpret_cast<const char*>(keyPtr), keyBytesLen));
    outRecord.value = (valueLen == -1) ? std::nullopt
                                        : std::make_optional(std::string(
                                              reinterpret_cast<const char*>(valuePtr), valueBytesLen));
    outTotalBytes = totalRecordSize;
    return true;
}

}  // namespace minikafka
