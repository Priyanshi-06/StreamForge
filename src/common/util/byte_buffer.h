#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace minikafka {

// Sequential big-endian writer used to build protocol message payloads.
class ByteWriter {
public:
    void writeU8(uint8_t v);
    void writeI32(int32_t v);
    void writeI64(int64_t v);
    void writeString(const std::string& s);                        // I32 length + bytes
    void writeOptionalString(const std::optional<std::string>& s);  // -1 length = nullopt
    void writeBytes(const uint8_t* data, size_t len);

    const std::vector<uint8_t>& data() const { return buf_; }

private:
    std::vector<uint8_t> buf_;
};

// Sequential big-endian reader with bounds checking - every read returns
// false (without partially advancing in a way the caller should trust) if
// there isn't enough data left, so callers can bail out cleanly on a
// malformed/truncated payload instead of reading out of bounds.
class ByteReader {
public:
    ByteReader(const uint8_t* data, size_t len);

    bool readU8(uint8_t& out);
    bool readI32(int32_t& out);
    bool readI64(int64_t& out);
    bool readString(std::string& out);
    bool readOptionalString(std::optional<std::string>& out);

    size_t remaining() const { return len_ - pos_; }
    const uint8_t* cursor() const { return data_ + pos_; }
    void advance(size_t n) { pos_ += n; }

private:
    const uint8_t* data_;
    size_t len_;
    size_t pos_ = 0;
};

}  // namespace minikafka
