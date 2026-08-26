#include "common/util/byte_buffer.h"

#include <cstring>

#include "common/net/byteorder.h"

namespace minikafka {

void ByteWriter::writeU8(uint8_t v) { buf_.push_back(v); }

void ByteWriter::writeI32(int32_t v) {
    uint8_t tmp[4];
    putI32BE(tmp, v);
    buf_.insert(buf_.end(), tmp, tmp + 4);
}

void ByteWriter::writeI64(int64_t v) {
    uint8_t tmp[8];
    putI64BE(tmp, v);
    buf_.insert(buf_.end(), tmp, tmp + 8);
}

void ByteWriter::writeString(const std::string& s) {
    writeI32(static_cast<int32_t>(s.size()));
    writeBytes(reinterpret_cast<const uint8_t*>(s.data()), s.size());
}

void ByteWriter::writeOptionalString(const std::optional<std::string>& s) {
    if (!s.has_value()) {
        writeI32(-1);
        return;
    }
    writeString(*s);
}

void ByteWriter::writeBytes(const uint8_t* data, size_t len) {
    if (len == 0) return;
    buf_.insert(buf_.end(), data, data + len);
}

ByteReader::ByteReader(const uint8_t* data, size_t len) : data_(data), len_(len) {}

bool ByteReader::readU8(uint8_t& out) {
    if (remaining() < 1) return false;
    out = data_[pos_];
    pos_ += 1;
    return true;
}

bool ByteReader::readI32(int32_t& out) {
    if (remaining() < 4) return false;
    out = getI32BE(data_ + pos_);
    pos_ += 4;
    return true;
}

bool ByteReader::readI64(int64_t& out) {
    if (remaining() < 8) return false;
    out = getI64BE(data_ + pos_);
    pos_ += 8;
    return true;
}

bool ByteReader::readString(std::string& out) {
    int32_t length;
    if (!readI32(length)) return false;
    if (length < 0) return false;
    if (remaining() < static_cast<size_t>(length)) return false;
    out.assign(reinterpret_cast<const char*>(data_ + pos_), static_cast<size_t>(length));
    pos_ += static_cast<size_t>(length);
    return true;
}

bool ByteReader::readOptionalString(std::optional<std::string>& out) {
    if (remaining() < 4) return false;
    int32_t length = getI32BE(data_ + pos_);
    if (length == -1) {
        pos_ += 4;
        out = std::nullopt;
        return true;
    }
    std::string s;
    if (!readString(s)) return false;
    out = std::move(s);
    return true;
}

}  // namespace minikafka
