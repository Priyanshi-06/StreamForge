#pragma once

#include <cstdint>
#include <cstring>

namespace minikafka {

inline void putU32BE(uint8_t* out, uint32_t v) {
    out[0] = static_cast<uint8_t>(v >> 24);
    out[1] = static_cast<uint8_t>(v >> 16);
    out[2] = static_cast<uint8_t>(v >> 8);
    out[3] = static_cast<uint8_t>(v);
}

inline uint32_t getU32BE(const uint8_t* in) {
    return (static_cast<uint32_t>(in[0]) << 24) |
           (static_cast<uint32_t>(in[1]) << 16) |
           (static_cast<uint32_t>(in[2]) << 8) |
           static_cast<uint32_t>(in[3]);
}

inline void putU64BE(uint8_t* out, uint64_t v) {
    for (int i = 0; i < 8; ++i) {
        out[i] = static_cast<uint8_t>(v >> (56 - 8 * i));
    }
}

inline uint64_t getU64BE(const uint8_t* in) {
    uint64_t v = 0;
    for (int i = 0; i < 8; ++i) {
        v = (v << 8) | in[i];
    }
    return v;
}

inline void putI32BE(uint8_t* out, int32_t v) {
    putU32BE(out, static_cast<uint32_t>(v));
}

inline int32_t getI32BE(const uint8_t* in) {
    return static_cast<int32_t>(getU32BE(in));
}

inline void putI64BE(uint8_t* out, int64_t v) {
    putU64BE(out, static_cast<uint64_t>(v));
}

inline int64_t getI64BE(const uint8_t* in) {
    return static_cast<int64_t>(getU64BE(in));
}

}  // namespace minikafka
