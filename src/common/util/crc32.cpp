#include "common/util/crc32.h"

#include <array>

namespace minikafka {

namespace {

std::array<uint32_t, 256> buildTable() {
    std::array<uint32_t, 256> table{};
    for (uint32_t i = 0; i < 256; ++i) {
        uint32_t c = i;
        for (int k = 0; k < 8; ++k) {
            c = (c & 1) ? (0xEDB88320u ^ (c >> 1)) : (c >> 1);
        }
        table[i] = c;
    }
    return table;
}

const std::array<uint32_t, 256>& crcTable() {
    static const std::array<uint32_t, 256> table = buildTable();
    return table;
}

}  // namespace

uint32_t crc32(const uint8_t* data, size_t len) {
    const auto& table = crcTable();
    uint32_t c = 0xFFFFFFFFu;
    for (size_t i = 0; i < len; ++i) {
        c = table[(c ^ data[i]) & 0xFF] ^ (c >> 8);
    }
    return c ^ 0xFFFFFFFFu;
}

}  // namespace minikafka
