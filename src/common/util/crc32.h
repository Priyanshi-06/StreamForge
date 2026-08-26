#pragma once

#include <cstddef>
#include <cstdint>

namespace minikafka {

// Standard CRC-32 (IEEE 802.3 polynomial, same table as zlib's crc32).
uint32_t crc32(const uint8_t* data, size_t len);

}  // namespace minikafka
