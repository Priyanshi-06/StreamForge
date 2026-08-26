#pragma once

#include <cstdint>
#include <string>

namespace minikafka {

// FNV-1a 32-bit. Hand-rolled (rather than std::hash) so key->partition
// mapping is simple, documented, and stable across compilers/versions.
inline uint32_t fnv1a32(const std::string& s) {
    uint32_t h = 2166136261u;
    for (unsigned char c : s) {
        h ^= c;
        h *= 16777619u;
    }
    return h;
}

}  // namespace minikafka
