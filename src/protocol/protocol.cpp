#include "protocol/protocol.h"

#include "common/net/byteorder.h"

namespace minikafka {

bool writeFrame(Socket& socket, uint8_t typeOrStatus, const std::vector<uint8_t>& payload) {
    uint32_t length = static_cast<uint32_t>(1 + payload.size());
    uint8_t header[5];
    putU32BE(header, length);
    header[4] = typeOrStatus;

    if (!socket.sendAll(header, sizeof(header))) return false;
    if (!payload.empty() && !socket.sendAll(payload.data(), payload.size())) return false;
    return true;
}

bool readFrame(Socket& socket, uint8_t& typeOrStatus, std::vector<uint8_t>& payload) {
    uint8_t lenBuf[4];
    if (!socket.recvAll(lenBuf, 4)) return false;
    uint32_t length = getU32BE(lenBuf);
    if (length < 1) return false;  // must at least contain the type/status byte

    if (!socket.recvAll(&typeOrStatus, 1)) return false;

    size_t payloadLen = length - 1;
    payload.resize(payloadLen);
    if (payloadLen > 0 && !socket.recvAll(payload.data(), payloadLen)) return false;
    return true;
}

std::vector<uint8_t> wrapForwarded(RequestType innerType, const std::vector<uint8_t>& innerPayload) {
    std::vector<uint8_t> out;
    out.reserve(1 + innerPayload.size());
    out.push_back(static_cast<uint8_t>(innerType));
    out.insert(out.end(), innerPayload.begin(), innerPayload.end());
    return out;
}

bool unwrapForwarded(const std::vector<uint8_t>& payload, RequestType& outInnerType,
                      std::vector<uint8_t>& outInnerPayload) {
    if (payload.empty()) return false;
    outInnerType = static_cast<RequestType>(payload[0]);
    outInnerPayload.assign(payload.begin() + 1, payload.end());
    return true;
}

}  // namespace minikafka
