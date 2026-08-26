#pragma once

#include <cstdint>
#include <vector>

#include "common/net/socket.h"

namespace minikafka {

enum class RequestType : uint8_t {
    CreateTopic = 1,
    Metadata = 2,
    Produce = 3,
    Fetch = 4,
    CommitOffset = 5,
    FetchOffset = 6,
    JoinGroup = 7,
    Heartbeat = 8,
    LeaveGroup = 9,
    // Broker-to-broker only, never sent by a client CLI:
    AnnounceTopic = 10,  // peer notification that a topic was created cluster-wide
    Forwarded = 11,      // wraps another request for a broker that isn't the leader to relay to one that is
};

enum class StatusCode : uint8_t {
    Ok = 0,
    UnknownTopic = 1,
    UnknownPartition = 2,
    TopicExists = 3,
    OffsetOutOfRange = 4,
    // Another consumer currently holds a valid lease on this
    // (group, topic, partition); the caller must wait for it to be
    // released or expire before it can join/commit/fetch-offset here.
    PartitionOwnedByAnother = 5,
    // This partition's leader broker (per the static cluster assignment)
    // could not be reached to forward the request to. Distinct from
    // InternalError: this means "the cluster currently has no reachable
    // leader for this partition," an expected condition in this phase
    // since there's no automatic failover yet - not "our own logic broke."
    LeaderUnavailable = 6,
    InternalError = 99,
};

// Frame: [4-byte BE length][1-byte type-or-status][payload]
// `length` covers everything after the length field itself, i.e.
// 1 + payload.size(). Connections are strictly synchronous
// request/response (blocking, one in flight at a time per connection), so
// no correlation ID is needed to match responses to requests.
bool writeFrame(Socket& socket, uint8_t typeOrStatus, const std::vector<uint8_t>& payload);
bool readFrame(Socket& socket, uint8_t& typeOrStatus, std::vector<uint8_t>& payload);

// RequestType::Forwarded payload = [1-byte inner RequestType][inner payload].
// A broker that isn't the leader for a partition wraps the client's exact
// original request this way and sends it to the leader, which unwraps and
// executes it as if received directly - but NEVER re-forwards a request
// that arrived already-wrapped, which is what makes forwarding
// structurally single-hop (no possible infinite loop, even under a
// misconfigured/inconsistent cluster view - a mismatch just fails the
// sanity check on the receiving end instead of looping).
std::vector<uint8_t> wrapForwarded(RequestType innerType, const std::vector<uint8_t>& innerPayload);
bool unwrapForwarded(const std::vector<uint8_t>& payload, RequestType& outInnerType,
                      std::vector<uint8_t>& outInnerPayload);

}  // namespace minikafka
