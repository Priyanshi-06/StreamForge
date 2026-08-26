#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "storage/record.h"

namespace minikafka {

// --- CreateTopic ---

struct CreateTopicRequest {
    std::string topicName;
    int32_t numPartitions = 1;
    int32_t replicationFactor = 1;  // recorded only, unused in this build
};
std::vector<uint8_t> encodeCreateTopicRequest(const CreateTopicRequest& r);
bool decodeCreateTopicRequest(const uint8_t* data, size_t len, CreateTopicRequest& out);
// Response is status-only (see StatusCode) - no payload.

// --- Metadata ---

struct MetadataRequest {
    std::string topicName;  // empty = all topics
};
std::vector<uint8_t> encodeMetadataRequest(const MetadataRequest& r);
bool decodeMetadataRequest(const uint8_t* data, size_t len, MetadataRequest& out);

struct MetadataPartitionInfo {
    int32_t index = 0;
    std::vector<int32_t> replicaBrokerIds;  // index 0 = leader; empty if unknown/single-broker
};
struct MetadataTopicInfo {
    std::string name;
    int32_t numPartitions = 0;
    int32_t replicationFactor = 1;
    std::vector<MetadataPartitionInfo> partitions;
};
struct MetadataResponse {
    std::vector<MetadataTopicInfo> topics;
};
std::vector<uint8_t> encodeMetadataResponse(const MetadataResponse& r);
bool decodeMetadataResponse(const uint8_t* data, size_t len, MetadataResponse& out);

// --- Produce ---

struct ProduceRequest {
    std::string topic;
    int32_t partition = -1;  // -1 = broker chooses (hash(key) or round-robin)
    std::optional<std::string> key;
    std::string value;
};
std::vector<uint8_t> encodeProduceRequest(const ProduceRequest& r);
bool decodeProduceRequest(const uint8_t* data, size_t len, ProduceRequest& out);

struct ProduceResponse {
    int32_t partition = 0;
    int64_t offset = 0;
};
std::vector<uint8_t> encodeProduceResponse(const ProduceResponse& r);
bool decodeProduceResponse(const uint8_t* data, size_t len, ProduceResponse& out);

// --- Fetch ---

struct FetchRequest {
    std::string topic;
    int32_t partition = 0;
    int64_t startOffset = 0;
    int32_t maxRecords = 100;
};
std::vector<uint8_t> encodeFetchRequest(const FetchRequest& r);
bool decodeFetchRequest(const uint8_t* data, size_t len, FetchRequest& out);

struct FetchResponse {
    int64_t highWatermark = 0;  // next offset that will be assigned (one past the last record)
    std::vector<Record> records;
};
// Records are serialized back-to-back using the same self-framed encoding
// as on-disk storage (storage/record.h) - a fetch response can stream
// segment bytes almost directly rather than needing a separate wire format.
std::vector<uint8_t> encodeFetchResponse(const FetchResponse& r);
bool decodeFetchResponse(const uint8_t* data, size_t len, FetchResponse& out);

// --- CommitOffset ---
// Broker-side consumer group offset tracking (flat-file store, see
// broker/consumer_offsets.h) - lets a consumer resume after disconnect
// without re-reading records it already processed.

struct CommitOffsetRequest {
    std::string group;
    std::string topic;
    int32_t partition = 0;
    int64_t offset = 0;
    // Must match the consumerId currently holding the lease on this
    // (group, topic, partition) - see broker/group_coordinator.h. Rejected
    // with StatusCode::PartitionOwnedByAnother otherwise.
    std::string consumerId;
};
std::vector<uint8_t> encodeCommitOffsetRequest(const CommitOffsetRequest& r);
bool decodeCommitOffsetRequest(const uint8_t* data, size_t len, CommitOffsetRequest& out);
// Response is status-only - no payload.

// --- FetchOffset ---

struct FetchOffsetRequest {
    std::string group;
    std::string topic;
    int32_t partition = 0;
    std::string consumerId;  // same ownership requirement as CommitOffsetRequest
};
std::vector<uint8_t> encodeFetchOffsetRequest(const FetchOffsetRequest& r);
bool decodeFetchOffsetRequest(const uint8_t* data, size_t len, FetchOffsetRequest& out);

struct FetchOffsetResponse {
    int64_t offset = -1;  // -1 = no committed offset yet; consumer should start at 0
};
std::vector<uint8_t> encodeFetchOffsetResponse(const FetchOffsetResponse& r);
bool decodeFetchOffsetResponse(const uint8_t* data, size_t len, FetchOffsetResponse& out);

// --- JoinGroup / Heartbeat / LeaveGroup ---
// All three share the same payload shape - the RequestType byte on the
// frame (not the payload) distinguishes which operation it is. See
// broker/group_coordinator.h for the ownership/lease semantics.

struct GroupMembershipRequest {
    std::string group;
    std::string topic;
    int32_t partition = 0;
    std::string consumerId;
};
std::vector<uint8_t> encodeGroupMembershipRequest(const GroupMembershipRequest& r);
bool decodeGroupMembershipRequest(const uint8_t* data, size_t len, GroupMembershipRequest& out);
// Response is status-only (Ok, or PartitionOwnedByAnother for Join/Heartbeat) - no payload.

// --- AnnounceTopic ---
// Broker-to-broker only (RequestType::AnnounceTopic): broadcast by
// whichever broker handled a client's CreateTopic, to every broker in
// the cluster (including itself), so each one's TopicRegistry learns
// about the topic and opens local storage for whatever partitions it's
// assigned. Idempotent - a peer that already knows this topic just no-ops.

struct AnnounceTopicRequest {
    std::string name;
    int32_t numPartitions = 1;
    int32_t replicationFactor = 1;
};
std::vector<uint8_t> encodeAnnounceTopicRequest(const AnnounceTopicRequest& r);
bool decodeAnnounceTopicRequest(const uint8_t* data, size_t len, AnnounceTopicRequest& out);
// Response is status-only - no payload.

}  // namespace minikafka
