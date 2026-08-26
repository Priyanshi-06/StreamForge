#include "protocol/messages.h"

#include "common/util/byte_buffer.h"

namespace minikafka {

// --- CreateTopic ---

std::vector<uint8_t> encodeCreateTopicRequest(const CreateTopicRequest& r) {
    ByteWriter w;
    w.writeString(r.topicName);
    w.writeI32(r.numPartitions);
    w.writeI32(r.replicationFactor);
    return w.data();
}

bool decodeCreateTopicRequest(const uint8_t* data, size_t len, CreateTopicRequest& out) {
    ByteReader r(data, len);
    return r.readString(out.topicName) && r.readI32(out.numPartitions) &&
           r.readI32(out.replicationFactor);
}

// --- Metadata ---

std::vector<uint8_t> encodeMetadataRequest(const MetadataRequest& r) {
    ByteWriter w;
    w.writeString(r.topicName);
    return w.data();
}

bool decodeMetadataRequest(const uint8_t* data, size_t len, MetadataRequest& out) {
    ByteReader r(data, len);
    return r.readString(out.topicName);
}

std::vector<uint8_t> encodeMetadataResponse(const MetadataResponse& r) {
    ByteWriter w;
    w.writeI32(static_cast<int32_t>(r.topics.size()));
    for (const auto& t : r.topics) {
        w.writeString(t.name);
        w.writeI32(t.numPartitions);
        w.writeI32(t.replicationFactor);
        w.writeI32(static_cast<int32_t>(t.partitions.size()));
        for (const auto& p : t.partitions) {
            w.writeI32(p.index);
            w.writeI32(static_cast<int32_t>(p.replicaBrokerIds.size()));
            for (int32_t brokerId : p.replicaBrokerIds) {
                w.writeI32(brokerId);
            }
        }
    }
    return w.data();
}

bool decodeMetadataResponse(const uint8_t* data, size_t len, MetadataResponse& out) {
    ByteReader r(data, len);
    int32_t count;
    if (!r.readI32(count) || count < 0) return false;
    out.topics.clear();
    out.topics.reserve(static_cast<size_t>(count));
    for (int32_t i = 0; i < count; ++i) {
        MetadataTopicInfo t;
        if (!r.readString(t.name)) return false;
        if (!r.readI32(t.numPartitions)) return false;
        if (!r.readI32(t.replicationFactor)) return false;

        int32_t partitionCount;
        if (!r.readI32(partitionCount) || partitionCount < 0) return false;
        t.partitions.reserve(static_cast<size_t>(partitionCount));
        for (int32_t p = 0; p < partitionCount; ++p) {
            MetadataPartitionInfo info;
            if (!r.readI32(info.index)) return false;
            int32_t replicaCount;
            if (!r.readI32(replicaCount) || replicaCount < 0) return false;
            info.replicaBrokerIds.reserve(static_cast<size_t>(replicaCount));
            for (int32_t j = 0; j < replicaCount; ++j) {
                int32_t brokerId;
                if (!r.readI32(brokerId)) return false;
                info.replicaBrokerIds.push_back(brokerId);
            }
            t.partitions.push_back(std::move(info));
        }

        out.topics.push_back(std::move(t));
    }
    return true;
}

// --- Produce ---

std::vector<uint8_t> encodeProduceRequest(const ProduceRequest& r) {
    ByteWriter w;
    w.writeString(r.topic);
    w.writeI32(r.partition);
    w.writeOptionalString(r.key);
    w.writeString(r.value);
    return w.data();
}

bool decodeProduceRequest(const uint8_t* data, size_t len, ProduceRequest& out) {
    ByteReader r(data, len);
    return r.readString(out.topic) && r.readI32(out.partition) &&
           r.readOptionalString(out.key) && r.readString(out.value);
}

std::vector<uint8_t> encodeProduceResponse(const ProduceResponse& r) {
    ByteWriter w;
    w.writeI32(r.partition);
    w.writeI64(r.offset);
    return w.data();
}

bool decodeProduceResponse(const uint8_t* data, size_t len, ProduceResponse& out) {
    ByteReader r(data, len);
    return r.readI32(out.partition) && r.readI64(out.offset);
}

// --- Fetch ---

std::vector<uint8_t> encodeFetchRequest(const FetchRequest& r) {
    ByteWriter w;
    w.writeString(r.topic);
    w.writeI32(r.partition);
    w.writeI64(r.startOffset);
    w.writeI32(r.maxRecords);
    return w.data();
}

bool decodeFetchRequest(const uint8_t* data, size_t len, FetchRequest& out) {
    ByteReader r(data, len);
    return r.readString(out.topic) && r.readI32(out.partition) &&
           r.readI64(out.startOffset) && r.readI32(out.maxRecords);
}

std::vector<uint8_t> encodeFetchResponse(const FetchResponse& r) {
    ByteWriter w;
    w.writeI64(r.highWatermark);
    for (const auto& rec : r.records) {
        std::vector<uint8_t> encoded = encodeRecord(rec);
        w.writeBytes(encoded.data(), encoded.size());
    }
    return w.data();
}

bool decodeFetchResponse(const uint8_t* data, size_t len, FetchResponse& out) {
    ByteReader r(data, len);
    if (!r.readI64(out.highWatermark)) return false;

    out.records.clear();
    while (r.remaining() > 0) {
        Record rec;
        size_t totalBytes;
        bool crcOk;
        if (!tryDecodeRecord(r.cursor(), r.remaining(), rec, totalBytes, crcOk)) return false;
        if (!crcOk) return false;
        out.records.push_back(std::move(rec));
        r.advance(totalBytes);
    }
    return true;
}

// --- CommitOffset ---

std::vector<uint8_t> encodeCommitOffsetRequest(const CommitOffsetRequest& r) {
    ByteWriter w;
    w.writeString(r.group);
    w.writeString(r.topic);
    w.writeI32(r.partition);
    w.writeI64(r.offset);
    w.writeString(r.consumerId);
    return w.data();
}

bool decodeCommitOffsetRequest(const uint8_t* data, size_t len, CommitOffsetRequest& out) {
    ByteReader r(data, len);
    return r.readString(out.group) && r.readString(out.topic) && r.readI32(out.partition) &&
           r.readI64(out.offset) && r.readString(out.consumerId);
}

// --- FetchOffset ---

std::vector<uint8_t> encodeFetchOffsetRequest(const FetchOffsetRequest& r) {
    ByteWriter w;
    w.writeString(r.group);
    w.writeString(r.topic);
    w.writeI32(r.partition);
    w.writeString(r.consumerId);
    return w.data();
}

bool decodeFetchOffsetRequest(const uint8_t* data, size_t len, FetchOffsetRequest& out) {
    ByteReader r(data, len);
    return r.readString(out.group) && r.readString(out.topic) && r.readI32(out.partition) &&
           r.readString(out.consumerId);
}

std::vector<uint8_t> encodeFetchOffsetResponse(const FetchOffsetResponse& r) {
    ByteWriter w;
    w.writeI64(r.offset);
    return w.data();
}

bool decodeFetchOffsetResponse(const uint8_t* data, size_t len, FetchOffsetResponse& out) {
    ByteReader r(data, len);
    return r.readI64(out.offset);
}

// --- JoinGroup / Heartbeat / LeaveGroup ---

std::vector<uint8_t> encodeGroupMembershipRequest(const GroupMembershipRequest& r) {
    ByteWriter w;
    w.writeString(r.group);
    w.writeString(r.topic);
    w.writeI32(r.partition);
    w.writeString(r.consumerId);
    return w.data();
}

bool decodeGroupMembershipRequest(const uint8_t* data, size_t len, GroupMembershipRequest& out) {
    ByteReader r(data, len);
    return r.readString(out.group) && r.readString(out.topic) && r.readI32(out.partition) &&
           r.readString(out.consumerId);
}

// --- AnnounceTopic ---

std::vector<uint8_t> encodeAnnounceTopicRequest(const AnnounceTopicRequest& r) {
    ByteWriter w;
    w.writeString(r.name);
    w.writeI32(r.numPartitions);
    w.writeI32(r.replicationFactor);
    return w.data();
}

bool decodeAnnounceTopicRequest(const uint8_t* data, size_t len, AnnounceTopicRequest& out) {
    ByteReader r(data, len);
    return r.readString(out.name) && r.readI32(out.numPartitions) && r.readI32(out.replicationFactor);
}

}  // namespace minikafka
