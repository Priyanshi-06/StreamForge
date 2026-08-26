// Milestone 3 tests: frame read/write over a real loopback socket, and
// encode/decode round trips for every request/response message type.

#include <cassert>
#include <iostream>
#include <thread>

#include "common/net/socket.h"
#include "protocol/messages.h"
#include "protocol/protocol.h"

using namespace minikafka;

static void testFrameLoopback() {
    Socket::globalInit();

    Socket server;
    bool ok = server.listenOn("127.0.0.1", 18375, 4);
    assert(ok);

    std::thread serverThread([&server]() {
        Socket conn = server.accept();
        assert(conn.valid());

        uint8_t type;
        std::vector<uint8_t> payload;
        bool received = readFrame(conn, type, payload);
        assert(received);
        assert(type == static_cast<uint8_t>(RequestType::Produce));
        assert(payload.size() == 3);
        assert(payload[0] == 'a' && payload[1] == 'b' && payload[2] == 'c');

        std::vector<uint8_t> replyPayload = {'o', 'k'};
        bool sent = writeFrame(conn, static_cast<uint8_t>(StatusCode::Ok), replyPayload);
        assert(sent);
    });

    Socket client;
    ok = client.connectTo("127.0.0.1", 18375);
    assert(ok);

    std::vector<uint8_t> payload = {'a', 'b', 'c'};
    bool sent = writeFrame(client, static_cast<uint8_t>(RequestType::Produce), payload);
    assert(sent);

    uint8_t status;
    std::vector<uint8_t> reply;
    bool received = readFrame(client, status, reply);
    assert(received);
    assert(status == static_cast<uint8_t>(StatusCode::Ok));
    assert(reply.size() == 2 && reply[0] == 'o' && reply[1] == 'k');

    serverThread.join();
    Socket::globalCleanup();

    std::cout << "[PASS] frame read/write over loopback socket\n";
}

static void testCreateTopicRoundTrip() {
    CreateTopicRequest req;
    req.topicName = "orders";
    req.numPartitions = 3;
    req.replicationFactor = 2;

    auto encoded = encodeCreateTopicRequest(req);
    CreateTopicRequest decoded;
    bool ok = decodeCreateTopicRequest(encoded.data(), encoded.size(), decoded);
    assert(ok);
    assert(decoded.topicName == "orders");
    assert(decoded.numPartitions == 3);
    assert(decoded.replicationFactor == 2);

    std::cout << "[PASS] CreateTopicRequest round trip\n";
}

static void testMetadataRoundTrip() {
    MetadataRequest req{"orders"};
    auto encodedReq = encodeMetadataRequest(req);
    MetadataRequest decodedReq;
    assert(decodeMetadataRequest(encodedReq.data(), encodedReq.size(), decodedReq));
    assert(decodedReq.topicName == "orders");

    MetadataResponse resp;
    MetadataTopicInfo orders;
    orders.name = "orders";
    orders.numPartitions = 2;
    orders.partitions.push_back(MetadataPartitionInfo{0, {1, 2}});  // broker 1 leads, broker 2 follows
    orders.partitions.push_back(MetadataPartitionInfo{1, {2, 1}});
    resp.topics.push_back(orders);
    resp.topics.push_back({"payments", 1, {}});
    auto encodedResp = encodeMetadataResponse(resp);
    MetadataResponse decodedResp;
    assert(decodeMetadataResponse(encodedResp.data(), encodedResp.size(), decodedResp));
    assert(decodedResp.topics.size() == 2);
    assert(decodedResp.topics[0].name == "orders");
    assert(decodedResp.topics[0].numPartitions == 2);
    assert(decodedResp.topics[0].partitions.size() == 2);
    assert(decodedResp.topics[0].partitions[0].index == 0);
    assert((decodedResp.topics[0].partitions[0].replicaBrokerIds == std::vector<int32_t>{1, 2}));
    assert((decodedResp.topics[0].partitions[1].replicaBrokerIds == std::vector<int32_t>{2, 1}));
    assert(decodedResp.topics[1].partitions.empty());
    assert(decodedResp.topics[1].name == "payments");
    assert(decodedResp.topics[1].numPartitions == 1);

    std::cout << "[PASS] Metadata request/response round trip\n";
}

static void testProduceRoundTrip() {
    ProduceRequest req;
    req.topic = "orders";
    req.partition = -1;
    req.key = std::string("order-42");
    req.value = "some payload";

    auto encoded = encodeProduceRequest(req);
    ProduceRequest decoded;
    assert(decodeProduceRequest(encoded.data(), encoded.size(), decoded));
    assert(decoded.topic == "orders");
    assert(decoded.partition == -1);
    assert(decoded.key == "order-42");
    assert(decoded.value == "some payload");

    // No key (broker should round-robin).
    ProduceRequest req2;
    req2.topic = "orders";
    req2.partition = -1;
    req2.key = std::nullopt;
    req2.value = "keyless payload";
    auto encoded2 = encodeProduceRequest(req2);
    ProduceRequest decoded2;
    assert(decodeProduceRequest(encoded2.data(), encoded2.size(), decoded2));
    assert(!decoded2.key.has_value());

    ProduceResponse resp{2, 17};
    auto encodedResp = encodeProduceResponse(resp);
    ProduceResponse decodedResp;
    assert(decodeProduceResponse(encodedResp.data(), encodedResp.size(), decodedResp));
    assert(decodedResp.partition == 2);
    assert(decodedResp.offset == 17);

    std::cout << "[PASS] Produce request/response round trip\n";
}

static void testFetchRoundTrip() {
    FetchRequest req{"orders", 1, 5, 50};
    auto encodedReq = encodeFetchRequest(req);
    FetchRequest decodedReq;
    assert(decodeFetchRequest(encodedReq.data(), encodedReq.size(), decodedReq));
    assert(decodedReq.topic == "orders");
    assert(decodedReq.partition == 1);
    assert(decodedReq.startOffset == 5);
    assert(decodedReq.maxRecords == 50);

    FetchResponse resp;
    resp.highWatermark = 3;
    Record r0;
    r0.offset = 0;
    r0.timestampMs = 111;
    r0.key = std::string("k0");
    r0.value = std::string("v0");
    Record r1;
    r1.offset = 1;
    r1.timestampMs = 222;
    r1.key = std::nullopt;
    r1.value = std::string("v1");
    Record r2;
    r2.offset = 2;
    r2.timestampMs = 333;
    r2.key = std::string("k2");
    r2.value = std::nullopt;  // tombstone
    resp.records = {r0, r1, r2};

    auto encodedResp = encodeFetchResponse(resp);
    FetchResponse decodedResp;
    assert(decodeFetchResponse(encodedResp.data(), encodedResp.size(), decodedResp));
    assert(decodedResp.highWatermark == 3);
    assert(decodedResp.records.size() == 3);
    assert(decodedResp.records[0].key == "k0" && decodedResp.records[0].value == "v0");
    assert(!decodedResp.records[1].key.has_value() && decodedResp.records[1].value == "v1");
    assert(decodedResp.records[2].key == "k2" && !decodedResp.records[2].value.has_value());

    // Empty records list should round-trip cleanly too.
    FetchResponse emptyResp;
    emptyResp.highWatermark = 0;
    auto encodedEmpty = encodeFetchResponse(emptyResp);
    FetchResponse decodedEmpty;
    assert(decodeFetchResponse(encodedEmpty.data(), encodedEmpty.size(), decodedEmpty));
    assert(decodedEmpty.records.empty());

    std::cout << "[PASS] Fetch request/response round trip\n";
}

static void testOffsetRoundTrip() {
    CommitOffsetRequest commitReq{"group-1", "orders", 2, 17, "consumer-a"};
    auto encodedCommit = encodeCommitOffsetRequest(commitReq);
    CommitOffsetRequest decodedCommit;
    assert(decodeCommitOffsetRequest(encodedCommit.data(), encodedCommit.size(), decodedCommit));
    assert(decodedCommit.group == "group-1");
    assert(decodedCommit.topic == "orders");
    assert(decodedCommit.partition == 2);
    assert(decodedCommit.offset == 17);
    assert(decodedCommit.consumerId == "consumer-a");

    FetchOffsetRequest fetchReq{"group-1", "orders", 2, "consumer-a"};
    auto encodedFetchReq = encodeFetchOffsetRequest(fetchReq);
    FetchOffsetRequest decodedFetchReq;
    assert(decodeFetchOffsetRequest(encodedFetchReq.data(), encodedFetchReq.size(), decodedFetchReq));
    assert(decodedFetchReq.group == "group-1");
    assert(decodedFetchReq.topic == "orders");
    assert(decodedFetchReq.partition == 2);
    assert(decodedFetchReq.consumerId == "consumer-a");

    FetchOffsetResponse resp{-1};
    auto encodedResp = encodeFetchOffsetResponse(resp);
    FetchOffsetResponse decodedResp;
    assert(decodeFetchOffsetResponse(encodedResp.data(), encodedResp.size(), decodedResp));
    assert(decodedResp.offset == -1);

    std::cout << "[PASS] CommitOffset/FetchOffset round trip\n";
}

static void testGroupMembershipRoundTrip() {
    GroupMembershipRequest req{"group-1", "orders", 1, "consumer-a"};
    auto encoded = encodeGroupMembershipRequest(req);
    GroupMembershipRequest decoded;
    assert(decodeGroupMembershipRequest(encoded.data(), encoded.size(), decoded));
    assert(decoded.group == "group-1");
    assert(decoded.topic == "orders");
    assert(decoded.partition == 1);
    assert(decoded.consumerId == "consumer-a");

    std::cout << "[PASS] GroupMembership (Join/Heartbeat/Leave) round trip\n";
}

static void testAnnounceTopicRoundTrip() {
    AnnounceTopicRequest req{"orders", 3, 2};
    auto encoded = encodeAnnounceTopicRequest(req);
    AnnounceTopicRequest decoded;
    assert(decodeAnnounceTopicRequest(encoded.data(), encoded.size(), decoded));
    assert(decoded.name == "orders");
    assert(decoded.numPartitions == 3);
    assert(decoded.replicationFactor == 2);

    std::cout << "[PASS] AnnounceTopic round trip\n";
}

static void testForwardedWrapUnwrap() {
    ProduceRequest inner;
    inner.topic = "orders";
    inner.partition = 1;
    inner.key = std::string("k");
    inner.value = "v";
    auto innerPayload = encodeProduceRequest(inner);

    auto wrapped = wrapForwarded(RequestType::Produce, innerPayload);
    assert(wrapped.size() == 1 + innerPayload.size());

    RequestType outType;
    std::vector<uint8_t> outPayload;
    bool ok = unwrapForwarded(wrapped, outType, outPayload);
    assert(ok);
    assert(outType == RequestType::Produce);
    assert(outPayload == innerPayload);

    ProduceRequest roundTripped;
    assert(decodeProduceRequest(outPayload.data(), outPayload.size(), roundTripped));
    assert(roundTripped.topic == "orders");
    assert(roundTripped.partition == 1);
    assert(roundTripped.key == "k");
    assert(roundTripped.value == "v");

    // Empty payload (malformed) must be rejected, not read out of bounds.
    std::vector<uint8_t> empty;
    assert(!unwrapForwarded(empty, outType, outPayload));

    std::cout << "[PASS] Forwarded wrap/unwrap round trip\n";
}

int main() {
    testFrameLoopback();
    testCreateTopicRoundTrip();
    testMetadataRoundTrip();
    testProduceRoundTrip();
    testFetchRoundTrip();
    testOffsetRoundTrip();
    testGroupMembershipRoundTrip();
    testAnnounceTopicRoundTrip();
    testForwardedWrapUnwrap();
    std::cout << "All Milestone 3 tests passed.\n";
    return 0;
}
