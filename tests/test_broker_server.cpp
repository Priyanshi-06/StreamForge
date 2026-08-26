// Milestone 5 test: starts a real BrokerServer on a background thread and
// drives it over actual loopback TCP connections, exercising the full
// dispatch path (CreateTopic, Metadata, Produce with hash/round-robin
// partitioning, Fetch, and error status codes).

#include <cassert>
#include <chrono>
#include <filesystem>
#include <iostream>
#include <set>
#include <thread>

#include "broker/broker_server.h"
#include "protocol/messages.h"
#include "protocol/protocol.h"

using namespace minikafka;
namespace fs = std::filesystem;

namespace {

constexpr uint16_t kTestPort = 19092;

Socket connectWithRetry() {
    for (int attempt = 0; attempt < 50; ++attempt) {
        Socket s;
        if (s.connectTo("127.0.0.1", kTestPort)) return s;
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    return Socket();
}

template <typename Req>
StatusCode sendRequest(RequestType type, const Req& req,
                        std::vector<uint8_t> (*encodeFn)(const Req&), std::vector<uint8_t>& outPayload) {
    Socket s = connectWithRetry();
    assert(s.valid() && "failed to connect to test broker");

    std::vector<uint8_t> payload = encodeFn(req);
    bool sent = writeFrame(s, static_cast<uint8_t>(type), payload);
    assert(sent);

    uint8_t status;
    bool received = readFrame(s, status, outPayload);
    assert(received);
    return static_cast<StatusCode>(status);
}

}  // namespace

int main() {
    // Initialize Winsock in the main thread before the server thread and
    // this thread's client connections race to use sockets concurrently.
    Socket::globalInit();

    fs::path dataDir = fs::temp_directory_path() / "minikafka_test_broker_server";
    std::error_code ec;
    fs::remove_all(dataDir, ec);

    BrokerServer server(dataDir, kTestPort);
    std::thread serverThread([&server]() { server.run(); });

    // --- Metadata: "orders" (3 partitions) should already exist on startup ---
    {
        std::vector<uint8_t> respPayload;
        StatusCode status =
            sendRequest(RequestType::Metadata, MetadataRequest{""}, &encodeMetadataRequest, respPayload);
        assert(status == StatusCode::Ok);
        MetadataResponse resp;
        assert(decodeMetadataResponse(respPayload.data(), respPayload.size(), resp));
        bool foundOrders = false;
        for (const auto& t : resp.topics) {
            if (t.name == "orders") {
                foundOrders = true;
                assert(t.numPartitions == 3);
            }
        }
        assert(foundOrders && "broker should auto-create 'orders' topic with 3 partitions on startup");
        std::cout << "[PASS] Metadata lists auto-created 'orders' topic\n";
    }

    // --- CreateTopic: success then TopicExists on retry ---
    {
        std::vector<uint8_t> respPayload;
        CreateTopicRequest req{"test-topic", 4, 1};
        StatusCode status =
            sendRequest(RequestType::CreateTopic, req, &encodeCreateTopicRequest, respPayload);
        assert(status == StatusCode::Ok);

        status = sendRequest(RequestType::CreateTopic, req, &encodeCreateTopicRequest, respPayload);
        assert(status == StatusCode::TopicExists);
        std::cout << "[PASS] CreateTopic succeeds once, then reports TopicExists\n";
    }

    // --- Produce with a key: hash partitioning must be consistent for the same key ---
    {
        ProduceRequest req;
        req.topic = "test-topic";
        req.partition = -1;
        req.key = std::string("consistent-key");
        req.value = "first";

        std::vector<uint8_t> respPayload;
        StatusCode status = sendRequest(RequestType::Produce, req, &encodeProduceRequest, respPayload);
        assert(status == StatusCode::Ok);
        ProduceResponse resp1;
        assert(decodeProduceResponse(respPayload.data(), respPayload.size(), resp1));
        assert(resp1.offset == 0);

        req.value = "second";
        status = sendRequest(RequestType::Produce, req, &encodeProduceRequest, respPayload);
        assert(status == StatusCode::Ok);
        ProduceResponse resp2;
        assert(decodeProduceResponse(respPayload.data(), respPayload.size(), resp2));
        assert(resp2.offset == 1);
        assert(resp2.partition == resp1.partition && "same key must hash to the same partition");

        std::cout << "[PASS] Produce with key routes consistently by hash\n";
    }

    // --- Produce without a key: broker-side round robin should spread across partitions ---
    {
        std::set<int32_t> seenPartitions;
        for (int i = 0; i < 8; ++i) {
            ProduceRequest req;
            req.topic = "test-topic";
            req.partition = -1;
            req.key = std::nullopt;
            req.value = "rr-" + std::to_string(i);

            std::vector<uint8_t> respPayload;
            StatusCode status = sendRequest(RequestType::Produce, req, &encodeProduceRequest, respPayload);
            assert(status == StatusCode::Ok);
            ProduceResponse resp;
            assert(decodeProduceResponse(respPayload.data(), respPayload.size(), resp));
            seenPartitions.insert(resp.partition);
        }
        assert(seenPartitions.size() > 1 && "round robin should spread across more than one partition");
        std::cout << "[PASS] Produce without key round-robins across partitions\n";
    }

    // --- Fetch back what was produced to a specific partition ---
    {
        ProduceRequest produceReq;
        produceReq.topic = "test-topic";
        produceReq.partition = 2;
        produceReq.key = std::string("fetch-me");
        produceReq.value = "fetch-me-value";
        std::vector<uint8_t> producePayload;
        StatusCode pStatus =
            sendRequest(RequestType::Produce, produceReq, &encodeProduceRequest, producePayload);
        assert(pStatus == StatusCode::Ok);
        ProduceResponse produceResp;
        assert(decodeProduceResponse(producePayload.data(), producePayload.size(), produceResp));

        FetchRequest fetchReq{"test-topic", 2, produceResp.offset, 10};
        std::vector<uint8_t> fetchPayload;
        StatusCode fStatus = sendRequest(RequestType::Fetch, fetchReq, &encodeFetchRequest, fetchPayload);
        assert(fStatus == StatusCode::Ok);
        FetchResponse fetchResp;
        assert(decodeFetchResponse(fetchPayload.data(), fetchPayload.size(), fetchResp));
        assert(!fetchResp.records.empty());
        assert(fetchResp.records[0].key == "fetch-me");
        assert(fetchResp.records[0].value == "fetch-me-value");
        std::cout << "[PASS] Fetch returns the record just produced\n";
    }

    // --- Error paths: unknown topic / unknown partition ---
    {
        std::vector<uint8_t> respPayload;
        FetchRequest req{"does-not-exist", 0, 0, 10};
        StatusCode status = sendRequest(RequestType::Fetch, req, &encodeFetchRequest, respPayload);
        assert(status == StatusCode::UnknownTopic);

        FetchRequest req2{"test-topic", 99, 0, 10};
        status = sendRequest(RequestType::Fetch, req2, &encodeFetchRequest, respPayload);
        assert(status == StatusCode::UnknownPartition);

        std::cout << "[PASS] Unknown topic/partition report correct error status codes\n";
    }

    // --- CommitOffset / FetchOffset (now require an active JoinGroup lease) ---
    {
        std::vector<uint8_t> respPayload;

        GroupMembershipRequest joinReq{"group-a", "test-topic", 0, "consumer-a"};
        StatusCode status =
            sendRequest(RequestType::JoinGroup, joinReq, &encodeGroupMembershipRequest, respPayload);
        assert(status == StatusCode::Ok);

        FetchOffsetRequest fetchReq{"group-a", "test-topic", 0, "consumer-a"};
        status =
            sendRequest(RequestType::FetchOffset, fetchReq, &encodeFetchOffsetRequest, respPayload);
        assert(status == StatusCode::Ok);
        FetchOffsetResponse resp;
        assert(decodeFetchOffsetResponse(respPayload.data(), respPayload.size(), resp));
        assert(resp.offset == -1 && "no commit yet should report -1");

        CommitOffsetRequest commitReq{"group-a", "test-topic", 0, 5, "consumer-a"};
        status = sendRequest(RequestType::CommitOffset, commitReq, &encodeCommitOffsetRequest,
                              respPayload);
        assert(status == StatusCode::Ok);

        status =
            sendRequest(RequestType::FetchOffset, fetchReq, &encodeFetchOffsetRequest, respPayload);
        assert(status == StatusCode::Ok);
        assert(decodeFetchOffsetResponse(respPayload.data(), respPayload.size(), resp));
        assert(resp.offset == 5 && "should reflect the just-committed offset");

        // Committing against an unknown topic/partition should be rejected.
        CommitOffsetRequest badReq{"group-a", "does-not-exist", 0, 1, "consumer-a"};
        status = sendRequest(RequestType::CommitOffset, badReq, &encodeCommitOffsetRequest,
                              respPayload);
        assert(status == StatusCode::UnknownTopic);

        // A different consumerId (no lease) must be rejected.
        CommitOffsetRequest impostorReq{"group-a", "test-topic", 0, 6, "consumer-impostor"};
        status = sendRequest(RequestType::CommitOffset, impostorReq, &encodeCommitOffsetRequest,
                              respPayload);
        assert(status == StatusCode::PartitionOwnedByAnother);

        sendRequest(RequestType::LeaveGroup, joinReq, &encodeGroupMembershipRequest, respPayload);

        std::cout << "[PASS] CommitOffset/FetchOffset round trip through the broker (with lease)\n";
    }

    // --- Consumer-group partition ownership: the exact race this feature exists to prevent ---
    {
        std::vector<uint8_t> respPayload;

        GroupMembershipRequest consumerA{"shared-group", "test-topic", 1, "consumer-a"};
        GroupMembershipRequest consumerB{"shared-group", "test-topic", 1, "consumer-b"};

        // First consumer claims the partition.
        StatusCode status = sendRequest(RequestType::JoinGroup, consumerA,
                                         &encodeGroupMembershipRequest, respPayload);
        assert(status == StatusCode::Ok);

        // A second consumer in the SAME group trying the SAME partition must be rejected -
        // this is the exact double-processing race the feature closes.
        status = sendRequest(RequestType::JoinGroup, consumerB, &encodeGroupMembershipRequest,
                              respPayload);
        assert(status == StatusCode::PartitionOwnedByAnother);

        // The second consumer also can't commit or fetch-offset without a lease.
        CommitOffsetRequest bCommit{"shared-group", "test-topic", 1, 3, "consumer-b"};
        status = sendRequest(RequestType::CommitOffset, bCommit, &encodeCommitOffsetRequest,
                              respPayload);
        assert(status == StatusCode::PartitionOwnedByAnother);

        // Re-joining as the SAME consumer (heartbeat-equivalent refresh) still succeeds.
        status = sendRequest(RequestType::JoinGroup, consumerA, &encodeGroupMembershipRequest,
                              respPayload);
        assert(status == StatusCode::Ok);

        // Heartbeat also refreshes the same consumer's lease.
        status = sendRequest(RequestType::Heartbeat, consumerA, &encodeGroupMembershipRequest,
                              respPayload);
        assert(status == StatusCode::Ok);

        // Once the first consumer leaves, the second can immediately claim it.
        status = sendRequest(RequestType::LeaveGroup, consumerA, &encodeGroupMembershipRequest,
                              respPayload);
        assert(status == StatusCode::Ok);

        status = sendRequest(RequestType::JoinGroup, consumerB, &encodeGroupMembershipRequest,
                              respPayload);
        assert(status == StatusCode::Ok);

        sendRequest(RequestType::LeaveGroup, consumerB, &encodeGroupMembershipRequest, respPayload);

        std::cout << "[PASS] Consumer-group partition ownership prevents concurrent double-consumption\n";
    }

    server.stop();
    serverThread.join();

    std::cout << "All Milestone 5 tests passed.\n";
    return 0;
}
