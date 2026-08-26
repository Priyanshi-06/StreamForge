// Multi-broker integration tests: spins up 2-3 real BrokerServer instances
// on background threads, driven over real loopback TCP, exercising
// transparent forwarding, cross-broker CreateTopic propagation (both the
// AnnounceTopic broadcast and the periodic Metadata pull-reconciliation
// fallback), follower replication catch-up, leader-down error handling,
// and that consumer-group ownership correctness emerges from routing
// through the leader rather than replicating GroupCoordinator itself.
//
// IMPORTANT: since transparent forwarding routes every client read to a
// partition's leader, "did the follower actually replicate this" can't be
// verified via a normal Fetch (it would just get forwarded back to the
// leader) - tests instead inspect a follower's PartitionLog directly via
// BrokerServer::registry(), exactly like the HTTP dashboard does.

#include <cassert>
#include <chrono>
#include <filesystem>
#include <iostream>
#include <thread>

#include "broker/broker_server.h"
#include "broker/cluster_config.h"
#include "protocol/messages.h"
#include "protocol/protocol.h"

using namespace minikafka;
namespace fs = std::filesystem;

namespace {

constexpr uint16_t kBroker0Port = 19300;
constexpr uint16_t kBroker1Port = 19310;
constexpr uint16_t kBroker2Port = 19320;
const std::string kClusterArg = "0:127.0.0.1:19300,1:127.0.0.1:19310,2:127.0.0.1:19320";

Socket connectWithRetry(uint16_t port, int attempts = 100) {
    for (int i = 0; i < attempts; ++i) {
        Socket s;
        if (s.connectTo("127.0.0.1", port)) return s;
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    return Socket();
}

template <typename Req>
StatusCode sendRequest(uint16_t port, RequestType type, const Req& req,
                        std::vector<uint8_t> (*encodeFn)(const Req&), std::vector<uint8_t>& outPayload) {
    Socket s = connectWithRetry(port);
    assert(s.valid() && "failed to connect to test broker");

    std::vector<uint8_t> payload = encodeFn(req);
    bool sent = writeFrame(s, static_cast<uint8_t>(type), payload);
    assert(sent);

    uint8_t status;
    bool received = readFrame(s, status, outPayload);
    assert(received);
    return static_cast<StatusCode>(status);
}

fs::path scratchDir(const std::string& name) {
    fs::path dir = fs::temp_directory_path() / ("minikafka_test_multi_broker_" + name);
    std::error_code ec;
    fs::remove_all(dir, ec);
    return dir;
}

}  // namespace

int main() {
    Socket::globalInit();

    ClusterConfig cluster0 = ClusterConfig::parse(0, kClusterArg);
    ClusterConfig cluster1 = ClusterConfig::parse(1, kClusterArg);
    ClusterConfig cluster2 = ClusterConfig::parse(2, kClusterArg);

    BrokerServer broker0(scratchDir("b0"), kBroker0Port, cluster0);
    BrokerServer broker1(scratchDir("b1"), kBroker1Port, cluster1);

    std::thread t0([&broker0]() { broker0.run(); });
    std::thread t1([&broker1]() { broker1.run(); });

    // --- CreateTopic on broker0, RF=2 -> partitions get leader+follower across brokers ---
    // (named "cluster-topic", not "orders" - broker0 auto-creates a demo "orders" topic
    // on startup, which would otherwise collide with this test's own CreateTopic call.)
    {
        std::vector<uint8_t> respPayload;
        CreateTopicRequest req{"cluster-topic", 2, 2};
        StatusCode status =
            sendRequest(kBroker0Port, RequestType::CreateTopic, req, &encodeCreateTopicRequest, respPayload);
        assert(status == StatusCode::Ok);
        std::cout << "[PASS] CreateTopic on broker0 succeeds\n";
    }

    // --- AnnounceTopic broadcast: broker1 should learn about "cluster-topic" almost immediately ---
    {
        bool found = false;
        for (int attempt = 0; attempt < 100 && !found; ++attempt) {
            Topic* t = broker1.registry().getTopic("cluster-topic");
            if (t) found = true;
            else std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
        assert(found && "broker1 should learn about the topic via AnnounceTopic broadcast");
        std::cout << "[PASS] CreateTopic propagates to broker1 via AnnounceTopic broadcast\n";
    }

    Topic* topicOnBroker0 = broker0.registry().getTopic("cluster-topic");
    assert(topicOnBroker0 != nullptr);
    int32_t leaderOfPartition0 = topicOnBroker0->leaderBrokerId(0);
    int32_t leaderOfPartition1 = topicOnBroker0->leaderBrokerId(1);
    // Both brokers must agree on leadership (deterministic assignment).
    assert(broker1.registry().getTopic("cluster-topic")->leaderBrokerId(0) == leaderOfPartition0);
    assert(broker1.registry().getTopic("cluster-topic")->leaderBrokerId(1) == leaderOfPartition1);

    uint16_t leaderPort0 = (leaderOfPartition0 == 0) ? kBroker0Port : kBroker1Port;
    uint16_t nonLeaderPort0 = (leaderOfPartition0 == 0) ? kBroker1Port : kBroker0Port;
    BrokerServer& leaderServer0 = (leaderOfPartition0 == 0) ? broker0 : broker1;

    // --- Produce with an EXPLICIT partition, sent to the non-leader broker: must forward ---
    {
        std::vector<uint8_t> respPayload;
        ProduceRequest req;
        req.topic = "cluster-topic";
        req.partition = 0;
        req.key = std::string("explicit-key");
        req.value = "explicit-value";
        StatusCode status =
            sendRequest(nonLeaderPort0, RequestType::Produce, req, &encodeProduceRequest, respPayload);
        assert(status == StatusCode::Ok);
        ProduceResponse resp;
        assert(decodeProduceResponse(respPayload.data(), respPayload.size(), resp));
        assert(resp.partition == 0);
        assert(resp.offset == 0);

        // Verify directly on the LEADER's own storage - not just "a response came back."
        PartitionLog* log = leaderServer0.registry().getTopic("cluster-topic")->partition(0);
        assert(log != nullptr);
        assert(log->nextOffset() == 1);
        auto records = log->fetch(0, 10);
        assert(records.size() == 1);
        assert(records[0].key == "explicit-key");

        std::cout << "[PASS] Produce with explicit partition forwards to the leader correctly\n";
    }

    // --- Produce with auto partition (-1), keyed: broker resolves partition BEFORE forwarding ---
    {
        std::vector<uint8_t> respPayload;
        ProduceRequest req;
        req.topic = "cluster-topic";
        req.partition = -1;
        req.key = std::string("some-consistent-key");
        req.value = "v1";
        StatusCode status =
            sendRequest(nonLeaderPort0, RequestType::Produce, req, &encodeProduceRequest, respPayload);
        assert(status == StatusCode::Ok);
        ProduceResponse resp1;
        assert(decodeProduceResponse(respPayload.data(), respPayload.size(), resp1));

        // Same key, same call again - must land on the same partition.
        status = sendRequest(nonLeaderPort0, RequestType::Produce, req, &encodeProduceRequest, respPayload);
        assert(status == StatusCode::Ok);
        ProduceResponse resp2;
        assert(decodeProduceResponse(respPayload.data(), respPayload.size(), resp2));
        assert(resp2.partition == resp1.partition);
        assert(resp2.offset == resp1.offset + 1);

        std::cout << "[PASS] Produce with auto-selected (keyed) partition routes consistently\n";
    }

    // --- Fetch through the non-leader broker must be forwarded and match the leader's data ---
    {
        std::vector<uint8_t> respPayload;
        FetchRequest req{"cluster-topic", 0, 0, 10};
        StatusCode status = sendRequest(nonLeaderPort0, RequestType::Fetch, req, &encodeFetchRequest, respPayload);
        assert(status == StatusCode::Ok);
        FetchResponse resp;
        assert(decodeFetchResponse(respPayload.data(), respPayload.size(), resp));
        assert(!resp.records.empty());
        assert(resp.records[0].key == "explicit-key");

        std::cout << "[PASS] Fetch through a non-leader broker is forwarded and returns correct data\n";
    }

    // --- Consumer-group ownership: JoinGroup via the non-leader broker still routes to the leader ---
    {
        std::vector<uint8_t> respPayload;
        GroupMembershipRequest joinA{"cluster-group", "cluster-topic", 0, "consumer-a"};
        StatusCode status = sendRequest(nonLeaderPort0, RequestType::JoinGroup, joinA,
                                         &encodeGroupMembershipRequest, respPayload);
        assert(status == StatusCode::Ok);

        // A second consumer for the SAME group/partition, sent to the OTHER broker, must
        // still be rejected - proving ownership correctness comes from routing to the
        // leader, not from replicating GroupCoordinator itself.
        GroupMembershipRequest joinB{"cluster-group", "cluster-topic", 0, "consumer-b"};
        status = sendRequest(leaderPort0, RequestType::JoinGroup, joinB, &encodeGroupMembershipRequest,
                              respPayload);
        assert(status == StatusCode::PartitionOwnedByAnother);

        std::cout << "[PASS] Consumer-group ownership is correct across brokers via leader routing\n";
    }

    // --- Follower replication catch-up: verify partition 0's follower actually has the data ---
    {
        // Whichever broker is NOT the leader for partition 0 should be its follower
        // (RF=2, 2 brokers) - poll its local storage directly (a Fetch would just be
        // forwarded back to the leader, so this must bypass the network layer).
        BrokerServer& followerServer0 = (leaderOfPartition0 == 0) ? broker1 : broker0;
        Topic* followerTopic = followerServer0.registry().getTopic("cluster-topic");
        assert(followerTopic != nullptr);
        assert(followerTopic->roleOf(0) == PartitionRole::Follower);

        bool caughtUp = false;
        for (int attempt = 0; attempt < 100 && !caughtUp; ++attempt) {
            PartitionLog* followerLog = followerTopic->partition(0);
            if (followerLog && followerLog->nextOffset() == 3) caughtUp = true;  // 1 + 2 produces above
            else std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
        assert(caughtUp && "follower should replicate all records from the leader");

        PartitionLog* followerLog = followerTopic->partition(0);
        auto followerRecords = followerLog->fetch(0, 10);
        assert(followerRecords.size() == 3);
        assert(followerRecords[0].key == "explicit-key");
        assert(followerRecords[0].timestampMs > 0);  // preserved from the leader, not zero/regenerated

        std::cout << "[PASS] Follower replication actually persists data on the follower's own disk\n";
    }

    // --- Late-joining broker: create a SECOND topic only on broker0, then start broker2
    //     AFTER it already exists - must self-heal via periodic Metadata pull-reconciliation,
    //     since it can't have received the AnnounceTopic broadcast (it wasn't running yet). ---
    {
        std::vector<uint8_t> respPayload;
        CreateTopicRequest req{"late-topic", 1, 1};
        StatusCode status = sendRequest(kBroker0Port, RequestType::CreateTopic, req,
                                         &encodeCreateTopicRequest, respPayload);
        assert(status == StatusCode::Ok);

        BrokerServer broker2(scratchDir("b2"), kBroker2Port, cluster2);
        std::thread t2([&broker2]() { broker2.run(); });

        bool found = false;
        // Reconciliation runs on a ~5s interval - generous timeout for that plus startup.
        for (int attempt = 0; attempt < 140 && !found; ++attempt) {
            if (broker2.registry().getTopic("late-topic") != nullptr) found = true;
            else std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        assert(found && "a broker started after CreateTopic must self-heal via pull-reconciliation");
        std::cout << "[PASS] A late-joining broker learns about existing topics via reconciliation\n";

        broker2.stop();
        t2.join();
    }

    // --- Leader-down: producing via the surviving broker must fail cleanly, not hang/crash ---
    {
        // Stop whichever broker leads partition 1.
        BrokerServer& leaderServer1 = (leaderOfPartition1 == 0) ? broker0 : broker1;
        BrokerServer& survivor1 = (leaderOfPartition1 == 0) ? broker1 : broker0;
        uint16_t survivorPort1 = (leaderOfPartition1 == 0) ? kBroker1Port : kBroker0Port;

        leaderServer1.stop();  // this also stops broker0 or broker1 entirely - fine, last use of it

        std::vector<uint8_t> respPayload;
        ProduceRequest req;
        req.topic = "cluster-topic";
        req.partition = 1;
        req.key = std::nullopt;
        req.value = "should not succeed";

        StatusCode status;
        bool gotResponse = false;
        for (int attempt = 0; attempt < 50 && !gotResponse; ++attempt) {
            Socket s = connectWithRetry(survivorPort1, 20);
            if (!s.valid()) {
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
                continue;
            }
            std::vector<uint8_t> payload = encodeProduceRequest(req);
            if (!writeFrame(s, static_cast<uint8_t>(RequestType::Produce), payload)) continue;
            uint8_t rawStatus;
            if (!readFrame(s, rawStatus, respPayload)) continue;
            status = static_cast<StatusCode>(rawStatus);
            gotResponse = true;
        }
        assert(gotResponse && "the surviving broker must respond, not hang forever");
        assert(status == StatusCode::LeaderUnavailable);

        std::cout << "[PASS] Producing to a partition whose leader is down fails cleanly\n";

        survivor1.stop();  // both brokers must be stopped before joining their threads below
    }

    t0.join();
    t1.join();

    std::cout << "All multi-broker integration tests passed.\n";
    return 0;
}
