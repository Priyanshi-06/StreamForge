#include "broker/broker_server.h"

#include <chrono>
#include <iostream>
#include <stdexcept>
#include <thread>

#include "common/util/logger.h"
#include "common/util/hash.h"
#include "protocol/messages.h"
#include "protocol/protocol.h"

namespace minikafka {

namespace {
constexpr auto kReconciliationInterval = std::chrono::seconds(5);
constexpr auto kReconciliationCheckGranularity = std::chrono::milliseconds(200);
}  // namespace

BrokerServer::BrokerServer(std::filesystem::path dataDir, uint16_t port, ClusterConfig cluster)
    : dataDir_(std::move(dataDir)),
      port_(port),
      registry_(dataDir_, std::move(cluster)),
      offsetStore_(dataDir_ / "consumer_offsets.meta"),
      replicationManager_(registry_, activityLog_) {}

BrokerServer::BrokerServer(std::filesystem::path dataDir, uint16_t port)
    : BrokerServer(dataDir, port, ClusterConfig::singleNode("127.0.0.1", port)) {}

void BrokerServer::run() {
    Socket::globalInit();
    std::filesystem::create_directories(dataDir_);
    registry_.open();
    offsetStore_.open();
    replicationManager_.reconcileAll();  // resume following any partitions from a prior run

    // Demo/reference topic from the project's original spec: "orders", 3
    // partitions, replication factor 2. Only the lowest broker id creates
    // it, to avoid every broker in the cluster racing to do so on startup
    // (harmless either way since createTopic is idempotent, just noisy).
    bool isLowestBrokerId =
        registry_.cluster().selfBrokerId() == registry_.cluster().sortedBrokerIds().front();
    if (isLowestBrokerId && registry_.getTopic("orders") == nullptr) {
        if (createAndBroadcastTopic("orders", 3, 2)) {
            logActivity("Created default topic \"orders\" with 3 partitions");
        }
    }

    if (!listenSocket_.listenOn("0.0.0.0", port_, 16)) {
        throw std::runtime_error("BrokerServer: failed to bind/listen on port " +
                                  std::to_string(port_));
    }

    running_ = true;

    if (registry_.cluster().brokers().size() > 1) {
        reconciliationThread_ = std::thread(&BrokerServer::reconciliationLoop, this);
    }

    Logger::panel("MiniKafka Broker Running",
                  {{"Broker id", std::to_string(registry_.cluster().selfBrokerId())},
                   {"Port", std::to_string(port_)},
                   {"Data dir", dataDir_.string()},
                   {"Status", "Waiting for producer/consumer connections"}});
    Logger::info("Press Ctrl+C to stop.");

    while (running_) {
        Socket conn = listenSocket_.accept();
        if (!conn.valid()) {
            if (!running_) break;  // stop() closed the listen socket intentionally
            continue;
        }
        std::thread(&BrokerServer::handleConnection, this, std::move(conn)).detach();
    }
}

void BrokerServer::stop() {
    running_ = false;
    listenSocket_.close();
    if (reconciliationThread_.joinable()) reconciliationThread_.join();
    replicationManager_.stopAll();
}

void BrokerServer::logActivity(const std::string& line) {
    // Single operator<< call with the whole line pre-built, so lines from
    // different connection-handling threads are unlikely to interleave
    // mid-line on the terminal. Flushed immediately (std::endl) since this
    // is meant to be watched live, not read after the fact.
    Logger::activity(line);
    activityLog_.add(line);
}

void BrokerServer::handleConnection(Socket conn) {
    logActivity("Client connected");

    while (true) {
        uint8_t type;
        std::vector<uint8_t> payload;
        if (!readFrame(conn, type, payload)) break;  // client disconnected or malformed frame

        RequestType effectiveType = static_cast<RequestType>(type);
        bool isForwarded = false;
        if (effectiveType == RequestType::Forwarded) {
            RequestType innerType;
            std::vector<uint8_t> innerPayload;
            if (!unwrapForwarded(payload, innerType, innerPayload)) {
                writeFrame(conn, static_cast<uint8_t>(StatusCode::InternalError), {});
                continue;
            }
            effectiveType = innerType;
            payload = std::move(innerPayload);
            isForwarded = true;
        }

        switch (effectiveType) {
            case RequestType::CreateTopic:
                handleCreateTopic(conn, payload);
                break;
            case RequestType::Metadata:
                handleMetadata(conn, payload);
                break;
            case RequestType::Produce:
                handleProduce(conn, payload, isForwarded);
                break;
            case RequestType::Fetch:
                handleFetch(conn, payload, isForwarded);
                break;
            case RequestType::CommitOffset:
                handleCommitOffset(conn, payload, isForwarded);
                break;
            case RequestType::FetchOffset:
                handleFetchOffset(conn, payload, isForwarded);
                break;
            case RequestType::JoinGroup:
                handleJoinGroup(conn, payload, isForwarded);
                break;
            case RequestType::Heartbeat:
                handleHeartbeat(conn, payload, isForwarded);
                break;
            case RequestType::LeaveGroup:
                handleLeaveGroup(conn, payload, isForwarded);
                break;
            case RequestType::AnnounceTopic:
                handleAnnounceTopic(conn, payload);
                break;
            default:
                writeFrame(conn, static_cast<uint8_t>(StatusCode::InternalError), {});
                logActivity("Client disconnected (sent an unrecognized request type)");
                return;
        }
    }

    logActivity("Client disconnected");
}

bool BrokerServer::resolveOrForward(Socket& conn, int32_t leaderBrokerId, bool isForwarded,
                                     RequestType type, const std::vector<uint8_t>& payload) {
    bool isSelfLeader = (leaderBrokerId == registry_.cluster().selfBrokerId());

    if (isForwarded) {
        // Already forwarded once - never forward again (this is what makes
        // forwarding structurally single-hop). If the sanity check fails,
        // it means the two brokers disagree about who the leader is
        // (cluster misconfiguration), so fail cleanly instead of looping.
        if (!isSelfLeader) {
            writeFrame(conn, static_cast<uint8_t>(StatusCode::InternalError), {});
            logActivity("[Forwarding] Rejected a forwarded request for a partition this broker "
                        "doesn't lead - cluster config mismatch?");
        }
        return isSelfLeader;
    }

    if (isSelfLeader) return true;  // handle locally, as normal

    forwardToLeader(conn, leaderBrokerId, type, payload);
    return false;
}

void BrokerServer::forwardToLeader(Socket& clientConn, int32_t leaderBrokerId, RequestType innerType,
                                    const std::vector<uint8_t>& innerPayload) {
    const BrokerAddress* addr = registry_.cluster().find(leaderBrokerId);
    if (!addr) {
        writeFrame(clientConn, static_cast<uint8_t>(StatusCode::LeaderUnavailable), {});
        return;
    }

    Socket leaderConn;
    if (!leaderConn.connectTo(addr->host, addr->port)) {
        writeFrame(clientConn, static_cast<uint8_t>(StatusCode::LeaderUnavailable), {});
        return;
    }

    std::vector<uint8_t> wrapped = wrapForwarded(innerType, innerPayload);
    if (!writeFrame(leaderConn, static_cast<uint8_t>(RequestType::Forwarded), wrapped)) {
        writeFrame(clientConn, static_cast<uint8_t>(StatusCode::LeaderUnavailable), {});
        return;
    }

    uint8_t status;
    std::vector<uint8_t> respPayload;
    if (!readFrame(leaderConn, status, respPayload)) {
        writeFrame(clientConn, static_cast<uint8_t>(StatusCode::LeaderUnavailable), {});
        return;
    }

    writeFrame(clientConn, status, respPayload);
}

bool BrokerServer::applyTopicLocally(const std::string& name, int32_t numPartitions,
                                      int32_t replicationFactor) {
    bool created = registry_.createTopic(name, numPartitions, replicationFactor);
    if (created) {
        replicationManager_.reconcileAll();
    }
    return created;
}

bool BrokerServer::createAndBroadcastTopic(const std::string& name, int32_t numPartitions,
                                            int32_t replicationFactor) {
    bool created = applyTopicLocally(name, numPartitions, replicationFactor);
    if (created) {
        broadcastAnnounceTopic(name, numPartitions, replicationFactor);
    }
    return created;
}

void BrokerServer::broadcastAnnounceTopic(const std::string& name, int32_t numPartitions,
                                           int32_t replicationFactor) {
    AnnounceTopicRequest req{name, numPartitions, replicationFactor};
    std::vector<uint8_t> payload = encodeAnnounceTopicRequest(req);

    for (const auto& addr : registry_.cluster().brokers()) {
        if (addr.brokerId == registry_.cluster().selfBrokerId()) continue;  // applied locally already

        Socket s;
        if (!s.connectTo(addr.host, addr.port)) continue;  // best-effort; reconciliation heals this
        if (!writeFrame(s, static_cast<uint8_t>(RequestType::AnnounceTopic), payload)) continue;
        uint8_t status;
        std::vector<uint8_t> respPayload;
        readFrame(s, status, respPayload);  // don't care - best-effort push
    }
}

void BrokerServer::reconciliationLoop() {
    while (running_.load()) {
        for (int i = 0; running_.load() && i < static_cast<int>(kReconciliationInterval /
                                                                  kReconciliationCheckGranularity);
             ++i) {
            std::this_thread::sleep_for(kReconciliationCheckGranularity);
        }
        if (!running_.load()) break;

        for (const auto& addr : registry_.cluster().brokers()) {
            if (addr.brokerId == registry_.cluster().selfBrokerId()) continue;

            Socket s;
            if (!s.connectTo(addr.host, addr.port)) continue;

            MetadataRequest req{""};
            if (!writeFrame(s, static_cast<uint8_t>(RequestType::Metadata), encodeMetadataRequest(req))) {
                continue;
            }
            uint8_t status;
            std::vector<uint8_t> payload;
            if (!readFrame(s, status, payload) || static_cast<StatusCode>(status) != StatusCode::Ok) {
                continue;
            }
            MetadataResponse resp;
            if (!decodeMetadataResponse(payload.data(), payload.size(), resp)) continue;

            for (const auto& t : resp.topics) {
                if (registry_.getTopic(t.name) != nullptr) continue;  // already known locally
                if (applyTopicLocally(t.name, t.numPartitions, t.replicationFactor)) {
                    logActivity("[Cluster] Learned about topic \"" + t.name +
                                "\" via reconciliation with broker " + std::to_string(addr.brokerId));
                }
            }
        }
    }
}

void BrokerServer::handleCreateTopic(Socket& conn, const std::vector<uint8_t>& payload) {
    CreateTopicRequest req;
    if (!decodeCreateTopicRequest(payload.data(), payload.size(), req)) {
        writeFrame(conn, static_cast<uint8_t>(StatusCode::InternalError), {});
        return;
    }

    bool created = createAndBroadcastTopic(req.topicName, req.numPartitions, req.replicationFactor);
    writeFrame(conn,
               static_cast<uint8_t>(created ? StatusCode::Ok : StatusCode::TopicExists), {});

    if (created) {
        logActivity("Created topic \"" + req.topicName + "\" with " +
                    std::to_string(req.numPartitions) + " partition(s)");
    } else {
        logActivity("CreateTopic \"" + req.topicName + "\" rejected - topic already exists");
    }
}

void BrokerServer::handleAnnounceTopic(Socket& conn, const std::vector<uint8_t>& payload) {
    AnnounceTopicRequest req;
    if (!decodeAnnounceTopicRequest(payload.data(), payload.size(), req)) {
        writeFrame(conn, static_cast<uint8_t>(StatusCode::InternalError), {});
        return;
    }

    bool created = applyTopicLocally(req.name, req.numPartitions, req.replicationFactor);
    writeFrame(conn, static_cast<uint8_t>(StatusCode::Ok), {});  // idempotent - Ok either way

    if (created) {
        logActivity("[Cluster] Learned about topic \"" + req.name + "\" via AnnounceTopic");
    }
}

void BrokerServer::handleMetadata(Socket& conn, const std::vector<uint8_t>& payload) {
    MetadataRequest req;
    if (!decodeMetadataRequest(payload.data(), payload.size(), req)) {
        writeFrame(conn, static_cast<uint8_t>(StatusCode::InternalError), {});
        return;
    }

    auto describeTopic = [](Topic* topic) {
        MetadataTopicInfo info;
        info.name = topic->name();
        info.numPartitions = topic->numPartitions();
        info.replicationFactor = topic->replicationFactor();
        for (int32_t i = 0; i < topic->numPartitions(); ++i) {
            info.partitions.push_back(MetadataPartitionInfo{i, topic->replicaBrokerIds(i)});
        }
        return info;
    };

    MetadataResponse resp;
    if (req.topicName.empty()) {
        for (const auto& summary : registry_.listTopics()) {
            Topic* topic = registry_.getTopic(summary.name);
            if (topic) resp.topics.push_back(describeTopic(topic));
        }
    } else {
        Topic* topic = registry_.getTopic(req.topicName);
        if (!topic) {
            writeFrame(conn, static_cast<uint8_t>(StatusCode::UnknownTopic), {});
            return;
        }
        resp.topics.push_back(describeTopic(topic));
    }

    writeFrame(conn, static_cast<uint8_t>(StatusCode::Ok), encodeMetadataResponse(resp));
}

void BrokerServer::handleProduce(Socket& conn, const std::vector<uint8_t>& payload, bool isForwarded) {
    ProduceRequest req;
    if (!decodeProduceRequest(payload.data(), payload.size(), req)) {
        writeFrame(conn, static_cast<uint8_t>(StatusCode::InternalError), {});
        return;
    }

    Topic* topic = registry_.getTopic(req.topic);
    if (!topic) {
        writeFrame(conn, static_cast<uint8_t>(StatusCode::UnknownTopic), {});
        return;
    }

    int32_t partition = req.partition;
    if (partition == -1) {
        // Partition selection MUST happen here, before forwarding - the
        // leader must not re-run hash/round-robin (round-robin especially
        // would pick a different partition than the one this broker
        // already committed to telling the client about).
        partition = req.key.has_value()
                        ? static_cast<int32_t>(fnv1a32(*req.key) %
                                                static_cast<uint32_t>(topic->numPartitions()))
                        : topic->nextRoundRobinPartition();
    } else if (partition < 0 || partition >= topic->numPartitions()) {
        writeFrame(conn, static_cast<uint8_t>(StatusCode::UnknownPartition), {});
        return;
    }

    int32_t leaderId = topic->leaderBrokerId(partition);

    // If a forward is needed, it must carry the now-resolved partition,
    // not the original -1 - the leader must not re-run hash/round-robin.
    std::vector<uint8_t> forwardPayload;
    if (req.partition == partition) {
        forwardPayload = payload;
    } else {
        ProduceRequest resolved = req;
        resolved.partition = partition;
        forwardPayload = encodeProduceRequest(resolved);
    }
    if (!resolveOrForward(conn, leaderId, isForwarded, RequestType::Produce, forwardPayload)) return;

    PartitionLog* log = topic->partition(partition);
    int64_t offset = log->append(req.key.value_or(std::string()), req.value);

    ProduceResponse resp{partition, offset};
    writeFrame(conn, static_cast<uint8_t>(StatusCode::Ok), encodeProduceResponse(resp));

    logActivity("Produce -> topic \"" + req.topic + "\", partition " + std::to_string(partition) +
                ", offset " + std::to_string(offset) +
                (req.key.has_value() ? " (key: \"" + *req.key + "\")" : " (no key)"));
}

void BrokerServer::handleFetch(Socket& conn, const std::vector<uint8_t>& payload, bool isForwarded) {
    FetchRequest req;
    if (!decodeFetchRequest(payload.data(), payload.size(), req)) {
        writeFrame(conn, static_cast<uint8_t>(StatusCode::InternalError), {});
        return;
    }

    Topic* topic = registry_.getTopic(req.topic);
    if (!topic) {
        writeFrame(conn, static_cast<uint8_t>(StatusCode::UnknownTopic), {});
        return;
    }
    if (req.partition < 0 || req.partition >= topic->numPartitions()) {
        writeFrame(conn, static_cast<uint8_t>(StatusCode::UnknownPartition), {});
        return;
    }

    int32_t leaderId = topic->leaderBrokerId(req.partition);
    if (!resolveOrForward(conn, leaderId, isForwarded, RequestType::Fetch, payload)) return;

    PartitionLog* log = topic->partition(req.partition);

    // fetch() copies matching records out and releases PartitionLog's
    // internal lock before returning, so no lock is held across the
    // socket write below.
    FetchResponse resp;
    resp.records = log->fetch(req.startOffset, req.maxRecords);
    resp.highWatermark = log->nextOffset();

    writeFrame(conn, static_cast<uint8_t>(StatusCode::Ok), encodeFetchResponse(resp));

    logActivity("Fetch -> topic \"" + req.topic + "\", partition " +
                std::to_string(req.partition) + ", from offset " +
                std::to_string(req.startOffset) + " - returned " +
                std::to_string(resp.records.size()) + " record(s)");
}

void BrokerServer::handleCommitOffset(Socket& conn, const std::vector<uint8_t>& payload,
                                       bool isForwarded) {
    CommitOffsetRequest req;
    if (!decodeCommitOffsetRequest(payload.data(), payload.size(), req)) {
        writeFrame(conn, static_cast<uint8_t>(StatusCode::InternalError), {});
        return;
    }

    Topic* topic = registry_.getTopic(req.topic);
    if (!topic) {
        writeFrame(conn, static_cast<uint8_t>(StatusCode::UnknownTopic), {});
        return;
    }
    if (req.partition < 0 || req.partition >= topic->numPartitions()) {
        writeFrame(conn, static_cast<uint8_t>(StatusCode::UnknownPartition), {});
        return;
    }

    int32_t leaderId = topic->leaderBrokerId(req.partition);
    if (!resolveOrForward(conn, leaderId, isForwarded, RequestType::CommitOffset, payload)) return;

    if (!groupCoordinator_.isOwner(req.group, req.topic, req.partition, req.consumerId)) {
        writeFrame(conn, static_cast<uint8_t>(StatusCode::PartitionOwnedByAnother), {});
        return;
    }

    offsetStore_.commit(req.group, req.topic, req.partition, req.offset);
    writeFrame(conn, static_cast<uint8_t>(StatusCode::Ok), {});

    logActivity("Saved progress -> group \"" + req.group + "\", topic \"" + req.topic +
                "\", partition " + std::to_string(req.partition) + ", offset " +
                std::to_string(req.offset));
}

void BrokerServer::handleFetchOffset(Socket& conn, const std::vector<uint8_t>& payload,
                                      bool isForwarded) {
    FetchOffsetRequest req;
    if (!decodeFetchOffsetRequest(payload.data(), payload.size(), req)) {
        writeFrame(conn, static_cast<uint8_t>(StatusCode::InternalError), {});
        return;
    }

    Topic* topic = registry_.getTopic(req.topic);
    if (!topic) {
        writeFrame(conn, static_cast<uint8_t>(StatusCode::UnknownTopic), {});
        return;
    }
    if (req.partition < 0 || req.partition >= topic->numPartitions()) {
        writeFrame(conn, static_cast<uint8_t>(StatusCode::UnknownPartition), {});
        return;
    }

    int32_t leaderId = topic->leaderBrokerId(req.partition);
    if (!resolveOrForward(conn, leaderId, isForwarded, RequestType::FetchOffset, payload)) return;

    if (!groupCoordinator_.isOwner(req.group, req.topic, req.partition, req.consumerId)) {
        writeFrame(conn, static_cast<uint8_t>(StatusCode::PartitionOwnedByAnother), {});
        return;
    }

    FetchOffsetResponse resp{offsetStore_.fetch(req.group, req.topic, req.partition)};
    writeFrame(conn, static_cast<uint8_t>(StatusCode::Ok), encodeFetchOffsetResponse(resp));
}

void BrokerServer::handleJoinGroup(Socket& conn, const std::vector<uint8_t>& payload,
                                    bool isForwarded) {
    GroupMembershipRequest req;
    if (!decodeGroupMembershipRequest(payload.data(), payload.size(), req)) {
        writeFrame(conn, static_cast<uint8_t>(StatusCode::InternalError), {});
        return;
    }

    Topic* topic = registry_.getTopic(req.topic);
    if (!topic) {
        writeFrame(conn, static_cast<uint8_t>(StatusCode::UnknownTopic), {});
        return;
    }
    if (req.partition < 0 || req.partition >= topic->numPartitions()) {
        writeFrame(conn, static_cast<uint8_t>(StatusCode::UnknownPartition), {});
        return;
    }

    int32_t leaderId = topic->leaderBrokerId(req.partition);
    if (!resolveOrForward(conn, leaderId, isForwarded, RequestType::JoinGroup, payload)) return;

    bool claimed = groupCoordinator_.claim(req.group, req.topic, req.partition, req.consumerId);
    writeFrame(conn,
               static_cast<uint8_t>(claimed ? StatusCode::Ok : StatusCode::PartitionOwnedByAnother),
               {});

    if (claimed) {
        logActivity("JoinGroup -> \"" + req.consumerId + "\" now owns group \"" + req.group +
                    "\", topic \"" + req.topic + "\", partition " + std::to_string(req.partition));
    } else {
        logActivity("JoinGroup rejected -> \"" + req.consumerId + "\" tried group \"" + req.group +
                    "\", topic \"" + req.topic + "\", partition " + std::to_string(req.partition) +
                    " but it's already owned by another active consumer");
    }
}

void BrokerServer::handleHeartbeat(Socket& conn, const std::vector<uint8_t>& payload,
                                    bool isForwarded) {
    GroupMembershipRequest req;
    if (!decodeGroupMembershipRequest(payload.data(), payload.size(), req)) {
        writeFrame(conn, static_cast<uint8_t>(StatusCode::InternalError), {});
        return;
    }

    Topic* topic = registry_.getTopic(req.topic);
    if (!topic) {
        writeFrame(conn, static_cast<uint8_t>(StatusCode::UnknownTopic), {});
        return;
    }
    if (req.partition < 0 || req.partition >= topic->numPartitions()) {
        writeFrame(conn, static_cast<uint8_t>(StatusCode::UnknownPartition), {});
        return;
    }

    int32_t leaderId = topic->leaderBrokerId(req.partition);
    if (!resolveOrForward(conn, leaderId, isForwarded, RequestType::Heartbeat, payload)) return;

    bool ok = groupCoordinator_.claim(req.group, req.topic, req.partition, req.consumerId);
    writeFrame(conn, static_cast<uint8_t>(ok ? StatusCode::Ok : StatusCode::PartitionOwnedByAnother),
               {});
}

void BrokerServer::handleLeaveGroup(Socket& conn, const std::vector<uint8_t>& payload,
                                     bool isForwarded) {
    GroupMembershipRequest req;
    if (!decodeGroupMembershipRequest(payload.data(), payload.size(), req)) {
        writeFrame(conn, static_cast<uint8_t>(StatusCode::InternalError), {});
        return;
    }

    Topic* topic = registry_.getTopic(req.topic);
    if (!topic) {
        writeFrame(conn, static_cast<uint8_t>(StatusCode::UnknownTopic), {});
        return;
    }
    if (req.partition < 0 || req.partition >= topic->numPartitions()) {
        writeFrame(conn, static_cast<uint8_t>(StatusCode::UnknownPartition), {});
        return;
    }

    int32_t leaderId = topic->leaderBrokerId(req.partition);
    if (!resolveOrForward(conn, leaderId, isForwarded, RequestType::LeaveGroup, payload)) return;

    groupCoordinator_.release(req.group, req.topic, req.partition, req.consumerId);
    writeFrame(conn, static_cast<uint8_t>(StatusCode::Ok), {});

    logActivity("LeaveGroup -> \"" + req.consumerId + "\" released group \"" + req.group +
                "\", topic \"" + req.topic + "\", partition " + std::to_string(req.partition));
}

}  // namespace minikafka
