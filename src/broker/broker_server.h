#pragma once

#include <atomic>
#include <cstdint>
#include <filesystem>
#include <string>
#include <thread>

#include "broker/activity_log.h"
#include "broker/cluster_config.h"
#include "broker/consumer_offsets.h"
#include "broker/group_coordinator.h"
#include "broker/replication_manager.h"
#include "broker/topic_registry.h"
#include "common/net/socket.h"
#include "protocol/protocol.h"

namespace minikafka {

// TCP broker: accept loop + thread-per-connection, dispatching frames to
// the TopicRegistry/PartitionLog storage layer. Blocking sockets throughout
// - simplicity over throughput, matching the rest of this project's scope.
//
// Multi-broker: every partition-scoped request (Produce/Fetch/CommitOffset/
// FetchOffset/JoinGroup/Heartbeat/LeaveGroup) is accepted by ANY broker for
// ANY topic/partition. If this broker isn't the partition's leader (per the
// static, deterministic replica assignment - see broker/replica_assignment.h),
// it transparently forwards the exact request to the broker that is, and
// relays the response back - so the wire protocol and CLIs need no changes
// at all. A request that arrives already-forwarded is never re-forwarded,
// which makes forwarding structurally single-hop with no possible loop.
class BrokerServer {
public:
    // Cluster-aware constructor.
    BrokerServer(std::filesystem::path dataDir, uint16_t port, ClusterConfig cluster);

    // Single-broker convenience overload: synthesizes a degenerate 1-node
    // cluster - byte-for-byte the original single-broker behavior.
    BrokerServer(std::filesystem::path dataDir, uint16_t port);

    // Opens the registry, binds/listens, and blocks running the accept
    // loop until stop() is called from another thread. Throws
    // std::runtime_error if the listen socket can't be bound.
    void run();

    // Closes the listen socket, causing the blocking accept() in run() to
    // return and the accept loop to exit. Also stops the replication and
    // reconciliation background threads.
    void stop();

    // Read-only access for the HTTP admin dashboard (see
    // broker/http_admin_server.h), which is constructed separately in
    // main_broker.cpp and polls this same in-process state.
    TopicRegistry& registry() { return registry_; }
    GroupCoordinator& groupCoordinator() { return groupCoordinator_; }
    ActivityLog& activityLog() { return activityLog_; }
    ReplicationManager& replicationManager() { return replicationManager_; }

private:
    // Prints a single line to stdout and appends it to activityLog_, in
    // one call each, so lines from different connection-handling threads
    // don't interleave mid-line.
    void logActivity(const std::string& line);

    void handleConnection(Socket conn);

    void handleCreateTopic(Socket& conn, const std::vector<uint8_t>& payload);
    void handleMetadata(Socket& conn, const std::vector<uint8_t>& payload);
    void handleProduce(Socket& conn, const std::vector<uint8_t>& payload, bool isForwarded);
    void handleFetch(Socket& conn, const std::vector<uint8_t>& payload, bool isForwarded);
    void handleCommitOffset(Socket& conn, const std::vector<uint8_t>& payload, bool isForwarded);
    void handleFetchOffset(Socket& conn, const std::vector<uint8_t>& payload, bool isForwarded);
    void handleJoinGroup(Socket& conn, const std::vector<uint8_t>& payload, bool isForwarded);
    void handleHeartbeat(Socket& conn, const std::vector<uint8_t>& payload, bool isForwarded);
    void handleLeaveGroup(Socket& conn, const std::vector<uint8_t>& payload, bool isForwarded);
    void handleAnnounceTopic(Socket& conn, const std::vector<uint8_t>& payload);

    // Core of the multi-broker dispatch: if isForwarded, only sanity-checks
    // that this broker really is the leader (never re-forwards - that's
    // what keeps forwarding a single hop) and returns whether to proceed.
    // Otherwise, returns true if this broker IS the leader (proceed
    // locally), or forwards the request itself and returns false (caller
    // must do nothing further - a response has already been written).
    bool resolveOrForward(Socket& conn, int32_t leaderBrokerId, bool isForwarded, RequestType type,
                          const std::vector<uint8_t>& payload);
    void forwardToLeader(Socket& clientConn, int32_t leaderBrokerId, RequestType innerType,
                         const std::vector<uint8_t>& innerPayload);

    // Applies a topic locally (idempotent) and starts following any
    // partitions this broker is now a follower of. Does NOT broadcast.
    bool applyTopicLocally(const std::string& name, int32_t numPartitions, int32_t replicationFactor);
    // applyTopicLocally() + broadcasts AnnounceTopic to every other broker
    // in the cluster (best-effort - a peer that misses it self-heals via
    // the periodic Metadata-reconciliation loop below).
    bool createAndBroadcastTopic(const std::string& name, int32_t numPartitions,
                                 int32_t replicationFactor);
    void broadcastAnnounceTopic(const std::string& name, int32_t numPartitions,
                                int32_t replicationFactor);

    // Background self-heal: periodically asks every peer for its Metadata
    // and locally applies any topic this broker doesn't know about yet -
    // covers a broker that was down when AnnounceTopic was broadcast, or a
    // race between CreateTopic and an immediate forwarded request.
    void reconciliationLoop();

    std::filesystem::path dataDir_;
    uint16_t port_;
    TopicRegistry registry_;
    ConsumerOffsetStore offsetStore_;
    GroupCoordinator groupCoordinator_;
    ActivityLog activityLog_;
    ReplicationManager replicationManager_;
    Socket listenSocket_;
    std::atomic<bool> running_{false};
    std::thread reconciliationThread_;
};

}  // namespace minikafka
