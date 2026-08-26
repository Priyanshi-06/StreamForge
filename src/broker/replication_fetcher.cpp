#include "broker/replication_fetcher.h"

#include <chrono>
#include <thread>

#include "common/net/socket.h"
#include "protocol/messages.h"
#include "protocol/protocol.h"

namespace minikafka {

namespace {
constexpr auto kRetryDelay = std::chrono::milliseconds(500);
constexpr auto kIdlePollDelay = std::chrono::milliseconds(300);
}  // namespace

ReplicationFetcher::ReplicationFetcher(std::string topic, int32_t partition,
                                        std::string leaderHost, uint16_t leaderPort,
                                        PartitionLog& localLog, ActivityLog& activityLog)
    : topic_(std::move(topic)),
      partition_(partition),
      leaderHost_(std::move(leaderHost)),
      leaderPort_(leaderPort),
      localLog_(localLog),
      activityLog_(activityLog) {}

ReplicationFetcher::~ReplicationFetcher() { stop(); }

void ReplicationFetcher::start() {
    if (running_.exchange(true)) return;  // already running
    thread_ = std::thread(&ReplicationFetcher::run, this);
}

void ReplicationFetcher::stop() {
    running_ = false;
    if (thread_.joinable()) thread_.join();
}

void ReplicationFetcher::run() {
    activityLog_.add("[Replication] Following " + topic_ + "-" + std::to_string(partition_) +
                      " from leader " + leaderHost_ + ":" + std::to_string(leaderPort_));

    while (running_.load()) {
        Socket s;
        if (!s.connectTo(leaderHost_, leaderPort_)) {
            std::this_thread::sleep_for(kRetryDelay);
            continue;
        }

        FetchRequest req{topic_, partition_, localLog_.nextOffset(), 100};
        if (!writeFrame(s, static_cast<uint8_t>(RequestType::Fetch), encodeFetchRequest(req))) {
            std::this_thread::sleep_for(kRetryDelay);
            continue;
        }

        uint8_t status;
        std::vector<uint8_t> payload;
        if (!readFrame(s, status, payload) || static_cast<StatusCode>(status) != StatusCode::Ok) {
            std::this_thread::sleep_for(kRetryDelay);
            continue;
        }

        FetchResponse resp;
        if (!decodeFetchResponse(payload.data(), payload.size(), resp)) {
            std::this_thread::sleep_for(kRetryDelay);
            continue;
        }
        leaderHighWatermark_.store(resp.highWatermark);

        if (resp.records.empty()) {
            std::this_thread::sleep_for(kIdlePollDelay);
            continue;
        }

        for (const auto& rec : resp.records) {
            if (!running_.load()) break;

            bool ok = localLog_.appendReplicated(rec);
            if (!ok) {
                activityLog_.add("[Replication] FATAL desync on " + topic_ + "-" +
                                  std::to_string(partition_) + ": leader offset " +
                                  std::to_string(rec.offset) + " did not match this follower's " +
                                  "expected next offset (" + std::to_string(localLog_.nextOffset()) +
                                  "). Stopping replication for this partition - no auto-repair.");
                running_ = false;
                break;
            }
            lastAppliedOffset_.store(rec.offset);
        }
        // No delay here - catch up as fast as possible while behind.
    }
}

}  // namespace minikafka
