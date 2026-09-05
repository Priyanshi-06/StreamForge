// minikafka-consume: claims exclusive ownership of a (group, topic,
// partition) via JoinGroup before consuming, so two consumers sharing the
// same group name can never process the same partition concurrently and
// race on offset commits. Fetches the last committed offset, prints
// records from there forward, and commits its new position after each
// batch so it can resume where it left off if disconnected and restarted.
// Releases ownership (LeaveGroup) on exit; if it's killed instead, the
// broker-side lease simply expires after a few seconds, freeing the
// partition for the next consumer.
//
// Usage: minikafka-consume [--host=H] [--port=P] [--follow]
//                           [--consumer-id=ID] <topic> <partition> <group>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "common/net/socket.h"
#include "common/util/logger.h"
#include "protocol/messages.h"
#include "protocol/protocol.h"

#ifdef _WIN32
#include <process.h>
#else
#include <unistd.h>
#endif

using namespace minikafka;

namespace
{

    int currentProcessId()
    {
#ifdef _WIN32
        return _getpid();
#else
        return getpid();
#endif
    }

    // Result of a group-membership request: distinguishes "another consumer
    // owns this partition" (expected, actionable) from a connection/protocol
    // failure (unexpected).
    enum class MembershipResult
    {
        Ok,
        OwnedByAnother,
        Failed
    };

    MembershipResult sendMembershipRequest(RequestType type, const std::string &host, uint16_t port,
                                           const std::string &group, const std::string &topic,
                                           int32_t partition, const std::string &consumerId)
    {
        Socket s;
        if (!s.connectTo(host, port))
            return MembershipResult::Failed;

        GroupMembershipRequest req{group, topic, partition, consumerId};
        if (!writeFrame(s, static_cast<uint8_t>(type), encodeGroupMembershipRequest(req)))
        {
            return MembershipResult::Failed;
        }

        uint8_t status;
        std::vector<uint8_t> payload;
        if (!readFrame(s, status, payload))
            return MembershipResult::Failed;

        switch (static_cast<StatusCode>(status))
        {
        case StatusCode::Ok:
            return MembershipResult::Ok;
        case StatusCode::PartitionOwnedByAnother:
            return MembershipResult::OwnedByAnother;
        default:
            return MembershipResult::Failed;
        }
    }

    bool fetchCommittedOffset(const std::string &host, uint16_t port, const std::string &group,
                              const std::string &topic, int32_t partition,
                              const std::string &consumerId, int64_t &outOffset)
    {
        Socket s;
        if (!s.connectTo(host, port))
            return false;

        FetchOffsetRequest req{group, topic, partition, consumerId};
        if (!writeFrame(s, static_cast<uint8_t>(RequestType::FetchOffset),
                        encodeFetchOffsetRequest(req)))
        {
            return false;
        }

        uint8_t status;
        std::vector<uint8_t> payload;
        if (!readFrame(s, status, payload))
            return false;
        if (static_cast<StatusCode>(status) != StatusCode::Ok)
        {
            Logger::error("FetchOffset error status " + std::to_string(static_cast<int>(status)));
            return false;
        }

        FetchOffsetResponse resp;
        if (!decodeFetchOffsetResponse(payload.data(), payload.size(), resp))
            return false;
        outOffset = resp.offset;
        return true;
    }

    bool fetchRecords(const std::string &host, uint16_t port, const std::string &topic,
                      int32_t partition, int64_t startOffset, int32_t maxRecords,
                      FetchResponse &outResp)
    {
        Socket s;
        if (!s.connectTo(host, port))
            return false;

        FetchRequest req{topic, partition, startOffset, maxRecords};
        if (!writeFrame(s, static_cast<uint8_t>(RequestType::Fetch), encodeFetchRequest(req)))
        {
            return false;
        }

        uint8_t status;
        std::vector<uint8_t> payload;
        if (!readFrame(s, status, payload))
            return false;
        if (static_cast<StatusCode>(status) != StatusCode::Ok)
        {
            Logger::error("Fetch error status " + std::to_string(static_cast<int>(status)));
            return false;
        }

        return decodeFetchResponse(payload.data(), payload.size(), outResp);
    }

    bool commitOffset(const std::string &host, uint16_t port, const std::string &group,
                      const std::string &topic, int32_t partition, int64_t offset,
                      const std::string &consumerId)
    {
        Socket s;
        if (!s.connectTo(host, port))
            return false;

        CommitOffsetRequest req{group, topic, partition, offset, consumerId};
        if (!writeFrame(s, static_cast<uint8_t>(RequestType::CommitOffset),
                        encodeCommitOffsetRequest(req)))
        {
            return false;
        }

        uint8_t status;
        std::vector<uint8_t> payload;
        if (!readFrame(s, status, payload))
            return false;
        return static_cast<StatusCode>(status) == StatusCode::Ok;
    }

} // namespace

int main(int argc, char **argv)
{
    std::string host = "127.0.0.1";
    uint16_t port = 9092;
    bool follow = false;
    std::string consumerId = "pid-" + std::to_string(currentProcessId());
    std::vector<std::string> positional;

    for (int i = 1; i < argc; ++i)
    {
        std::string arg = argv[i];
        if (arg.rfind("--host=", 0) == 0)
        {
            host = arg.substr(7);
        }
        else if (arg.rfind("--port=", 0) == 0)
        {
            port = static_cast<uint16_t>(std::atoi(arg.substr(7).c_str()));
        }
        else if (arg.rfind("--consumer-id=", 0) == 0)
        {
            consumerId = arg.substr(14);
        }
        else if (arg == "--follow")
        {
            follow = true;
        }
        else
        {
            positional.push_back(arg);
        }
    }

    if (positional.size() != 3)
    {
        Logger::usage("minikafka-consume [--host=H] [--port=P] [--follow] "
                      "[--consumer-id=ID] <topic> <partition> <group>",
                      {"<topic>      topic to consume",
                       "<partition>  partition number",
                       "<group>      consumer group name"});
        return 1;
    }

    const std::string &topic = positional[0];
    int32_t partition = std::atoi(positional[1].c_str());
    const std::string &group = positional[2];

    Socket::globalInit();

    MembershipResult joinResult =
        sendMembershipRequest(RequestType::JoinGroup, host, port, group, topic, partition, consumerId);
    if (joinResult == MembershipResult::OwnedByAnother)
    {
        Logger::error("Cannot start: group \"" + group + "\" is already consuming topic \"" +
                      topic + "\", partition " + std::to_string(partition) + ".");
        Logger::info("Use a different group name, or wait for that consumer to finish/time out.");
        return 1;
    }
    if (joinResult == MembershipResult::Failed)
    {
        Logger::error("Failed to join group - is the broker running at " + host + ":" +
                      std::to_string(port) + "?");
        return 1;
    }

    int exitCode = 0;
    std::atomic<bool> stopHeartbeat{false};
    std::thread heartbeatThread;
    if (follow)
    {
        heartbeatThread = std::thread([&]()
                                      {
            while (!stopHeartbeat.load()) {
                std::this_thread::sleep_for(std::chrono::seconds(3));
                if (stopHeartbeat.load()) break;
                sendMembershipRequest(RequestType::Heartbeat, host, port, group, topic, partition,
                                      consumerId);
            } });
    }

    int64_t offset;
    if (!fetchCommittedOffset(host, port, group, topic, partition, consumerId, offset))
    {
        Logger::error("Failed to fetch committed offset from broker at " + host + ":" +
                      std::to_string(port));
        exitCode = 1;
    }
    else
    {
        if (offset < 0)
        {
            offset = 0;
            Logger::info("No previous progress found for group \"" + group + "\" on topic \"" +
                         topic + "\", partition " + std::to_string(partition) +
                         " - starting from the beginning.");
        }
        else
        {
            Logger::info("Resuming group \"" + group + "\" on topic \"" + topic +
                         "\", partition " + std::to_string(partition) + " from offset " +
                         std::to_string(offset) + " (where it left off last time).");
        }

        do
        {
            FetchResponse resp;
            if (!fetchRecords(host, port, topic, partition, offset, 100, resp))
            {
                Logger::error("Fetch failed - is the broker running at " + host + ":" +
                              std::to_string(port) + "?");
                exitCode = 1;
                break;
            }

            if (resp.records.empty())
            {
                if (!follow)
                {
                    Logger::info("No new messages.");
                }
            }
            else
            {
                Logger::panel("Messages Received",
                              {{"Topic", topic},
                               {"Partition", std::to_string(partition)},
                               {"Group", group},
                               {"Records", std::to_string(resp.records.size())}});
                for (const auto &rec : resp.records)
                {
                    std::cout << "  " << "[offset " << rec.offset << "] "
                              << (rec.key.has_value() ? "key=\"" + *rec.key + "\"  " : std::string())
                              << "value="
                              << (rec.value.has_value() ? "\"" + *rec.value + "\""
                                                        : std::string("<deleted>"))
                              << "\n";
                }
            }

            if (!resp.records.empty())
            {
                offset = resp.records.back().offset + 1;
                if (!commitOffset(host, port, group, topic, partition, offset, consumerId))
                {
                    Logger::warn("Failed to save progress at offset " + std::to_string(offset));
                }
            }
            else if (follow)
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
            }
        } while (follow);
    }

    stopHeartbeat = true;
    if (heartbeatThread.joinable())
        heartbeatThread.join();

    sendMembershipRequest(RequestType::LeaveGroup, host, port, group, topic, partition, consumerId);

    return exitCode;
}
