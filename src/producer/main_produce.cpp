// minikafka-produce: connects to a broker and sends one or more Produce
// requests, letting the broker choose the partition (hash(key) if a key
// is given, otherwise broker-side round robin).
//
// Usage: minikafka-produce [--host=H] [--port=P] <topic> [key] [value]
//   <topic> only          - reads lines from stdin as unkeyed message values
//   <topic> <value>       - produce one unkeyed message
//   <topic> <key> <value> - produce one keyed message

#include <cstdlib>
#include <iostream>
#include <optional>
#include <string>
#include <vector>

#include "common/net/socket.h"
#include "common/util/logger.h"
#include "protocol/messages.h"
#include "protocol/protocol.h"

using namespace minikafka;

namespace
{

    bool produceOne(const std::string &host, uint16_t port, const std::string &topic,
                    const std::optional<std::string> &key, const std::string &value)
    {
        Socket s;
        if (!s.connectTo(host, port))
        {
            Logger::error("Failed to connect to broker at " + host + ":" + std::to_string(port));
            return false;
        }

        ProduceRequest req;
        req.topic = topic;
        req.partition = -1;
        req.key = key;
        req.value = value;

        if (!writeFrame(s, static_cast<uint8_t>(RequestType::Produce), encodeProduceRequest(req)))
        {
            Logger::error("Failed to send produce request");
            return false;
        }

        uint8_t status;
        std::vector<uint8_t> respPayload;
        if (!readFrame(s, status, respPayload))
        {
            Logger::error("Failed to read broker response (connection closed?)");
            return false;
        }
        if (static_cast<StatusCode>(status) != StatusCode::Ok)
        {
            Logger::error("Broker returned error status " + std::to_string(static_cast<int>(status)));
            return false;
        }

        ProduceResponse resp;
        if (!decodeProduceResponse(respPayload.data(), respPayload.size(), resp))
        {
            Logger::error("Failed to decode produce response");
            return false;
        }

        Logger::panel("Message Produced",
                      {{"Topic", topic},
                       {"Partition", std::to_string(resp.partition)},
                       {"Offset", std::to_string(resp.offset)},
                       {"Key", key.has_value() ? *key : std::string("None")},
                       {"Status", "SUCCESS"}});

        if (key.has_value())
            Logger::info("Partition selected from key \"" + *key + "\".");
        else
            Logger::info("No key supplied; broker selected the partition by round robin.");
        return true;
    }

} // namespace

int main(int argc, char **argv)
{
    std::string host = "127.0.0.1";
    uint16_t port = 9092;
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
        else
        {
            positional.push_back(arg);
        }
    }

    if (positional.empty())
    {
        Logger::usage("minikafka-produce [--host=H] [--port=P] <topic> [key] [value]",
                      {"<topic> only          reads lines from stdin as unkeyed message values",
                       "<topic> <value>       produce one unkeyed message",
                       "<topic> <key> <value> produce one keyed message"});
        return 1;
    }

    Socket::globalInit();

    const std::string &topic = positional[0];
    bool ok = true;

    if (positional.size() == 1)
    {
        std::string line;
        while (std::getline(std::cin, line))
        {
            if (!produceOne(host, port, topic, std::nullopt, line))
            {
                ok = false;
                break;
            }
        }
    }
    else if (positional.size() == 2)
    {
        ok = produceOne(host, port, topic, std::nullopt, positional[1]);
    }
    else
    {
        ok = produceOne(host, port, topic, positional[1], positional[2]);
    }

    Socket::globalCleanup();
    return ok ? 0 : 1;
}
