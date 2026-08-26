#include "broker/cluster_config.h"

#include <algorithm>
#include <sstream>
#include <stdexcept>
#include <unordered_set>

namespace minikafka {

ClusterConfig::ClusterConfig(int32_t selfBrokerId, std::vector<BrokerAddress> brokers)
    : selfBrokerId_(selfBrokerId), brokers_(std::move(brokers)) {
    std::sort(brokers_.begin(), brokers_.end(),
              [](const BrokerAddress& a, const BrokerAddress& b) { return a.brokerId < b.brokerId; });
}

ClusterConfig ClusterConfig::singleNode(std::string host, uint16_t port) {
    return ClusterConfig(0, {BrokerAddress{0, std::move(host), port}});
}

ClusterConfig ClusterConfig::parse(int32_t selfBrokerId, const std::string& clusterArg) {
    std::vector<BrokerAddress> brokers;
    std::unordered_set<int32_t> seenIds;

    std::stringstream ss(clusterArg);
    std::string entry;
    while (std::getline(ss, entry, ',')) {
        if (entry.empty()) continue;

        size_t firstColon = entry.find(':');
        size_t secondColon = entry.rfind(':');
        if (firstColon == std::string::npos || secondColon == firstColon) {
            throw std::runtime_error("ClusterConfig: malformed cluster entry \"" + entry +
                                      "\" (expected id:host:port)");
        }

        std::string idPart = entry.substr(0, firstColon);
        std::string hostPart = entry.substr(firstColon + 1, secondColon - firstColon - 1);
        std::string portPart = entry.substr(secondColon + 1);
        if (idPart.empty() || hostPart.empty() || portPart.empty()) {
            throw std::runtime_error("ClusterConfig: malformed cluster entry \"" + entry + "\"");
        }

        int32_t id = std::stoi(idPart);
        uint16_t port = static_cast<uint16_t>(std::stoi(portPart));

        if (!seenIds.insert(id).second) {
            throw std::runtime_error("ClusterConfig: duplicate broker id " + std::to_string(id) +
                                      " in --cluster");
        }
        brokers.push_back(BrokerAddress{id, hostPart, port});
    }

    if (brokers.empty()) {
        throw std::runtime_error("ClusterConfig: --cluster produced no brokers");
    }
    if (seenIds.find(selfBrokerId) == seenIds.end()) {
        throw std::runtime_error("ClusterConfig: --broker-id=" + std::to_string(selfBrokerId) +
                                  " not found in --cluster list");
    }

    return ClusterConfig(selfBrokerId, std::move(brokers));
}

std::vector<int32_t> ClusterConfig::sortedBrokerIds() const {
    std::vector<int32_t> ids;
    ids.reserve(brokers_.size());
    for (const auto& b : brokers_) ids.push_back(b.brokerId);
    return ids;
}

const BrokerAddress* ClusterConfig::find(int32_t brokerId) const {
    for (const auto& b : brokers_) {
        if (b.brokerId == brokerId) return &b;
    }
    return nullptr;
}

}  // namespace minikafka
