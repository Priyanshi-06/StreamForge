#include "broker/topic_registry.h"

#include <fstream>
#include <sstream>

namespace minikafka {

namespace {

bool parsePartitionDirName(const std::string& dirName, std::string& outTopicName,
                            int32_t& outIndex) {
    size_t pos = dirName.rfind('-');
    if (pos == std::string::npos || pos == 0 || pos + 1 >= dirName.size()) return false;

    std::string suffix = dirName.substr(pos + 1);
    for (char c : suffix) {
        if (c < '0' || c > '9') return false;
    }

    outTopicName = dirName.substr(0, pos);
    outIndex = std::stoi(suffix);
    return true;
}

}  // namespace

TopicRegistry::TopicRegistry(std::filesystem::path dataDir, ClusterConfig cluster)
    : dataDir_(std::move(dataDir)),
      metaFilePath_(dataDir_ / "topics.meta"),
      cluster_(std::move(cluster)) {}

TopicRegistry::TopicRegistry(std::filesystem::path dataDir)
    : TopicRegistry(dataDir, ClusterConfig::singleNode("localhost", 0)) {}

std::vector<TopicRegistry::MetaEntry> TopicRegistry::loadMetaFile() const {
    std::vector<MetaEntry> entries;
    std::ifstream in(metaFilePath_);
    if (!in) return entries;

    std::string line;
    while (std::getline(in, line)) {
        if (line.empty()) continue;
        std::istringstream iss(line);
        MetaEntry e;
        if (iss >> e.name >> e.numPartitions >> e.replicationFactor) {
            entries.push_back(std::move(e));
        }
    }
    return entries;
}

std::vector<TopicRegistry::MetaEntry> TopicRegistry::scanOnDiskTopics() const {
    std::unordered_map<std::string, int32_t> maxIndexByTopic;

    if (std::filesystem::exists(dataDir_)) {
        for (const auto& entry : std::filesystem::directory_iterator(dataDir_)) {
            if (!entry.is_directory()) continue;
            std::string topicName;
            int32_t index;
            if (!parsePartitionDirName(entry.path().filename().string(), topicName, index)) {
                continue;
            }
            auto it = maxIndexByTopic.find(topicName);
            if (it == maxIndexByTopic.end() || index > it->second) {
                maxIndexByTopic[topicName] = index;
            }
        }
    }

    std::vector<MetaEntry> entries;
    for (const auto& [name, maxIndex] : maxIndexByTopic) {
        entries.push_back(MetaEntry{name, maxIndex + 1, 1});
    }
    return entries;
}

void TopicRegistry::persistMeta() const {
    std::ofstream out(metaFilePath_, std::ios::trunc);
    for (const auto& [name, topic] : topics_) {
        out << topic->name() << ' ' << topic->numPartitions() << ' ' << topic->replicationFactor()
            << '\n';
    }
}

void TopicRegistry::open() {
    std::lock_guard<std::mutex> lock(mutex_);

    std::filesystem::create_directories(dataDir_);

    std::vector<MetaEntry> metaEntries = loadMetaFile();
    std::vector<MetaEntry> onDiskEntries = scanOnDiskTopics();

    std::unordered_map<std::string, MetaEntry> merged;
    for (auto& e : metaEntries) {
        merged[e.name] = e;
    }

    bool needsPersist = false;
    for (auto& e : onDiskEntries) {
        if (merged.find(e.name) == merged.end()) {
            merged[e.name] = e;  // meta file was missing/stale for this topic - reconstruct it
            needsPersist = true;
        }
    }

    for (auto& [name, entry] : merged) {
        auto topic =
            std::make_unique<Topic>(entry.name, entry.numPartitions, entry.replicationFactor, dataDir_);
        topic->open(cluster_);
        topics_[entry.name] = std::move(topic);
    }

    if (needsPersist) {
        persistMeta();
    }
}

bool TopicRegistry::createTopic(const std::string& name, int32_t numPartitions,
                                 int32_t replicationFactor) {
    std::lock_guard<std::mutex> lock(mutex_);

    if (topics_.find(name) != topics_.end()) return false;

    auto topic = std::make_unique<Topic>(name, numPartitions, replicationFactor, dataDir_);
    topic->open(cluster_);
    topics_[name] = std::move(topic);

    persistMeta();
    return true;
}

Topic* TopicRegistry::getTopic(const std::string& name) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = topics_.find(name);
    return it == topics_.end() ? nullptr : it->second.get();
}

std::vector<TopicRegistry::TopicSummary> TopicRegistry::listTopics() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<TopicSummary> result;
    result.reserve(topics_.size());
    for (const auto& [name, topic] : topics_) {
        result.push_back({topic->name(), topic->numPartitions()});
    }
    return result;
}

}  // namespace minikafka
