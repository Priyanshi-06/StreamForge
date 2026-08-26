// Milestone 4 tests: TopicRegistry create/lookup/list, persistence of
// topics.meta across a simulated restart, and reconciliation against
// on-disk partition directories when the meta file is missing/stale.

#include <cassert>
#include <iostream>

#include "broker/topic_registry.h"

using namespace minikafka;
namespace fs = std::filesystem;

static fs::path scratchDir() {
    fs::path dir = fs::temp_directory_path() / "minikafka_test_broker_core";
    std::error_code ec;
    fs::remove_all(dir, ec);
    fs::create_directories(dir);
    return dir;
}

static void testCreateAndLookup() {
    fs::path dir = scratchDir();
    TopicRegistry registry(dir);
    registry.open();

    bool created = registry.createTopic("orders", 3, 2);
    assert(created);

    bool createdAgain = registry.createTopic("orders", 3, 2);
    assert(!createdAgain && "creating an existing topic must fail");

    Topic* topic = registry.getTopic("orders");
    assert(topic != nullptr);
    assert(topic->numPartitions() == 3);
    assert(topic->replicationFactor() == 2);

    assert(registry.getTopic("nonexistent") == nullptr);

    PartitionLog* p0 = topic->partition(0);
    assert(p0 != nullptr);
    int64_t offset = p0->append("k", "v");
    assert(offset == 0);

    assert(topic->partition(3) == nullptr);  // out of range for 3 partitions (0,1,2)

    auto summaries = registry.listTopics();
    assert(summaries.size() == 1);
    assert(summaries[0].name == "orders");
    assert(summaries[0].numPartitions == 3);

    std::cout << "[PASS] TopicRegistry create/lookup/list\n";
}

static void testPersistenceAcrossRestart() {
    fs::path dir = scratchDir();

    {
        TopicRegistry registry(dir);
        registry.open();
        registry.createTopic("orders", 3, 2);
        Topic* topic = registry.getTopic("orders");
        topic->partition(1)->append("key-a", "value-a");
        topic->partition(1)->append("key-b", "value-b");
    }  // registry destroyed, all files closed - simulates broker shutdown

    assert(fs::exists(dir / "topics.meta"));

    TopicRegistry reopened(dir);
    reopened.open();

    Topic* topic = reopened.getTopic("orders");
    assert(topic != nullptr);
    assert(topic->numPartitions() == 3);
    assert(topic->replicationFactor() == 2);

    auto records = topic->partition(1)->fetch(0, 10);
    assert(records.size() == 2);
    assert(records[0].key == "key-a");
    assert(records[1].key == "key-b");

    std::cout << "[PASS] TopicRegistry persists topics.meta across restart\n";
}

static void testReconciliationWithoutMetaFile() {
    fs::path dir = scratchDir();

    {
        TopicRegistry registry(dir);
        registry.open();
        registry.createTopic("payments", 4, 1);
        registry.getTopic("payments")->partition(2)->append("k", "v");
    }

    // Simulate a lost/corrupted meta file: the on-disk partition
    // directories (payments-0 .. payments-3) are still there.
    fs::remove(dir / "topics.meta");
    assert(!fs::exists(dir / "topics.meta"));
    assert(fs::exists(dir / "payments-3"));

    TopicRegistry reconciled(dir);
    reconciled.open();

    Topic* topic = reconciled.getTopic("payments");
    assert(topic != nullptr);
    assert(topic->numPartitions() == 4 && "should infer partition count from on-disk dirs");

    auto records = topic->partition(2)->fetch(0, 10);
    assert(records.size() == 1);
    assert(records[0].key == "k");

    // Reconciliation should have rewritten topics.meta so future opens
    // don't need to re-scan the directory.
    assert(fs::exists(dir / "topics.meta"));

    std::cout << "[PASS] TopicRegistry reconciles on-disk dirs when meta file is missing\n";
}

int main() {
    testCreateAndLookup();
    testPersistenceAcrossRestart();
    testReconciliationWithoutMetaFile();
    std::cout << "All Milestone 4 tests passed.\n";
    return 0;
}
