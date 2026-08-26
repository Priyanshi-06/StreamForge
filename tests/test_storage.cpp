// Milestone 2 tests: Record encode/decode round trips, LogSegment
// append/read/recover (including deliberate torn-write truncation), and
// PartitionLog append/fetch/rolling/reopen behavior.

#include <cassert>
#include <cstdio>
#include <filesystem>
#include <iostream>

#include "storage/log_segment.h"
#include "storage/partition_log.h"
#include "storage/record.h"

using namespace minikafka;
namespace fs = std::filesystem;

static fs::path scratchDir() {
    fs::path dir = fs::temp_directory_path() / "minikafka_test_storage";
    std::error_code ec;
    fs::remove_all(dir, ec);
    fs::create_directories(dir);
    return dir;
}

static void testRecordRoundTrip() {
    Record r;
    r.offset = 42;
    r.timestampMs = 1700000000123;
    r.key = std::string("order-1");
    r.value = std::string("payload-bytes");

    std::vector<uint8_t> encoded = encodeRecord(r);

    Record decoded;
    size_t totalBytes;
    bool crcOk;
    bool ok = tryDecodeRecord(encoded.data(), encoded.size(), decoded, totalBytes, crcOk);
    assert(ok);
    assert(crcOk);
    assert(totalBytes == encoded.size());
    assert(decoded.offset == r.offset);
    assert(decoded.timestampMs == r.timestampMs);
    assert(decoded.key == r.key);
    assert(decoded.value == r.value);

    // Null key / tombstone value.
    Record r2;
    r2.offset = 43;
    r2.timestampMs = 1700000000456;
    r2.key = std::nullopt;
    r2.value = std::nullopt;
    std::vector<uint8_t> encoded2 = encodeRecord(r2);
    Record decoded2;
    ok = tryDecodeRecord(encoded2.data(), encoded2.size(), decoded2, totalBytes, crcOk);
    assert(ok && crcOk);
    assert(!decoded2.key.has_value());
    assert(!decoded2.value.has_value());

    // Insufficient bytes (torn) must be reported as "not decodable".
    ok = tryDecodeRecord(encoded.data(), encoded.size() - 1, decoded, totalBytes, crcOk);
    assert(!ok);

    std::cout << "[PASS] record encode/decode round trip\n";
}

static void testLogSegmentAppendRead() {
    fs::path dir = scratchDir();
    fs::path file = dir / "00000000000000000000.log";

    LogSegment seg(file, 0);
    seg.open();
    seg.recover();

    Record a;
    a.offset = 0;
    a.timestampMs = 1;
    a.key = std::string("k0");
    a.value = std::string("v0");
    size_t posA = seg.append(a);

    Record b;
    b.offset = 1;
    b.timestampMs = 2;
    b.key = std::string("k1");
    b.value = std::string("v1");
    seg.append(b);

    auto readA = seg.readAt(posA);
    assert(readA.has_value());
    assert(readA->key == "k0");
    assert(readA->value == "v0");

    auto posB = seg.findFilePos(1);
    assert(posB.has_value());
    auto readB = seg.readAt(*posB);
    assert(readB.has_value());
    assert(readB->key == "k1");

    std::cout << "[PASS] LogSegment append/read\n";
}

static void testLogSegmentTornWriteRecovery() {
    fs::path dir = scratchDir();
    fs::path file = dir / "00000000000000000000.log";

    {
        LogSegment seg(file, 0);
        seg.open();
        seg.recover();

        Record a;
        a.offset = 0;
        a.timestampMs = 1;
        a.key = std::string("good-key");
        a.value = std::string("good-value");
        seg.append(a);

        Record b;
        b.offset = 1;
        b.timestampMs = 2;
        b.key = std::string("second-key");
        b.value = std::string("second-value");
        seg.append(b);
    }  // segment closed here (file handle released)

    size_t validSize = fs::file_size(file);

    // Simulate a crash mid-write: append a truncated/garbage tail directly
    // to the file, as if a third record's write was interrupted partway.
    {
        std::FILE* raw = std::fopen(file.string().c_str(), "ab");
        assert(raw != nullptr);
        const uint8_t garbage[] = {0x00, 0x00, 0x00, 0x50, 0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x02};
        std::fwrite(garbage, 1, sizeof(garbage), raw);
        std::fclose(raw);
    }

    size_t tornSize = fs::file_size(file);
    assert(tornSize > validSize);

    // Recovery must detect the torn tail, truncate back to validSize, and
    // still correctly report the two good records.
    LogSegment seg2(file, 0);
    seg2.open();
    LogSegment::RecoveryResult result = seg2.recover();

    assert(result.lastOffset == 1);
    assert(result.validBytes == validSize);
    assert(fs::file_size(file) == validSize);
    assert(result.index.size() == 2);

    auto posOpt = seg2.findFilePos(0);
    assert(posOpt.has_value());
    auto rec = seg2.readAt(*posOpt);
    assert(rec.has_value());
    assert(rec->key == "good-key");

    std::cout << "[PASS] LogSegment torn-write recovery truncates cleanly\n";
}

static void testPartitionLogAppendFetch() {
    fs::path dir = scratchDir();

    PartitionLog log(dir);
    log.open();

    for (int i = 0; i < 5; ++i) {
        int64_t offset = log.append("key" + std::to_string(i), "value" + std::to_string(i));
        assert(offset == i);
    }
    assert(log.nextOffset() == 5);

    std::vector<Record> records = log.fetch(0, 10);
    assert(records.size() == 5);
    for (int i = 0; i < 5; ++i) {
        assert(records[i].offset == i);
        assert(records[i].key == "key" + std::to_string(i));
        assert(records[i].value == "value" + std::to_string(i));
    }

    std::vector<Record> partial = log.fetch(2, 2);
    assert(partial.size() == 2);
    assert(partial[0].offset == 2);
    assert(partial[1].offset == 3);

    std::vector<Record> caughtUp = log.fetch(5, 10);
    assert(caughtUp.empty());

    std::cout << "[PASS] PartitionLog append/fetch\n";
}

static void testPartitionLogRollingAndReopen() {
    fs::path dir = scratchDir();

    // Tiny segment size so a handful of records force multiple rolls.
    {
        PartitionLog log(dir, /*segmentMaxBytes=*/64);
        log.open();
        for (int i = 0; i < 10; ++i) {
            log.append("k" + std::to_string(i), "some-longer-value-" + std::to_string(i));
        }
        assert(log.nextOffset() == 10);
    }  // log goes out of scope, all segment files closed

    int segmentFileCount = 0;
    for (const auto& entry : fs::directory_iterator(dir)) {
        if (entry.path().extension() == ".log") ++segmentFileCount;
    }
    assert(segmentFileCount > 1 && "expected segment rolling to produce multiple files");

    // Reopen (simulating a broker restart) and verify state and data survive.
    PartitionLog reopened(dir, 64);
    reopened.open();
    assert(reopened.nextOffset() == 10);

    std::vector<Record> all = reopened.fetch(0, 100);
    assert(all.size() == 10);
    for (int i = 0; i < 10; ++i) {
        assert(all[i].offset == i);
        assert(all[i].key == "k" + std::to_string(i));
    }

    // Appending after reopen must continue the offset sequence, not restart it.
    int64_t next = reopened.append("k10", "v10");
    assert(next == 10);

    std::cout << "[PASS] PartitionLog segment rolling + reopen after restart\n";
}

static void testPartitionLogAppendReplicated() {
    fs::path dir = scratchDir();
    PartitionLog log(dir);
    log.open();

    Record r0;
    r0.offset = 0;
    r0.timestampMs = 123456789;  // deliberately NOT "now" - must be preserved verbatim
    r0.key = std::string("leader-key");
    r0.value = std::string("leader-value");
    bool ok = log.appendReplicated(r0);
    assert(ok);
    assert(log.nextOffset() == 1);

    // Duplicate (same offset again) must be rejected, no state change.
    bool dup = log.appendReplicated(r0);
    assert(!dup);
    assert(log.nextOffset() == 1);

    // A gap (skipping offset 1, jumping to 2) must be rejected.
    Record r2;
    r2.offset = 2;
    r2.timestampMs = 999;
    r2.key = std::string("k2");
    r2.value = std::string("v2");
    bool gap = log.appendReplicated(r2);
    assert(!gap);
    assert(log.nextOffset() == 1);

    // The contiguous next offset succeeds.
    Record r1;
    r1.offset = 1;
    r1.timestampMs = 987654321;
    r1.key = std::string("k1");
    r1.value = std::string("v1");
    ok = log.appendReplicated(r1);
    assert(ok);
    assert(log.nextOffset() == 2);

    // Fetch back and verify the timestamp was preserved exactly, not
    // regenerated from wall-clock time (the bug this method exists to avoid).
    std::vector<Record> fetched = log.fetch(0, 10);
    assert(fetched.size() == 2);
    assert(fetched[0].timestampMs == 123456789);
    assert(fetched[0].key == "leader-key");
    assert(fetched[1].timestampMs == 987654321);
    assert(fetched[1].key == "k1");

    std::cout << "[PASS] PartitionLog::appendReplicated rejects gaps/duplicates, preserves timestamp\n";
}

int main() {
    testRecordRoundTrip();
    testLogSegmentAppendRead();
    testLogSegmentTornWriteRecovery();
    testPartitionLogAppendFetch();
    testPartitionLogRollingAndReopen();
    testPartitionLogAppendReplicated();
    std::cout << "All Milestone 2 tests passed.\n";
    return 0;
}
