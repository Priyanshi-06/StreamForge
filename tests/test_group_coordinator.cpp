// Tests for GroupCoordinator in isolation: claim/release/expiry semantics,
// independent of the broker's networking layer.

#include <cassert>
#include <iostream>
#include <thread>

#include "broker/group_coordinator.h"

using namespace minikafka;

static void testClaimAndReject() {
    GroupCoordinator coord;

    bool ok = coord.claim("g", "t", 0, "consumer-a");
    assert(ok);
    assert(coord.isOwner("g", "t", 0, "consumer-a"));
    assert(!coord.isOwner("g", "t", 0, "consumer-b"));

    // A different consumer must be rejected while the lease is active.
    bool rejected = coord.claim("g", "t", 0, "consumer-b");
    assert(!rejected);

    // The original owner can re-claim (heartbeat-style refresh).
    ok = coord.claim("g", "t", 0, "consumer-a");
    assert(ok);

    // Different partition/topic/group are independent.
    assert(coord.claim("g", "t", 1, "consumer-b"));
    assert(coord.claim("g", "other-topic", 0, "consumer-b"));
    assert(coord.claim("other-group", "t", 0, "consumer-b"));

    std::cout << "[PASS] GroupCoordinator claim rejects a different consumer while lease is active\n";
}

static void testRelease() {
    GroupCoordinator coord;

    assert(coord.claim("g", "t", 0, "consumer-a"));
    assert(!coord.claim("g", "t", 0, "consumer-b"));

    coord.release("g", "t", 0, "consumer-a");
    assert(!coord.isOwner("g", "t", 0, "consumer-a"));

    // Now consumer-b can claim it immediately.
    assert(coord.claim("g", "t", 0, "consumer-b"));

    // Releasing with the wrong consumerId must be a no-op.
    coord.release("g", "t", 0, "consumer-a");
    assert(coord.isOwner("g", "t", 0, "consumer-b") && "release by a non-owner must not affect the real owner");

    std::cout << "[PASS] GroupCoordinator release frees the partition for the next claimant\n";
}

static void testExpiry() {
    GroupCoordinator coord(std::chrono::milliseconds(100));

    assert(coord.claim("g", "t", 0, "consumer-a"));
    assert(!coord.claim("g", "t", 0, "consumer-b"));

    std::this_thread::sleep_for(std::chrono::milliseconds(150));

    // consumer-a's lease has expired (it stopped heartbeating, e.g. crashed) -
    // consumer-b must now be able to claim it.
    assert(coord.claim("g", "t", 0, "consumer-b"));
    assert(coord.isOwner("g", "t", 0, "consumer-b"));
    assert(!coord.isOwner("g", "t", 0, "consumer-a"));

    std::cout << "[PASS] GroupCoordinator lease expiry frees an abandoned partition\n";
}

int main() {
    testClaimAndReject();
    testRelease();
    testExpiry();
    std::cout << "All GroupCoordinator tests passed.\n";
    return 0;
}
