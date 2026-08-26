// Milestone 1 smoke test: byteorder helpers, CRC32, and a loopback socket
// round trip. Hand-rolled assert-based test (no framework dependency).

#include <cassert>
#include <cstring>
#include <iostream>
#include <thread>

#include "common/net/byteorder.h"
#include "common/net/socket.h"
#include "common/util/crc32.h"

using namespace minikafka;

static void testByteorder() {
    uint8_t buf[8];

    putU32BE(buf, 0x01020304u);
    assert(buf[0] == 0x01 && buf[1] == 0x02 && buf[2] == 0x03 && buf[3] == 0x04);
    assert(getU32BE(buf) == 0x01020304u);

    putU64BE(buf, 0x0102030405060708ull);
    assert(buf[0] == 0x01 && buf[7] == 0x08);
    assert(getU64BE(buf) == 0x0102030405060708ull);

    putI32BE(buf, -1);
    assert(buf[0] == 0xFF && buf[1] == 0xFF && buf[2] == 0xFF && buf[3] == 0xFF);
    assert(getI32BE(buf) == -1);

    putI64BE(buf, -1);
    assert(getI64BE(buf) == -1);

    std::cout << "[PASS] byteorder round trips\n";
}

static void testCrc32() {
    // Known-answer test: CRC-32 (IEEE 802.3) of ASCII "123456789" is 0xCBF43926.
    const uint8_t data[] = "123456789";
    uint32_t c = crc32(data, 9);
    assert(c == 0xCBF43926u);
    std::cout << "[PASS] crc32 known-answer test\n";
}

static void testSocketLoopback() {
    Socket::globalInit();

    Socket server;
    bool ok = server.listenOn("127.0.0.1", 18374, 4);
    assert(ok && "server failed to bind/listen");

    std::thread serverThread([&server]() {
        Socket conn = server.accept();
        assert(conn.valid());
        uint8_t buf[5] = {0};
        bool received = conn.recvAll(buf, 5);
        assert(received);
        assert(std::memcmp(buf, "hello", 5) == 0);
        bool sent = conn.sendAll(reinterpret_cast<const uint8_t*>("world"), 5);
        assert(sent);
    });

    Socket client;
    ok = client.connectTo("127.0.0.1", 18374);
    assert(ok && "client failed to connect");

    bool sent = client.sendAll(reinterpret_cast<const uint8_t*>("hello"), 5);
    assert(sent);

    uint8_t reply[5] = {0};
    bool received = client.recvAll(reply, 5);
    assert(received);
    assert(std::memcmp(reply, "world", 5) == 0);

    serverThread.join();
    Socket::globalCleanup();

    std::cout << "[PASS] socket loopback round trip\n";
}

int main() {
    testByteorder();
    testCrc32();
    testSocketLoopback();
    std::cout << "All Milestone 1 tests passed.\n";
    return 0;
}
