#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

#ifdef _WIN32
#include <winsock2.h>
using socket_handle_t = SOCKET;
#else
using socket_handle_t = int;
#endif

namespace minikafka {

// Thin cross-platform blocking TCP socket wrapper. Move-only.
class Socket {
public:
    Socket();
    ~Socket();

    Socket(const Socket&) = delete;
    Socket& operator=(const Socket&) = delete;
    Socket(Socket&& other) noexcept;
    Socket& operator=(Socket&& other) noexcept;

    // Must be called once before any socket use (WSAStartup on Windows; no-op elsewhere).
    static void globalInit();
    static void globalCleanup();

    // Server side: bind + listen on the given address/port.
    bool listenOn(const std::string& bindAddr, uint16_t port, int backlog = 16);

    // Server side: blocking accept. Returned socket is invalid() on failure.
    Socket accept();

    // Client side: resolve + connect.
    bool connectTo(const std::string& host, uint16_t port);

    // Blocking send/recv of an exact number of bytes.
    // Returns false on any error or peer disconnect before len bytes are transferred.
    bool sendAll(const uint8_t* data, size_t len);
    bool recvAll(uint8_t* buf, size_t len);

    // Blocking recv of up to maxLen bytes (may return fewer, like a plain
    // read() - for variable-length protocols like HTTP where the exact
    // size isn't known up front). Returns false only on error/disconnect
    // with zero bytes received; otherwise outReceived is set (may be 0
    // only on graceful peer close after this returns false).
    bool recvSome(uint8_t* buf, size_t maxLen, size_t& outReceived);

    void close();
    bool valid() const;

private:
    explicit Socket(socket_handle_t fd);

    socket_handle_t fd_;
};

}  // namespace minikafka
