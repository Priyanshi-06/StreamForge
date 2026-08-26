#include "common/net/socket.h"

#include <cstring>

#ifdef _WIN32
#include <ws2tcpip.h>
#pragma comment(lib, "ws2_32.lib")
namespace {
constexpr socket_handle_t kInvalidHandle = INVALID_SOCKET;
}
#else
#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
namespace {
constexpr socket_handle_t kInvalidHandle = -1;
}
#endif

namespace minikafka {

void Socket::globalInit() {
#ifdef _WIN32
    WSADATA wsaData;
    WSAStartup(MAKEWORD(2, 2), &wsaData);
#endif
}

void Socket::globalCleanup() {
#ifdef _WIN32
    WSACleanup();
#endif
}

Socket::Socket() : fd_(kInvalidHandle) {}

Socket::Socket(socket_handle_t fd) : fd_(fd) {}

Socket::~Socket() { close(); }

Socket::Socket(Socket&& other) noexcept : fd_(other.fd_) { other.fd_ = kInvalidHandle; }

Socket& Socket::operator=(Socket&& other) noexcept {
    if (this != &other) {
        close();
        fd_ = other.fd_;
        other.fd_ = kInvalidHandle;
    }
    return *this;
}

void Socket::close() {
    if (fd_ != kInvalidHandle) {
#ifdef _WIN32
        closesocket(fd_);
#else
        ::close(fd_);
#endif
        fd_ = kInvalidHandle;
    }
}

bool Socket::valid() const { return fd_ != kInvalidHandle; }

bool Socket::listenOn(const std::string& bindAddr, uint16_t port, int backlog) {
    struct addrinfo hints{};
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;

    struct addrinfo* res = nullptr;
    const char* node = bindAddr.empty() ? nullptr : bindAddr.c_str();
    if (getaddrinfo(node, std::to_string(port).c_str(), &hints, &res) != 0) {
        return false;
    }

    socket_handle_t fd = kInvalidHandle;
    for (struct addrinfo* p = res; p != nullptr; p = p->ai_next) {
        fd = ::socket(p->ai_family, p->ai_socktype, p->ai_protocol);
        if (fd == kInvalidHandle) continue;

        int yes = 1;
#ifdef _WIN32
        // On Windows, SO_REUSEADDR permits a second process to silently
        // bind a port that's already actively listened on (unlike POSIX,
        // where it only allows fast rebind after TIME_WAIT) - two brokers
        // could then both accept connections and write to the same data
        // directory with no coordination. SO_EXCLUSIVEADDRUSE is
        // Microsoft's documented fix: it makes this bind fail loudly
        // instead of silently double-binding.
        setsockopt(fd, SOL_SOCKET, SO_EXCLUSIVEADDRUSE, reinterpret_cast<const char*>(&yes),
                   sizeof(yes));
#else
        setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, reinterpret_cast<const char*>(&yes), sizeof(yes));
#endif

        if (::bind(fd, p->ai_addr, static_cast<int>(p->ai_addrlen)) == 0) {
            break;
        }
#ifdef _WIN32
        closesocket(fd);
#else
        ::close(fd);
#endif
        fd = kInvalidHandle;
    }
    freeaddrinfo(res);

    if (fd == kInvalidHandle) return false;

    if (::listen(fd, backlog) != 0) {
#ifdef _WIN32
        closesocket(fd);
#else
        ::close(fd);
#endif
        return false;
    }

    close();
    fd_ = fd;
    return true;
}

Socket Socket::accept() {
    socket_handle_t clientFd = ::accept(fd_, nullptr, nullptr);
    if (clientFd == kInvalidHandle) {
        return Socket();
    }
    return Socket(clientFd);
}

bool Socket::connectTo(const std::string& host, uint16_t port) {
    struct addrinfo hints{};
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;

    struct addrinfo* res = nullptr;
    if (getaddrinfo(host.c_str(), std::to_string(port).c_str(), &hints, &res) != 0) {
        return false;
    }

    socket_handle_t fd = kInvalidHandle;
    for (struct addrinfo* p = res; p != nullptr; p = p->ai_next) {
        fd = ::socket(p->ai_family, p->ai_socktype, p->ai_protocol);
        if (fd == kInvalidHandle) continue;

        if (::connect(fd, p->ai_addr, static_cast<int>(p->ai_addrlen)) == 0) {
            break;
        }
#ifdef _WIN32
        closesocket(fd);
#else
        ::close(fd);
#endif
        fd = kInvalidHandle;
    }
    freeaddrinfo(res);

    if (fd == kInvalidHandle) return false;

    close();
    fd_ = fd;
    return true;
}

bool Socket::sendAll(const uint8_t* data, size_t len) {
    size_t sent = 0;
    while (sent < len) {
#ifdef _WIN32
        int n = ::send(fd_, reinterpret_cast<const char*>(data + sent),
                        static_cast<int>(len - sent), 0);
#else
        ssize_t n = ::send(fd_, data + sent, len - sent, 0);
#endif
        if (n <= 0) return false;
        sent += static_cast<size_t>(n);
    }
    return true;
}

bool Socket::recvSome(uint8_t* buf, size_t maxLen, size_t& outReceived) {
#ifdef _WIN32
    int n = ::recv(fd_, reinterpret_cast<char*>(buf), static_cast<int>(maxLen), 0);
#else
    ssize_t n = ::recv(fd_, buf, maxLen, 0);
#endif
    if (n <= 0) {
        outReceived = 0;
        return false;
    }
    outReceived = static_cast<size_t>(n);
    return true;
}

bool Socket::recvAll(uint8_t* buf, size_t len) {
    size_t received = 0;
    while (received < len) {
#ifdef _WIN32
        int n = ::recv(fd_, reinterpret_cast<char*>(buf + received),
                        static_cast<int>(len - received), 0);
#else
        ssize_t n = ::recv(fd_, buf + received, len - received, 0);
#endif
        if (n <= 0) return false;  // 0 = peer closed, <0 = error
        received += static_cast<size_t>(n);
    }
    return true;
}

}  // namespace minikafka
