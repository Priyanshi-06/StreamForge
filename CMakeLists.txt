cmake_minimum_required(VERSION 3.16)
project(MiniKafka CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Debug)
endif()

find_package(Threads REQUIRED)

# --- common: byteorder helpers, CRC32, cross-platform socket wrapper ---
add_library(minikafka_common STATIC
    src/common/net/socket.cpp
    src/common/util/crc32.cpp
    src/common/util/byte_buffer.cpp
)
target_include_directories(minikafka_common PUBLIC src)
target_link_libraries(minikafka_common PUBLIC Threads::Threads)

if(WIN32)
    target_link_libraries(minikafka_common PUBLIC ws2_32)
endif()

# --- storage: Record encode/decode, LogSegment, PartitionLog ---
add_library(minikafka_storage STATIC
    src/storage/record.cpp
    src/storage/log_segment.cpp
    src/storage/partition_log.cpp
)
target_include_directories(minikafka_storage PUBLIC src)
target_link_libraries(minikafka_storage PUBLIC minikafka_common)

# --- protocol: frame read/write, request/response message encode/decode ---
add_library(minikafka_protocol STATIC
    src/protocol/protocol.cpp
    src/protocol/messages.cpp
)
target_include_directories(minikafka_protocol PUBLIC src)
target_link_libraries(minikafka_protocol PUBLIC minikafka_common minikafka_storage)

# --- broker_core: Topic/TopicRegistry, consumer offsets, TCP server ---
add_library(minikafka_broker_core STATIC
    src/broker/topic.cpp
    src/broker/topic_registry.cpp
    src/broker/consumer_offsets.cpp
    src/broker/group_coordinator.cpp
    src/broker/activity_log.cpp
    src/broker/cluster_config.cpp
    src/broker/replica_assignment.cpp
    src/broker/replication_fetcher.cpp
    src/broker/replication_manager.cpp
    src/broker/broker_server.cpp
)
target_include_directories(minikafka_broker_core PUBLIC src)
target_link_libraries(minikafka_broker_core PUBLIC minikafka_common minikafka_storage minikafka_protocol)

add_executable(minikafka-broker src/broker/main_broker.cpp)
target_link_libraries(minikafka-broker PRIVATE minikafka_broker_core)

add_executable(minikafka-produce src/producer/main_produce.cpp)
target_link_libraries(minikafka-produce PRIVATE minikafka_protocol)

add_executable(minikafka-consume src/consumer/main_consume.cpp)
target_link_libraries(minikafka-consume PRIVATE minikafka_protocol)

enable_testing()
add_subdirectory(tests)
