# StreamForge

StreamForge is a hand-built, Kafka-inspired distributed message broker written in C++17.

It is designed as a learning project for understanding what happens inside a modern messaging system: how records are written to disk, how partitions route messages, how consumers resume from offsets, and how brokers forward and replicate data across a small cluster.

There are no external messaging frameworks hiding the hard parts. The broker, protocol, storage engine, replication loop, and CLI clients are built directly on top of C++ and TCP sockets.

## What This Project Gives You

- A runnable broker process: `minikafka-broker`
- A producer CLI for writing messages: `minikafka-produce`
- A consumer CLI for reading messages: `minikafka-consume`
- Append-only partition logs with crash recovery
- A custom binary TCP protocol
- Topic partitioning with keyed and unkeyed routing
- Consumer groups with offset commits
- Multi-broker leader/follower replication
- Transparent request forwarding when a client talks to a non-leader broker

Think of StreamForge as a compact version of the core ideas behind Kafka, built small enough that you can read the code and follow the whole system.

---

# How StreamForge Works

At a high level, producers write records to topics, topics are split into partitions, and consumers read records from partitions while saving their progress.

```text
Producer
   |
   | Produce(topic, key, value)
   v
Broker
   |
   | choose partition
   v
Topic: orders
   |
   +-- Partition 0 -> append-only log on disk
   +-- Partition 1 -> append-only log on disk
   +-- Partition 2 -> append-only log on disk
   |
   v
Consumer Group
   |
   | Fetch records and commit offsets
   v
Consumer
```

The broker assigns every produced message an offset. That offset becomes the message's permanent position inside a partition log.

---

# Core Concepts

## Topics and Partitions

A topic is a named stream of records. Each topic is split into partitions so data can be distributed and consumed independently.

Example:

```text
Topic: orders

Partition 0: offset 0, offset 1, offset 2
Partition 1: offset 0, offset 1
Partition 2: offset 0
```

Offsets are local to a partition. `orders` partition `0` offset `5` is a different record from `orders` partition `1` offset `5`.

## Keyed Messages

If a message has a key, StreamForge hashes the key and always sends that key to the same partition.

```text
key = order-123
partition = hash(order-123) % number_of_partitions
```

This is useful when related messages must stay ordered. For example, all events for `order-123` can land in the same partition.

## Unkeyed Messages

If a message has no key, the broker uses round-robin partition assignment.

```text
Message 1 -> Partition 0
Message 2 -> Partition 1
Message 3 -> Partition 2
Message 4 -> Partition 0
```

This spreads traffic across partitions when ordering by key is not needed.

## Consumer Groups

A consumer group tracks progress for a topic partition. StreamForge stores the committed offset, so a consumer can stop and later resume from where it left off.

```text
Consumer Group: payment-service

Partition 0 -> Consumer A
Partition 1 -> Consumer B
Partition 2 -> Consumer C
```

Inside the same group, StreamForge prevents two active consumers from owning the same partition at the same time. If a consumer disappears, its lease expires and another consumer can take over.

## Replication and Forwarding

In multi-broker mode, each partition has one leader broker and one or more follower brokers.

```text
Client
  |
  | produce to any broker
  v
Broker 1
  |
  | if Broker 1 is not the partition leader,
  | forward the request
  v
Broker 0
  |
  | append to leader log
  v
Follower brokers fetch and copy records
```

Clients do not need to know which broker leads each partition. If they connect to the wrong broker, StreamForge forwards the request to the leader and returns the leader's response.

Follower brokers replicate by fetching records from leaders and applying them at the same offsets and timestamps.

---

# Features

## Storage Layer

- Append-only log storage
- Segmented partition logs
- CRC-checked records
- Crash recovery
- Corrupted tail truncation
- Data persistence using `fsync`

## Network Protocol

StreamForge uses a custom binary wire protocol over TCP.

Implemented request types:

- `CreateTopic`
- `Metadata`
- `Produce`
- `Fetch`
- `CommitOffset`
- `FetchOffset`
- `JoinGroup`
- `Heartbeat`
- `LeaveGroup`
- `AnnounceTopic`
- `Forwarded`

## Broker Runtime

- Creates a default `orders` topic on startup
- Accepts producer and consumer TCP connections
- Stores records on disk by topic and partition
- Tracks consumer group ownership
- Saves committed offsets
- Reconciles topic metadata across brokers
- Replicates follower partitions in the background

---

# Prerequisites

StreamForge requires:

- C++17 compatible compiler
- CMake 3.20+
- Ninja build system (recommended)

---

# Installation

## Windows

The recommended setup uses MSYS2.

### 1. Install MSYS2

Download:

```
https://www.msys2.org/
```

---

### 2. Open MSYS2 UCRT64 terminal

Install compiler and tools:

```bash
pacman -Syu
```

Restart MSYS2 terminal if required.

Then:

```bash
pacman -S mingw-w64-ucrt-x86_64-gcc \
          mingw-w64-ucrt-x86_64-cmake \
          mingw-w64-ucrt-x86_64-ninja
```

---

### 3. Add compiler to PATH

Add:

```
C:\msys64\ucrt64\bin
```

Verify:

```powershell
g++ --version
cmake --version
ninja --version
```

---

# Linux

Ubuntu/Debian:

```bash
sudo apt update

sudo apt install \
build-essential \
cmake \
ninja-build
```

Verify:

```bash
g++ --version
cmake --version
ninja --version
```

---

# macOS

Using Homebrew:

```bash
brew install cmake ninja gcc
```

---

# Build

Clone repository:

```bash
git clone <repository-url>

cd StreamForge
```

Configure:

```bash
cmake -S . -B build -G Ninja
```

Build:

```bash
cmake --build build
```

Generated executables:

Linux/macOS:

```text
build/
|-- minikafka-broker
|-- minikafka-produce
`-- minikafka-consume
```

Windows:

```text
build/
|-- minikafka-broker.exe
|-- minikafka-produce.exe
`-- minikafka-consume.exe
```

---

# Run Tests

```bash
ctest --test-dir build
```

The test suite covers storage, protocol encoding, broker behavior, consumer-group ownership, replica assignment, and multi-broker forwarding/replication.

---

# Quick Start

This walkthrough starts one broker, produces a message, then consumes it back from the stored partition log.

## 1. Start a Single Broker

Linux/macOS:

```bash
./minikafka-broker demo_data 9092
```

Windows:

```powershell
.\minikafka-broker.exe demo_data 9092
```

Example output:

```text
[LOG]  Created default topic "orders" with 3 partitions

+--------------------------------------------------------+
|                MiniKafka Broker Running                |
+--------------------------------------------------------+
  Broker id   0
  Port        9092
  Data dir    demo_data
  Status      Waiting for producer/consumer connections
+--------------------------------------------------------+
[INFO] Press Ctrl+C to stop.
```

The broker creates an `orders` topic with 3 partitions and waits for clients.

## 2. Produce a Keyed Message

Linux/macOS:

```bash
./minikafka-produce orders order-1 "buy 10 widgets"
```

Windows:

```powershell
.\minikafka-produce.exe orders order-1 "buy 10 widgets"
```

Example output:

```text
+--------------------------------------------------------+
|                    Message Produced                    |
+--------------------------------------------------------+
  Topic       orders
  Partition   1
  Offset      0
  Key         order-1
  Status      SUCCESS
+--------------------------------------------------------+
[INFO] Partition selected from key "order-1".
```

Because the message has a key, the broker chooses the partition by hashing `order-1`.

## 3. Produce an Unkeyed Message

```bash
./minikafka-produce orders "hello world"
```

When no key is supplied, the broker chooses the partition using round robin.

## 4. Consume Messages

```bash
./minikafka-consume orders 1 my-group
```

Example output:

```text
[INFO] No previous progress found for group "my-group" on topic "orders", partition 1 - starting from the beginning.

+--------------------------------------------------------+
|                   Messages Received                    |
+--------------------------------------------------------+
  Topic       orders
  Partition   1
  Group       my-group
  Records     1
+--------------------------------------------------------+
  [offset 0] key="order-1"  value="buy 10 widgets"
```

After records are read, StreamForge commits the next offset for `my-group`. Running the same command again resumes from the committed offset.

To keep polling for new records:

```bash
./minikafka-consume --follow orders 1 my-group
```

---

# Multi-Broker Example

This example starts two brokers in the same cluster. Both brokers receive the same cluster definition, so they agree on which broker leads each partition.

## Start Broker 0

```bash
./minikafka-broker \
cluster_b0 \
9100 \
--broker-id=0 \
--cluster=0:127.0.0.1:9100,1:127.0.0.1:9110
```

## Start Broker 1

```bash
./minikafka-broker \
cluster_b1 \
9110 \
--broker-id=1 \
--cluster=0:127.0.0.1:9100,1:127.0.0.1:9110
```

## Produce Through Broker 1

```bash
./minikafka-produce \
--port=9110 \
orders \
order-9 \
"explicit test"
```

Even if Broker 1 is not the leader for the selected partition, it forwards the request to the leader broker automatically.

## Consume Through Broker 0

```bash
./minikafka-consume \
--port=9100 \
orders \
0 \
cluster-group
```

The client tools only need a host and port. Routing inside the broker cluster is handled by StreamForge.

---

# Crash Recovery

Every acknowledged record is written to disk. On startup, StreamForge scans partition logs and truncates any corrupted tail left by a crash during a write.

Recovery flow:

```text
Produce messages
Kill the broker
Restart with the same data directory
Consume again
```

The valid acknowledged records remain readable after restart.

What the recovery logic protects:

- Complete records are preserved
- Torn or partially written records at the end of a log are removed
- CRC checks prevent corrupted records from being served

---

# Project Structure

```text
src/
|-- common/
|   |-- net/       cross-platform TCP socket wrapper
|   `-- util/      byte encoding, CRC32, hashing, terminal logging
|-- storage/       record format, log segments, partition logs
|-- protocol/      binary framing and request/response messages
|-- broker/        broker server, topics, groups, offsets, replication
|-- producer/      producer CLI
`-- consumer/      consumer CLI

tests/
|-- test_common.cpp
|-- test_storage.cpp
|-- test_protocol.cpp
|-- test_broker_core.cpp
|-- test_broker_server.cpp
|-- test_group_coordinator.cpp
|-- test_replica_assignment.cpp
`-- test_multi_broker.cpp
```

---

# Design Boundaries

## Implemented

- Custom TCP protocol
- Partitioned topics
- Append-only logs
- CRC-based record validation
- Crash recovery
- Consumer groups
- Offset tracking
- Leader/follower replication
- Request forwarding
- Multi-broker topic propagation and reconciliation

## Not Implemented Yet

These are intentionally left for future versions:

- Automatic leader election
- Broker failover
- Consensus protocol
- Synchronous replication acknowledgements

Those features require a consensus layer, such as Raft, and are best treated as a separate project phase.

---

# Why This Project Is Useful

StreamForge is useful if you want to understand distributed systems by building one, not just by using one.

It shows how a broker can:

- Turn a client request into a durable record
- Keep ordered logs per partition
- Resume consumers using committed offsets
- Coordinate exclusive ownership inside a consumer group
- Forward requests to the correct partition leader
- Replicate records from leader to follower brokers

The codebase is intentionally small enough to explore, but complete enough to demonstrate the moving pieces that make log-based messaging systems work.

---

# Future Improvements

Possible future extensions:

- Raft-based consensus
- Dynamic cluster membership
- Better monitoring dashboard
- Authentication
- Compression
- Replication acknowledgements
- Metrics collection

---

