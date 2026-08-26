# StreamForge

A hand-rolled, multi-broker distributed message broker in C++17 — built from scratch to learn the real mechanics behind systems like Apache Kafka: log-structured storage with crash recovery, a custom binary wire protocol, partitioned topics, consumer-group coordination, and leader/follower replication across multiple broker processes.

No external dependencies. No framework. Every layer — sockets, storage, protocol, replication — is implemented directly on top of the C++ standard library and raw TCP.

This is a terminal/CLI project: a broker process (`minikafka-broker`) and two client tools (`minikafka-produce`, `minikafka-consume`). All output below is real, captured from actual runs.

## Build

Requires a C++17 compiler and CMake (tested with GCC 13+ via MSYS2 on Windows; should build on any platform with a POSIX or Winsock2 socket API).

```bash
cmake -S . -B build -G Ninja
cmake --build build
```

This produces three executables in `build/`: `minikafka-broker`, `minikafka-produce`, `minikafka-consume`.

Run the test suite:

```bash
ctest --test-dir build
```

## Quick start

**Start a broker:**

```
$ ./minikafka-broker demo_data 9092
Created default topic "orders" with 3 partitions
MiniKafka broker is running.
  Broker id:         0
  Listening on port: 9092
  Storing data in:   "demo_data"
  Waiting for producer/consumer connections... (Ctrl+C to stop)
```

**Produce messages** (keyed messages hash to a partition consistently; unkeyed messages round-robin):

```
$ ./minikafka-produce orders "order-1" "buy 10 widgets"
Message sent -> topic "orders", partition 1, offset 0 (key: "order-1")

$ ./minikafka-produce orders "unkeyed order"
Message sent -> topic "orders", partition 0, offset 0 (no key - broker chose the partition by round robin)
```

**Consume messages** (a consumer group tracks its own progress and resumes where it left off):

```
$ ./minikafka-consume orders 1 my-group
No previous progress found for group "my-group" on topic "orders", partition 1 - starting from the beginning.
Received 1 message(s):
  [offset 0] key: "order-1", value: "buy 10 widgets"
```

## Durability: survives a crash

Every record is fsync'd to disk on write, and on startup the broker scans its log files and truncates any torn/corrupted tail left by a crash mid-write — so a killed broker never loses an acknowledged record, and never serves corrupted data.

```
$ ./minikafka-produce orders "order-1" "buy widgets"
$ ./minikafka-produce orders "order-1" "cancel widgets"

$ taskkill /F broker.exe    (simulating a crash)

$ ./minikafka-broker demo_data 9092    (restart with the same data dir)

$ ./minikafka-consume orders 1 verify-group
No previous progress found for group "verify-group" on topic "orders", partition 1 - starting from the beginning.
Received 2 message(s):
  [offset 0] key: "order-1", value: "buy widgets"
  [offset 1] key: "order-1", value: "cancel widgets"
```

Both records survived the kill, in order, byte-for-byte.

## Multi-broker replication

Start several broker processes with the same `--cluster` list; each independently computes the same deterministic leader/follower assignment per partition, with zero coordination needed. Any broker accepts any client request — if it isn't the leader for the target partition, it transparently forwards the request to the broker that is and relays the response back, so the client never needs to know the topology.

```
$ ./minikafka-broker cluster_b0 9100 --broker-id=0 --cluster=0:127.0.0.1:9100,1:127.0.0.1:9110
Created default topic "orders" with 3 partitions
MiniKafka broker is running.
  Broker id:         0
  Listening on port: 9100
  ...

$ ./minikafka-broker cluster_b1 9110 --broker-id=1 --cluster=0:127.0.0.1:9100,1:127.0.0.1:9110
MiniKafka broker is running.
  Broker id:         1
  Listening on port: 9110
  ...
[Cluster] Learned about topic "orders" via AnnounceTopic
```

Producing to broker 1 for a key whose partition is led by broker 0 — the request is forwarded automatically:

```
$ ./minikafka-produce --port=9110 orders "order-9" "explicit test"
Message sent -> topic "orders", partition 0, offset 0 (key: "order-9")

$ ./minikafka-consume --port=9100 orders 0 cluster-group
No previous progress found for group "cluster-group" on topic "orders", partition 0 - starting from the beginning.
Received 1 message(s):
  [offset 0] key: "order-9", value: "explicit test"
```

The follower brokers replicate by continuously fetching from the leader in the background (reusing the same `Fetch` protocol a normal consumer uses), applying records at the exact offset and timestamp the leader assigned — so a follower's copy is byte-identical to the leader's, not just eventually consistent in spirit.

## What's implemented

- **Storage**: append-only, segmented log files per partition; CRC-checked records; startup recovery that truncates any torn write; fsync on every append
- **Wire protocol**: hand-rolled binary framing over TCP (no HTTP, no serialization library) — `CreateTopic`, `Metadata`, `Produce`, `Fetch`, `CommitOffset`/`FetchOffset`, `JoinGroup`/`Heartbeat`/`LeaveGroup`, and internal `AnnounceTopic`/`Forwarded` for cluster coordination
- **Partitioning**: keyed messages hash consistently to a partition; unkeyed messages round-robin
- **Consumer groups**: lease-based partition ownership so two consumers in the same group never process the same partition concurrently; a crashed consumer's lease expires automatically, freeing it for another consumer
- **Multi-broker replication**: static, deterministic leader/follower assignment per partition; pull-based follower replication; transparent request forwarding to whichever broker is the leader
- **Explicitly not implemented (by design, this phase)**: automatic leader election/failover and synchronous "acks=all" replication — both require a consensus protocol (e.g. Raft), which is a deliberately separate, future phase

## Layout

```
src/
  common/       cross-platform TCP socket wrapper, byte encoding, CRC32, hashing
  storage/      Record encode/decode, LogSegment (append/recover), PartitionLog
  protocol/     wire framing + every request/response message type
  broker/       Topic/TopicRegistry, GroupCoordinator, ClusterConfig, replication, BrokerServer
  producer/     minikafka-produce CLI
  consumer/     minikafka-consume CLI
tests/          one binary per component, plus a multi-broker integration suite
```
