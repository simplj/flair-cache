# FLAIR Cache

**F**ast **L**ocal **A**ccess with **I**n-memory **R**eplication

> Distributed. Embedded. Zero infrastructure. Every node. Full data. Zero network.

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Java](https://img.shields.io/badge/Java-11%2B-orange.svg)](https://openjdk.org/)
[![Status](https://img.shields.io/badge/Status-In%20Development-yellow.svg)]()

---

## What is FLAIR?

FLAIR is an embeddable, zero-infrastructure distributed in-memory cache library for Java microservices.

Most caches make you choose between **distributed** (Redis, Hazelcast) or **fast** (Caffeine, local HashMap). FLAIR gives you both — data is replicated across every node in your cluster in real time, but every read is a pure in-JVM lookup. No network. No serialization. No external process.

```
Service A          Service B          Service C
┌─────────────┐   ┌─────────────┐   ┌─────────────┐
│  Your App   │   │  Your App   │   │  Your App   │
├─────────────┤   ├─────────────┤   ├─────────────┤
│ FLAIR Cache │◄──► FLAIR Cache │◄──► FLAIR Cache │
│  (in-JVM)   │   │  (in-JVM)   │   │  (in-JVM)   │
└─────────────┘   └─────────────┘   └─────────────┘

  put("k", v)  ──────────────────────────────────►  replicated to all nodes
  get("k")     →  HashMap.get()  ←  always local, always sub-microsecond
```

---

## Why FLAIR?

| | Redis | Hazelcast | Caffeine | **FLAIR** |
|---|---|---|---|---|
| Read latency | Network + deser | Network + deser | Sub-µs (local only) | **Sub-µs (distributed)** |
| External process required | ✅ Yes | ✅ Yes | ❌ No | **❌ No** |
| Java-native object storage | ❌ No | ⚠️ Partial | ✅ Yes | **✅ Yes** |
| Zero dependencies | ❌ No | ❌ No | ❌ No | **✅ Yes — JDK only** |
| Query DSL (join, aggregate) | ❌ No | ⚠️ Limited | ❌ No | **✅ Yes** |
| Setup effort | High | High | Low | **Zero** |

---

## Core Principles

**1. Reads are always local**
Every `get()` is a `ConcurrentHashMap` lookup inside your JVM. No I/O. No network hop. No deserialization. Sub-microsecond, always.

**2. Writes replicate everywhere**
Every `put()` propagates to all peer nodes over persistent TCP connections in real time. A value written on Service A is available on Service B within milliseconds — from its own local memory.

**3. Zero infrastructure**
No Redis server. No Zookeeper. No message broker. Embed the JAR, add one annotation, and your cluster forms itself using gossip-based peer discovery.

**4. JDK only**
FLAIR has zero external dependencies. Everything — TCP transport, binary serialization, peer discovery, conflict resolution — is built from the ground up using pure JDK APIs. No vulnerability surface from transitive dependencies. Ever.

---

## Features

### Core
- ✅ Embedded in-JVM distributed cache — no external process
- ✅ Sub-microsecond local reads via `ConcurrentHashMap` + `StampedLock`
- ✅ Real-time replication over persistent native TCP sockets (Java NIO)
- ✅ Gossip-based peer discovery and failure detection (SWIM protocol, UDP)
- ✅ Automatic new-node bootstrap — full state sync on join
- ✅ TTL-based expiry with propagated deletes
- ✅ Pluggable eviction: LRU, LFU, SIZE\_BASED
- ✅ Consistency modes per cache block: EVENTUAL, QUORUM, STRONG
- ✅ Hybrid Logical Clock (HLC) for Last-Write-Wins conflict resolution
- ✅ TLS with mutual authentication (`javax.net.ssl` — no Bouncy Castle)

### Query DSL *(FLAIR's differentiator)*
- ✅ Filter entries with predicate chains (`where().and().or()`)
- ✅ Hash-join across two cache blocks — zero network, zero I/O
- ✅ Group by + aggregate (sum, avg, min, max, count)
- ✅ Sorting, pagination (`orderBy().limit().offset()`)
- ✅ Parallel query execution via `ForkJoinPool`

### Reactivity
- ✅ Subscribe to PUT / DELETE / EXPIRE events per cache block
- ✅ Key-level filtering before deserialization
- ✅ Async dispatch — slow listeners never block the write path

### Observability
- ✅ JMX metrics — hit rate, replication lag, cluster health, eviction counts

---

## Modules

FLAIR is built as composable standalone modules. Each module is independently useful and shippable.

| Module | Artifact | Standalone purpose |
|---|---|---|
| [Serial](flair-cache-serial/README.md) | `flair-cache-serial` | Binary serialization — POJOs ↔ `ByteBuffer`, zero deps |
| [Transport](flair-cache-transport/README.md) | `flair-cache-transport` | Non-blocking NIO TCP server + client |
| [Gossip](flair-cache-gossip/README.md) | `flair-cache-gossip` | SWIM peer discovery + failure detection (UDP) |
| [HLC](flair-cache-hlc/README.md) | `flair-cache-hlc` | Hybrid Logical Clock — causal timestamps for distributed systems |
| [Store](flair-cache-store/README.md) | `flair-cache-store` | Local in-memory cache — TTL, LRU/LFU eviction |
| [Replication](flair-cache-replication/README.md) | `flair-cache-replication` | TCP fanout, ACK tracking, consistency modes |
| [Bootstrap](flair-cache-bootstrap/README.md) | `flair-cache-bootstrap` | State sync for new node join |
| [DSL](flair-cache-dsl/README.md) | `flair-cache-dsl` | Query DSL — filter, join, aggregate over cache blocks |
| Watch | `flair-cache-watch` | Reactivity — subscribe to cache change events |
| Metrics | `flair-cache-metrics` | JMX metrics and monitoring |

---

## Quick Start

> ⚠️ **FLAIR is currently under active development.** Installation instructions and API examples will be published upon the first stable release. See [Project Status](#project-status).

### Planned usage (any Java app)

```java
// Non-Spring — direct instantiation
FlairCache cache = FlairCache.builder()
    .bindPort(7890)
    .seedPeers(List.of("10.0.0.1:7890", "10.0.0.2:7890"))
    .consistency(ConsistencyMode.QUORUM)
    .build()
    .start();
```

```java
// Spring Boot — register as a @Bean, no special module needed
@Configuration
public class FlairCacheConfig {
    @Bean
    public FlairCache flairCache() {
        return FlairCache.builder()
            .bindPort(7890)
            .seedPeers(List.of("10.0.0.1:7890"))
            .consistency(ConsistencyMode.QUORUM)
            .build()
            .start();
    }
    @Bean
    public SmartLifecycle flairCacheLifecycle(FlairCache cache) {
        return new SmartLifecycle() {
            public void start()        { /* already started */ }
            public void stop()         { cache.shutdown(); }
            public boolean isRunning() { return cache.isRunning(); }
        };
    }
}
```

```java
// Define a typed cache block
@Autowired FlairCache cache;

CacheBlock<String, Product> products = cache.block("products")
    .ttl(Duration.ofMinutes(30))
    .eviction(EvictionPolicy.LRU)
    .maxEntries(100_000)
    .build();

// 4. Write on any node — replicates everywhere
products.put("p1", product);

// 5. Read on any node — always local, always sub-microsecond
Product p = products.get("p1");
```

### Planned DSL usage

```java
// Filter within a block
List<Product> results = cache.query()
    .from("products", Product.class, productCodec)
    .where(p -> p.getCategory().equals("electronics"))
    .and(p -> p.getPrice() < 1000)
    .orderBy(Product::getPrice)
    .limit(20)
    .fetch();

// Join across two cache blocks — zero network
List<OrderSummary> summaries = cache.query()
    .from("orders", Order.class, orderCodec)
    .join("customers", Customer.class, customerCodec)
    .on((o, c) -> o.getCustomerId().equals(c.getId()))
    .where(o -> o.getTotal() > 500)
    .select((o, c) -> new OrderSummary(o, c.getName()))
    .fetch();

// Aggregate
Map<String, Double> avgPriceByCategory = cache.query()
    .from("products", Product.class, productCodec)
    .groupBy(Product::getCategory)
    .aggregate(Aggregators.avg(Product::getPrice));
```

---

## Architecture

```
┌──────────────────────────────────────────────────────┐
│                   Your Application                   │
├──────────────────────────────────────────────────────┤
│              FlairCache Public API                   │
│       CacheBlock<K,V>  ·  QueryDSL  ·  WatchAPI      │
├──────────────────┬───────────────────────────────────┤
│   Local Store    │       Replication Engine          │
│                  │  Fanout → PerPeerWriteQueue       │
│ ConcurrentHashMap│  NIO SocketChannel (TCP)          │
│ + StampedLock    │  FrameEncoder / FrameDecoder      │
│ + TTL Index      │  HLC Conflict Resolution (LWW)    │
├──────────────────┴───────────────────────────────────┤
│                   Cluster Layer                      │
│    SWIM Gossip (UDP)  ·  MembershipList  ·  Bootstrap│
├──────────────────────────────────────────────────────┤
│                Infrastructure Layer                  │
│  HybridLogicalClock · ThreadFactory · JMX · TLS      │
└──────────────────────────────────────────────────────┘
```

### How replication works

1. Your app calls `products.put("p1", product)` on Service A
2. FLAIR writes to Service A's local `ConcurrentHashMap` instantly
3. The write is encoded into a binary frame (custom protocol, big-endian)
4. The frame is enqueued to each live peer's TCP write queue (non-blocking)
5. Dedicated per-peer writer threads drain the queue and flush over persistent TCP connections
6. Peer nodes decode the frame, apply HLC-based conflict resolution, and write to their local store
7. Service B calls `products.get("p1")` — pure `HashMap.get()`, zero network

### How nodes discover each other

FLAIR uses the **SWIM gossip protocol** over UDP. Nodes periodically ping random peers,
piggyback membership updates onto every message, and use indirect probing to distinguish
slow nodes from dead ones — all without a central coordinator.

---

## Performance Targets

*Benchmarks will be published with the first release. These are design targets verified during development.*

| Operation | Target (p99) |
|---|---|
| `get()` local hit | < 200ns |
| `put()` + async replication enqueue | < 500ns |
| DSL `where().fetch()` (10k entries) | < 5ms |
| DSL `join()` (10k × 10k entries) | < 50ms |
| Frame encode/decode (1KB payload) | < 1µs |
| End-to-end replication (EVENTUAL, LAN) | < 5ms |

---

## Project Status

FLAIR is currently in active development. The library is being built module by module in the following order:

- [x] Architecture design & module specification
- [x] [`flair-cache-serial`](flair-cache-serial/README.md) — Binary serialization
- [x] [`flair-cache-hlc`](flair-cache-hlc/README.md) — Hybrid Logical Clock
- [x] [`flair-cache-transport`](flair-cache-transport/README.md) — NIO TCP transport
- [x] [`flair-cache-gossip`](flair-cache-gossip/README.md) — SWIM gossip
- [x] [`flair-cache-store`](flair-cache-store/README.md) — Local store
- [x] [`flair-cache-replication`](flair-cache-replication/README.md) — Replication engine
- [x] [`flair-cache-bootstrap`](flair-cache-bootstrap/README.md) — Bootstrap sync
- [x] [`flair-cache-dsl`](flair-cache-dsl/README.md) — Query DSL
- [ ] `flair-cache-watch` — Watch / reactivity
- [ ] `flair-cache-metrics` — JMX metrics
- [ ] First stable release on Maven Central

Watch / star the repository to be notified of the first release.

---

## Known Limitations

These are known V1 limitations. Each is a tracked enhancement for V2.

**Delete conflict resolution is unconditional**
`DELETE` operations do not participate in HLC-based Last-Write-Wins ordering. A `DELETE` always wins when it is applied, regardless of whether a concurrent `PUT` carries a higher HLC timestamp. Applications that issue concurrent writes and deletes to the same key may see unexpected data loss under network partition or message reordering. Tombstone-based LWW for deletes is planned for V2.

**Replication queue capacity is fixed**
The per-node replication queue is fixed at 65,536 frames and is not tunable via configuration in V1. Under sustained high-burst write workloads that exhaust this limit:
- `EVENTUAL` mode: the frame is dropped and a `WARNING` is logged — no exception.
- `QUORUM` / `STRONG` mode: the caller immediately receives a `ReplicationTimeoutException`.

Configurable queue capacity via `FlairCacheConfig` is planned for V2.

**No persistence**
FLAIR is a pure in-memory store. A node that restarts loses its local data and must receive a full state transfer from a live peer via the bootstrap sync process before serving consistent reads. Snapshot-to-disk and point-in-time recovery are planned for V2.

**DSL `groupBy()` always executes sequentially**
When `.parallel()` is chained before `.groupBy()`, the parallel hint is ignored — grouping
always executes sequentially in V1. The `.parallel()` flag is still respected for `.fetch()`,
`.count()`, `findFirst()`, and all other non-groupBy terminals. Parallel groupBy is planned
for V2.

**`Decoder.identity()` does not perform type checking**
`Decoder.identity()` performs no runtime type verification. If a source map contains values of
unexpected types, the error surfaces as a `ClassCastException` at the point of use rather than
at the point of decoding — making the root cause hard to trace. Use `Decoder.typed(Class<T>)`
when the source map may contain mixed value types. A convenience shorthand that defaults to safe
type-checked decoding is planned for V2.

**DSL join is limited to two sources**
A single query chain can join exactly two named sources. Joining three or more sources requires
running multiple queries in sequence and feeding intermediate results into the next query manually.
Multi-source join in a single query chain is planned for V2.

**DSL join returns only matched rows (inner join)**
The DSL join always behaves like a SQL `INNER JOIN` — rows with no match on the other side are
dropped. Left join (retain all left-side rows), right join, and full outer join are not supported
in V1. These join types are planned for V2.

**Bootstrap has no concurrency limit for simultaneous joins**
When multiple nodes join the cluster at the same time, the donor spawns one dedicated `flaircache-bootstrap-sync` thread per joiner with no upper bound. Each thread holds a full point-in-time snapshot in memory for the duration of the transfer. Joining 20+ nodes simultaneously will create 20+ threads and proportionally high heap pressure on the donor. Rolling joins — bringing nodes up one or a few at a time — are strongly recommended in V1. A semaphore-backed sync pool with a configurable concurrency cap is planned for V2.

**Asymmetric connectivity causes missed writes**
FLAIR uses a push-from-origin replication model: the node that writes a value fans it out directly to all live TCP peers. If the writing node (Node A) cannot reach a peer (Node C) over TCP, but other nodes (Node B) can reach Node C, the write is silently skipped for Node C. SWIM gossip correctly keeps Node C as `ALIVE` via indirect probing — the replication layer and the membership layer disagree. The write is lost for Node C until the TCP connection between A and C recovers. A relay replication mechanism is planned for V2.

---

## V2 Roadmap

| Enhancement | Description |
|---|---|
| Tombstone deletes | DELETE participates in HLC LWW — concurrent deletes and writes resolve correctly |
| Configurable queue capacity | Replication queue size tunable via `FlairCacheConfig` |
| Bounded bootstrap concurrency | Cap concurrent join syncs on the donor via a configurable semaphore-backed pool; shed excess with a clear error |
| Snapshot / recovery | Persist state to disk; restore on restart without full peer sync |
| CRDT merge strategies | Pluggable conflict resolution beyond LWW (G-Counter, PN-Counter, OR-Set) |
| Derived / computed blocks | Read-only blocks whose values are functions of other blocks |
| DSL index engine | Accelerated queries with in-memory secondary indexes |
| Reactive streaming | `Flow.Publisher` integration for reactive consumers |
| Write-through / write-behind | Propagate writes to an external store (database, file) |
| Topology-aware replication | Rack / zone awareness to reduce cross-AZ replication traffic |
| Per-block ACLs | Fine-grained read/write access control per cache block |
| **Relay replication** | Route writes through intermediary nodes when direct TCP to a peer is unavailable — eliminates missed writes under asymmetric connectivity |
| **DSL parallel groupBy** | `.parallel().groupBy().aggregate()` executes on the dedicated DSL worker pool instead of sequentially |
| **DSL auto-typed decoder** | Convenience shorthand for `QueryEngine.from()` that defaults to safe type-checked decoding without requiring the class token twice |
| **DSL multi-source join** | Join three or more named sources in a single fluent query chain |
| **DSL left/right/full outer join** | Left, right, and full outer join support — retain unmatched rows from either side, consistent with SQL semantics |

---

## Best Fit Use Cases

FLAIR is designed for specific scenarios where it genuinely excels:

**Perfect fit**
- Java microservices sharing reference data (product catalogs, pricing, config, feature flags, routing rules)
- Read-heavy workloads where data changes occasionally but is queried millions of times
- Teams wanting to eliminate Redis/Memcached as an infrastructure dependency
- Spring Boot microservice architectures

**Not the right tool**
- Write-heavy, high-churn data (thousands of mutations per second per key)
- Polyglot environments (non-Java services need the same data)
- Primary database replacement for mutable user data requiring ACID guarantees
- Datasets larger than available JVM heap across all nodes

---

## Design Decisions

**Why no external dependencies?**
Every external dependency is a potential CVE, a version conflict, and an operational burden. FLAIR is built entirely on JDK APIs — Java NIO for TCP, `javax.net.ssl` for TLS, `java.util.logging` for logging, and JMX for metrics. The result is a single JAR with zero transitive dependencies and zero external vulnerability surface.

**Why TCP for replication and UDP for discovery?**
Replication requires reliable, ordered delivery — TCP is the right primitive. Peer discovery and failure detection use the SWIM protocol, which is specifically designed around UDP's properties: low overhead, tolerance for packet loss, and natural probabilistic spread. Using the right transport for each concern rather than forcing one to do both jobs.

**Why Hybrid Logical Clocks?**
Wall clocks lie in distributed systems — NTP drift, clock skew, and leap seconds make them unreliable for ordering events. HLC combines physical time (for human readability and rough ordering) with a logical counter (for causality) to give consistent, monotonically increasing timestamps that never go backward even when clocks drift.

**Why a custom binary protocol?**
JSON adds parsing overhead and type ambiguity. Java native serialization is a security liability and a compatibility nightmare. A hand-rolled binary protocol with a defined frame format gives full control over the wire representation, enables zero-copy reads in the hot path, and makes the protocol version-stable from day one.

---

## License

FLAIR Cache is licensed under the **Apache License, Version 2.0**.

You may use it freely in commercial and open-source projects. See [LICENSE](LICENSE) for the full text.

```
Copyright 2024 simplj.com

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
```

---

## About

Built by [simplj.com](https://simplj.com) — *Simple Java, done with FLAIR.*

FLAIR stands for **F**ast **L**ocal **A**ccess with **I**n-memory **R**eplication.