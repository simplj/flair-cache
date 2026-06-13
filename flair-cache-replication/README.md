# flair-cache-replication

**Multi-node TCP data propagation with ACK tracking and configurable consistency guarantees.**

Part of the [FLAIR Cache](../README.md) library. This module is the bridge between the local store and the cluster: it fans out every write to all alive peers over persistent TCP connections and—depending on the configured consistency mode—waits for the required number of acknowledgements before returning to the caller.

---

## What it does

- Fans out `PUT` and `DELETE` events to all alive peers over persistent TCP connections
- Three per-block consistency modes: `EVENTUAL`, `QUORUM`, and `STRONG`
- Batches outgoing frames (up to 64 frames or 2 ms, whichever comes first) to amortise syscall overhead
- Tracks in-flight ACKs with a timeout-aware sweep; times out blocked callers cleanly via `ReplicationTimeoutException`
- Applies incoming writes through a pluggable `ConflictResolver`; the built-in resolver uses Hybrid Logical Clock Last-Write-Wins with node-ID tiebreaking
- Advances the local HLC on every incoming frame — including frames whose write loses the conflict — so causal ordering is preserved cluster-wide
- Wires into `flair-cache-transport` and `flair-cache-gossip` without coupling them to each other

**What it does not do:**

- Peer discovery — that is `flair-cache-gossip`'s responsibility
- Bootstrap (full state sync for new nodes joining mid-flight) — that belongs to `flair-cache-bootstrap`
- Persistence — FLAIR is purely in-memory; a restarted node receives state from a live peer

---

## Maven

```xml
<dependency>
    <groupId>com.simplj.flair</groupId>
    <artifactId>flair-cache-replication</artifactId>
    <version>${flair.version}</version>
</dependency>
```

This module brings in `flair-cache-serial`, `flair-cache-transport`, `flair-cache-gossip`, `flair-cache-hlc`, and `flair-cache-store` transitively. No additional dependencies are needed.

---

## Dependencies

| Module | Role in replication |
|---|---|
| `flair-cache-transport` | Provides `TcpServer` and `Connection` — the physical TCP layer |
| `flair-cache-gossip` | Provides `GossipNode` and `MembershipList` — who is alive right now |
| `flair-cache-store` | Provides `CacheBlock` and `CacheEntry` — where incoming writes land |
| `flair-cache-hlc` | Provides `HybridLogicalClock` and `HLCTimestamp` — for LWW conflict resolution |
| `flair-cache-serial` | Provides `Codec<T>` — used by `CacheBlock` for key/value encoding |

---

## How it works

```
Your app
  │
  └─► ReplicationEngine.replicate(event)
        │
        ├─ [EVENTUAL]  enqueue → return immediately
        │
        └─ [QUORUM / STRONG]  register PendingWrite → enqueue → block on CompletableFuture
                                                                        │
ReplicationFanout thread (every 2ms or 64 frames)                       │
  └─► encode frame → send to all alive TCP connections ──────────────►  │
                                                                        │
Remote node                                                             │
  └─► IncomingHandler.onFrame()                                         │
        ├─ advance local HLC                                            │
        ├─ resolve conflict (LWW)                                       │
        ├─ write winner to CacheBlock                                   │
        └─ if needsAck → send ACK frame back ───────────────────────►   │
                                                                        ▼
                                                           AckTracker.onAck()
                                                             └─ if required count reached
                                                                  └─ complete future → caller unblocks
```

---

## Minimal setup

The builder splits into two phases because `FrameHandler` must be wired into `TcpServer` before the engine can be built — the handler is the bridge between the transport layer and the engine.

```java
UUID nodeId = UUID.randomUUID(); // stable across restarts in production

// Phase 1 — create the builder and get the frame handler
ReplicationEngine.Builder eb = ReplicationEngine.builder()
        .localNodeId(nodeId)
        .blockLookup(name -> cacheBlocks.get(name)); // map of your CacheBlock instances

// Wire the handler into the transport — must happen before TcpServer starts
TcpServer server = TcpServer.builder()
        .port(7890)
        .handler(eb.frameHandler())
        .build();

// Phase 2 — supply transport and cluster, then build
GossipNode gossip = GossipNode.builder()
        .nodeId(nodeId)
        .bindPort(7891)
        .seedPeers(List.of("10.0.0.2:7891", "10.0.0.3:7891"))
        .build();

ReplicationEngine engine = eb
        .transport(server)
        .cluster(gossip)
        .build();

// Start in order: transport first, then engine, then gossip
server.start();
engine.start();
gossip.start();
```

> **Order matters.** Call `eb.frameHandler()` before building `TcpServer`. Start `server` before `engine` so the selector thread is running when `engine.start()` registers connections. Start `gossip` last so the node only begins announcing itself after it can accept incoming frames.

---

## Replicating writes

```java
// PUT — replicate a value to all peers
CacheEntry entry = new CacheEntry(
        valueBytes,          // serialised value
        hlcClock.now(),      // timestamp — must be the local HLC, not System.currentTimeMillis()
        expiryEpochMs,       // 0 = no TTL
        System.currentTimeMillis(),
        0L,
        nodeId               // originating node
);
engine.replicate(ReplicationEvent.put("products", keyBytes, entry, ConsistencyMode.QUORUM));

// DELETE — remove a key from all peers
engine.replicate(ReplicationEvent.delete(
        "products", keyBytes, hlcClock.now(), nodeId, ConsistencyMode.EVENTUAL));
```

`replicate()` is the only method that blocks. `EVENTUAL` always returns immediately. `QUORUM` and `STRONG` block the calling thread until the required ACKs arrive or the timeout fires.

---

## Consistency modes

Configure per `replicate()` call — different calls on the same engine can use different modes.

### EVENTUAL

```java
engine.replicate(ReplicationEvent.put("products", key, entry, ConsistencyMode.EVENTUAL));
```

Returns immediately after enqueuing. The write will reach all alive peers, but the caller has no confirmation. Dropped frames (queue full) are logged at `WARNING` and silently skipped — no exception is thrown.

**Best for:** high-throughput writes, metrics, counters, non-critical cache warming.

---

### QUORUM

```java
engine.replicate(ReplicationEvent.put("products", key, entry, ConsistencyMode.QUORUM));
// throws ReplicationTimeoutException if quorum not reached within ackTimeoutMs
```

Blocks until a **strict majority of all nodes** (self + alive peers) have applied the write.

**Required peer ACKs = `floor((alivePeers + 1) / 2)`**

| Cluster size | Alive peers | Peer ACKs required | Total writes (self included) |
|---|---|---|---|
| 2 | 1 | 1 | 2 of 2 |
| 3 | 2 | 1 | 2 of 3 |
| 4 | 3 | 2 | 3 of 4 |
| 5 | 4 | 2 | 3 of 5 |
| 51 | 50 | 25 | 26 of 51 |

**Best for:** writes that must survive a single node failure without data loss.

---

### STRONG

```java
engine.replicate(ReplicationEvent.put("products", key, entry, ConsistencyMode.STRONG));
// throws ReplicationTimeoutException if any peer doesn't ACK within ackTimeoutMs
```

Blocks until **every alive peer** has applied the write. More durable than QUORUM but more exposed to tail latency — a single slow or partitioned peer triggers a timeout.

**Required peer ACKs = `alivePeers`** (all of them)

**Best for:** writes that must be immediately readable from any node without staleness risk, where cluster size is small and network is reliable.

---

### Timeout behaviour

When `QUORUM` or `STRONG` times out, `replicate()` throws `ReplicationTimeoutException`. The local write has already been applied. Peers that ACKed before the timeout also have the write. Peers that did not ACK may or may not have it — the write was enqueued and may arrive late.

```java
try {
    engine.replicate(event);
} catch (ReplicationTimeoutException e) {
    // local write committed; remote convergence is not guaranteed
    metrics.increment("replication.timeout");
}
```

---

## Incoming event callback

Register a listener to be notified after each incoming replication frame is applied to the local store. Intended for the watch/reactivity layer and for metrics.

```java
engine.onIncoming(event -> {
    if (event instanceof ReplicationEvent.PutEvent put) {
        System.out.println("received PUT: block=" + put.blockName());
    } else if (event instanceof ReplicationEvent.DeleteEvent del) {
        System.out.println("received DELETE: block=" + del.blockName());
    }
    // event.mode() is always EVENTUAL for incoming frames —
    // it signals "applied from wire", not a local consistency request
});
```

The callback is called on the transport worker thread. It must not block.

---

## Conflict resolution

When an incoming PUT arrives for a key that already exists locally, a `ConflictResolver` decides which entry survives.

### Default: Last-Write-Wins via HLC (`LWWResolver`)

The built-in resolver compares `HLCTimestamp` values in order:

1. Higher logical clock wins
2. If equal logical: higher counter wins
3. If equal logical and counter: higher `originNodeId` UUID wins (deterministic tiebreak)

```java
// Default — no builder call needed
ReplicationEngine.Builder eb = ReplicationEngine.builder()
        .conflictResolver(LWWResolver.INSTANCE); // this is the default
```

### Custom resolver

```java
ReplicationEngine.Builder eb = ReplicationEngine.builder()
        .conflictResolver((existing, incoming) -> {
            // return the entry that should be stored
            return existing.hlc().compareTo(incoming.hlc()) >= 0 ? existing : incoming;
        });
```

`ConflictResolver` is a `@FunctionalInterface` — any lambda that accepts two `CacheEntry` instances and returns the winner is valid.

> **HLC advancement is unconditional.** Even when the existing entry wins the conflict, the local HLC is advanced to at least the incoming frame's timestamp. This preserves causal ordering across the cluster regardless of which entry survives.

---

## Builder reference

```java
ReplicationEngine engine = ReplicationEngine.builder()
        .localNodeId(UUID)               // required — stable node identity
        .transport(TcpServer)            // required — the TCP server for this node
        .cluster(GossipNode)             // required — source of alive membership
        .blockLookup(Function<String,    // required — maps block name to CacheBlock
                     CacheBlock<?,?>>)
        .conflictResolver(ConflictResolver) // default: LWWResolver.INSTANCE
        .ackTimeoutMs(long)              // default: 500 ms — see table below
        .batchWindowMs(long)             // default: 2 ms  — see table below
        .batchMaxFrames(int)             // default: 64    — see table below
        .build();
```

### Configuration reference

| Option | Default | When to change |
|---|---|---|
| `ackTimeoutMs` | `500` | Increase on high-latency or geographically distributed clusters. Decrease on fast LANs where sub-100 ms ACKs are expected and you want tight failure detection. |
| `batchWindowMs` | `2` | Increase (up to ~10 ms) to reduce syscall rate under sustained write bursts. Decrease to reduce replication latency at the cost of more `channel.write()` calls. |
| `batchMaxFrames` | `64` | Increase for write-heavy workloads producing many small frames. Decrease if per-frame memory budget is a concern (each enqueued event holds a reference until flushed). |

---

## Scaling selector threads

`selectorThreads` is a `TcpServer.Builder` option, not a `ReplicationEngine` option. It controls how many NIO selector threads share the incoming connection load. Because the replication engine puts one persistent TCP connection per peer on the selector, cluster size is the primary driver of when to increase it.

```java
TcpServer server = TcpServer.builder()
        .port(7890)
        .handler(eb.frameHandler())
        .selectorThreads(2)   // default 1 — see sizing table below
        .workerThreads(4)     // default 4 — frame dispatch pool per selector thread
        .build();
```

`total I/O threads = selectorThreads × workerThreads`

### Sizing table

| Cluster size | `selectorThreads` | `workerThreads` | Notes |
|---|---|---|---|
| 3–20 nodes | `1` (default) | `4` (default) | One selector handles all peer channels comfortably. No change needed. |
| 20–50 nodes | `2` | `4` | Two selectors split the channel load; batch ACK processing improves throughput. |
| 50–100 nodes | `4` | `4` | Four selectors; each handles ~25 peer channels. |
| 100+ nodes | `ceil(N / 30)`, capped at CPU cores | `4` | Rule of thumb: one selector per ~30 peer connections. |

**Rule of thumb:** 1 selector thread per ~30 peer connections, never exceeding the number of available CPU cores.

### When the default (1) is the right choice

- Clusters of fewer than ~20 nodes
- Any cluster where write throughput is moderate (< ~50k events/s per node)
- Environments where thread count must be minimised (containers with tight CPU limits)

A single selector thread batches all ready channels in one `select()` call — one wakeup handles N simultaneous ACKs. This is often faster than multiple threads because it eliminates cross-thread contention on `AckTracker`'s `ConcurrentHashMap` and keeps the CPU cache warm.

### When to increase selector threads

- Clusters of 20+ nodes where `select()` call latency becomes measurable (visible as elevated `AvgReplicationLagMs` in JMX)
- Sustained write bursts where ACK frames from many peers arrive simultaneously and compete for the single selector's attention
- Profiling shows the `flaircache-nio-selector` thread as a consistent CPU bottleneck

### What increasing selector threads does NOT help with

- Slow `FrameHandler` callbacks — those run on worker threads, not the selector; increase `workerThreads` instead
- Large frame payloads — frame encoding/decoding is CPU-bound on the worker pool, not the selector
- QUORUM/STRONG timeout frequency — timeouts are almost always caused by peer latency or a slow peer, not selector saturation

---

## Thread model

| Thread name | Count | Role |
|---|---|---|
| `flaircache-nio-selector` | 1 per event loop | TCP I/O — shared with `flair-cache-transport` |
| `flaircache-replication-fan` | 1 | Drains the event queue, encodes frames, fans out to all peers |
| `flaircache-ack-sweep` | 1 | Sweeps `PendingWrite` entries every 100 ms; expires overdue ones |
| `flaircache-peer-connect` | pool | Establishes outgoing TCP connections to new peers (short-lived) |

All threads are daemon threads created by `FlairCacheThreadFactory`.

The fanout thread is the only writer to the outgoing queue. The selector thread handles reads (inbound ACKs, inbound PUT/DELETE frames). The two never contend on queue state — `LinkedBlockingQueue` mediates between the calling thread (`replicate()`) and the fanout thread.

---

## Queue capacity

The internal replication queue holds up to **65,536 pending frames**. Behaviour when the queue is full:

| Mode | Behaviour |
|---|---|
| `EVENTUAL` | Frame is silently dropped; a `WARNING` is logged. No exception. |
| `QUORUM` / `STRONG` | `replicate()` immediately throws `ReplicationTimeoutException`. |

The queue capacity is not configurable in V1. Sustained burst workloads that exhaust this limit should switch to `EVENTUAL` for non-critical writes or scale write throughput across more nodes.

---

## Performance notes

- Outgoing frames are batched: up to 64 frames are coalesced per TCP flush, or flushed after 2 ms—whichever comes first. This keeps syscall rate low under write bursts.
- The selector thread never processes `FrameHandler` callbacks directly. Inbound frame dispatch is handed to the transport's worker pool immediately after reassembly, so a slow incoming callback cannot stall the selector.
- ACK tracking uses `ConcurrentHashMap` with atomic counters — no locking on the acknowledgement hot path.
- `EVENTUAL` writes incur one `LinkedBlockingQueue.offer()` call after the local store write. No further allocation on the calling thread.
- `QUORUM`/`STRONG` writes allocate a `PendingWrite` (one `CompletableFuture` + one `AtomicInteger`) per call. Reuse the calling thread for retry logic rather than spawning new threads per write.

---

## Known limitations

**DELETE is unconditional**
`DELETE` frames do not participate in HLC Last-Write-Wins ordering. When a DELETE is applied on a peer, the key is removed regardless of whether a concurrent `PUT` on another node carries a higher HLC timestamp. Applications issuing concurrent writes and deletes to the same key may observe unexpected removals under network partition or message reordering. Tombstone-based LWW for DELETEs is planned for V2.

**Replication queue capacity is fixed**
The 65,536-frame queue is not tunable via configuration in V1. See [Queue capacity](#queue-capacity) for behaviour under saturation.

**No persistence**
FLAIR is a pure in-memory store. A node that restarts loses its local state and relies on `flair-cache-bootstrap` to receive a full copy from a live peer before it can serve reads.

---

## Design constraints

- **No frame encoding in `flair-cache-transport`.** `RawFrame` carries a type byte and a raw `byte[]`. All encoding and decoding of cache protocol messages lives here — `FrameEncoder` and `FrameDecoder` are internal to `flair-cache-replication` and not exposed to the transport layer.
- **No dependency on `flair-cache-bootstrap`.** Bootstrap sync is a separate compositional layer. `ReplicationEngine` provides `connectAsync(NodeInfo)` as a package-accessible hook for bootstrap to use without creating a circular dependency.
- **Reads are always local.** `ReplicationEngine` only handles write propagation. `CacheBlock.get()` is a pure `ConcurrentHashMap` lookup — it has no awareness of the replication layer.

---

## License

Apache License, Version 2.0. See [LICENSE](../LICENSE).
