# flair-cache-bootstrap

**Full state transfer for new nodes joining a live FLAIR cluster.**

Part of the [FLAIR Cache](../README.md) library. When a fresh node starts up and joins a running cluster, its local store is empty. This module streams a point-in-time snapshot of every registered cache block from one live peer (the *donor*) to the joining node (the *joiner*) before the joiner begins serving reads.

---

## What it does

- Streams a full point-in-time snapshot from a donor node to a joining node over TCP
- Partitions the snapshot into bounded chunks — no single large frame, no head-of-line blocking
- Applies each incoming entry on the joiner using Last-Write-Wins conflict resolution; entries the joiner already holds are only overwritten if the donor's version is newer
- Filters expired entries at snapshot time — expired data is never transferred
- Preserves TTL (`expiryEpochMs`) exactly: entries expire on the joiner on the same schedule as on the donor
- Buffers replication events that arrive during the sync window and replays them with LWW after the snapshot is fully applied — no write is lost or silently overwritten
- Retries donor selection with exponential backoff; multiple seed addresses can be configured for resilience
- TLS support via `javax.net.ssl` — no external TLS dependency

**What it does not do:**

- Peer discovery — that is `flair-cache-gossip`'s responsibility
- Ongoing write replication — that belongs to `flair-cache-replication`
- Incremental or delta sync — every join transfers the full snapshot; partial sync is a V2 item

---

## Maven

```xml
<dependency>
    <groupId>com.simplj.flair</groupId>
    <artifactId>flair-cache-bootstrap</artifactId>
    <version>${flair.version}</version>
</dependency>
```

This module brings in `flair-cache-serial`, `flair-cache-transport`, `flair-cache-gossip`, `flair-cache-hlc`, `flair-cache-store`, and `flair-cache-replication` transitively.

---

## Dependencies

| Module | Role in bootstrap |
|---|---|
| `flair-cache-transport` | Provides `TcpServer` and `TcpClient` — the physical TCP connection to the donor |
| `flair-cache-store` | Provides `CacheBlock` — source of the snapshot on the donor, destination of entries on the joiner |
| `flair-cache-hlc` | Provides `HLCTimestamp` — used for LWW conflict resolution during chunk application |
| `flair-cache-replication` | Provides `ConflictResolver`, `LWWResolver`, and `ReplicationEvent` — wired for the replication buffer drain |
| `flair-cache-serial` | Provides `Codec<T>` — used by `CacheBlock` for key/value encoding |

---

## How it works

```
New node (Joiner)                          Existing node (Donor)
─────────────────────────────────          ──────────────────────────────────────
ReplicationBuffer.startBuffering()
                                           BootstrapServer installed on TcpServer
DonorSelector tries seed addresses ─ TCP ─► TcpServer accepts connection
BootstrapSync ───── SYNC_REQ ───────────►  BootstrapServer.onFrame()
                                             └─ spawns flaircache-bootstrap-sync thread
                                             └─ takes rawSnapshotEntries() across all blocks
                                             └─ partitions into ≤chunkSizeBytes chunks
            ◄───── SYNC_CHUNK 0/N ──────────
            ◄───── SYNC_CHUNK 1/N ──────────   (one per partition)
                      ...
            ◄───── SYNC_CHUNK N-1/N ─────────
            ◄───── SYNC_DONE (totalEntries) ─  donor closes its streaming thread

applyChunk() per SYNC_CHUNK — LWW per entry
drainAndApply() — replay buffered replication events with LWW
donor.close()      ─────────────────────────  TCP connection closed
```

Replication events arriving from other peers during the sync window are captured in the `ReplicationBuffer`. After `SYNC_DONE`, `drainAndApply()` replays them against the freshly populated store with LWW: events that arrived after the snapshot point win; events that predate the snapshot are discarded.

---

## Minimal setup

### Donor side — existing node

Install `BootstrapServer` as a `FrameHandler` on the node's `TcpServer`. The easiest way is to compose it with the replication engine's handler so one TCP server handles both replication traffic and bootstrap requests.

```java
Map<String, CacheBlock<?, ?>> blocks = Map.of(
        "products", productsBlock,
        "orders",   ordersBlock
);

BootstrapServer bootstrapServer = new BootstrapServer(blocks, 65_536);

// Compose with the replication frame handler
FrameHandler compositeHandler = (conn, frame) -> {
    replicationFrameHandler.onFrame(conn, frame);
    bootstrapServer.onFrame(conn, frame);
};

TcpServer server = TcpServer.builder()
        .port(7890)
        .handler(compositeHandler)
        .build();

server.start();
```

`BootstrapServer` is stateless and thread-safe — it can be installed once and handle any number of concurrent `SYNC_REQ` frames.

### Joiner side — new node

```java
// 1. Start capturing replication events BEFORE connecting to the donor
ReplicationBuffer buffer = new ReplicationBuffer();
replicationEngine.onIncoming(buffer::offer);
buffer.startBuffering();

// 2. Run the sync — blocks until SYNC_DONE is received or timeout fires
try {
    SyncResult result = BootstrapSync.builder()
            .blocks(Map.of("products", productsBlock, "orders", ordersBlock))
            .localNodeId(myNodeId)
            .donorAddress("10.0.0.1", 7890)   // single seed
            .syncTimeoutMs(30_000)
            .replicationBuffer(buffer)
            .build()
            .syncFromPeer();

    System.out.printf("Sync complete: %d entries in %d chunks (%d ms)%n",
            result.totalEntries(), result.chunksReceived(), result.durationMs());

} catch (SyncTimeoutException e) {
    // All donor seeds unreachable or SYNC_DONE never arrived within syncTimeoutMs
    buffer.stopBuffering(); // clear the buffer so it does not grow indefinitely
    throw e;
}

// 3. Node is ready — join the gossip ring and start serving reads
gossip.start();
```

> **Call `buffer.startBuffering()` before `syncFromPeer()`**, not after. Any replication events that arrive between those two calls would be missed, leaving the joiner's store inconsistent with peers that received the write during that window.

---

## Donor selection and retry

When a single seed address is insufficient — for example, when any node in the cluster can serve as a donor — configure multiple seeds. `DonorSelector` tries them in round-robin order with exponential backoff until one accepts the connection or the total timeout elapses.

```java
DonorSelector selector = DonorSelector.builder()
        .seed("10.0.0.1", 7890)
        .seed("10.0.0.2", 7890)
        .seed("10.0.0.3", 7890)
        .connectTimeoutMs(3_000)   // per-attempt connection timeout; default 3000 ms
        .build();

SyncResult result = BootstrapSync.builder()
        .blocks(blocks)
        .localNodeId(myNodeId)
        .donorSelector(selector)   // replaces .donorAddress()
        .syncTimeoutMs(30_000)
        .build()
        .syncFromPeer();
```

### Backoff schedule

| Retry round | Delay before next attempt |
|---|---|
| 1 | 100 ms |
| 2 | 200 ms |
| 3 | 400 ms |
| 4 | 800 ms |
| 5 | 1 600 ms |
| 6 | 3 200 ms |
| 7 | 6 400 ms |
| 8+ | 10 000 ms |

If the remaining time before `syncTimeoutMs` is less than the scheduled delay, the delay is capped to the remaining time.

---

## TLS

To encrypt the bootstrap connection, supply a `TlsConfig` to both the donor's `TcpServer` and the joiner's `DonorSelector`. The donor requires no bootstrap-specific TLS configuration beyond what its `TcpServer` already uses.

```java
// Joiner — supply the same TlsConfig used by the rest of the cluster
DonorSelector selector = DonorSelector.builder()
        .seed("10.0.0.1", 7890)
        .tls(clusterTlsConfig)   // TlsConfig.disabled() is the default
        .build();
```

---

## Conflict resolution during sync

When a joiner already holds an entry for a key (e.g. from a pre-loaded warm-up), the incoming donor entry is not applied blindly. The `ConflictResolver` decides which version survives.

The default resolver is `LWWResolver.INSTANCE` — Last-Write-Wins based on `HLCTimestamp`, with `originNodeId` as a deterministic tiebreaker when timestamps are equal. Entries with a higher timestamp always replace entries with a lower timestamp, regardless of which node holds which version.

```java
// Default — LWW, no configuration needed
BootstrapSync.builder()
        .conflictResolver(LWWResolver.INSTANCE); // this is the default
```

To use a custom resolver:

```java
BootstrapSync.builder()
        .conflictResolver((existing, incoming) -> {
            return existing.hlc().compareTo(incoming.hlc()) >= 0 ? existing : incoming;
        });
```

The same resolver is used for both chunk application and the replication buffer drain, so conflict semantics are consistent throughout the join sequence.

---

## Builder reference

### `BootstrapServer`

```java
new BootstrapServer(
    Map<String, CacheBlock<?, ?>> blocks,  // required — blocks to snapshot
    int chunkSizeBytes                     // required — max payload bytes per SYNC_CHUNK frame
)
```

| Parameter | Recommended | When to change |
|---|---|---|
| `chunkSizeBytes` | `65_536` (64 KB) | Reduce to ~16 KB if individual cache entries are large (> 10 KB) to avoid monopolising the TCP write buffer. Increase toward 256 KB only on very high-latency links where round-trip count dominates total sync time. |

### `BootstrapSync.Builder`

```java
SyncResult result = BootstrapSync.builder()
        .blocks(Map<String, CacheBlock<?,?>>)  // required
        .localNodeId(UUID)                      // default: random UUID
        .donorAddress(String host, int port)    // convenience: single-seed selector
        .donorSelector(DonorSelector)           // alternative: multi-seed selector
        .syncTimeoutMs(long)                    // default: 30 000 ms
        .conflictResolver(ConflictResolver)     // default: LWWResolver.INSTANCE
        .replicationBuffer(ReplicationBuffer)   // default: null (buffering disabled)
        .build()
        .syncFromPeer();
```

| Option | Default | When to change |
|---|---|---|
| `localNodeId` | `UUID.randomUUID()` | Always set this to the node's stable identity. A random UUID is only acceptable in tests. |
| `syncTimeoutMs` | `30 000` | Increase for large stores (> 1 M entries) or high-latency links. Decrease in test environments where immediate failure is preferred over a 30-second wait. |
| `conflictResolver` | `LWWResolver.INSTANCE` | Override only if the cluster uses a custom resolver for ongoing replication — keep both in sync. |
| `replicationBuffer` | `null` | Always supply one in production. Omitting it is only safe when the node is completely isolated from replication traffic during the sync window, which is never true in a live cluster. |

### `DonorSelector.Builder`

```java
DonorSelector selector = DonorSelector.builder()
        .seed(String host, int port)   // required — call multiple times for redundancy
        .connectTimeoutMs(int)         // default: 3 000 ms
        .tls(TlsConfig)                // default: TlsConfig.disabled()
        .build();
```

| Option | Default | When to change |
|---|---|---|
| `connectTimeoutMs` | `3 000` | Increase on slow or high-latency networks. Decrease in tests or when fast-fail behaviour is required. |

---

## `SyncResult`

`syncFromPeer()` returns a `SyncResult` on success:

```java
SyncResult result = sync.syncFromPeer();

result.totalEntries();   // total entries streamed, as reported by the donor in SYNC_DONE
result.chunksReceived(); // number of SYNC_CHUNK frames received
result.durationMs();     // wall-clock milliseconds from start to SYNC_DONE received
```

`totalEntries()` reflects the donor's count at snapshot time — entries for blocks the joiner does not have registered are included in the count but silently skipped during application.

---

## `ReplicationBuffer`

`ReplicationBuffer` is a thread-safe queue that captures incoming replication events during the bootstrap sync window. Wire it into the replication engine's incoming callback before calling `syncFromPeer()`.

```java
ReplicationBuffer buffer = new ReplicationBuffer();

// Wire into the replication engine
replicationEngine.onIncoming(buffer::offer);

// Activate BEFORE connecting to the donor
buffer.startBuffering();

// syncFromPeer() automatically calls drainAndApply() on success
SyncResult result = BootstrapSync.builder()
        .replicationBuffer(buffer)
        ...
        .build()
        .syncFromPeer();

// If sync fails, stop buffering to prevent unbounded queue growth
// (syncFromPeer() does NOT call stopBuffering on failure — the caller must)
```

| Method | When to call |
|---|---|
| `startBuffering()` | Before `syncFromPeer()` — captures all events from this point forward |
| `drainAndApply()` | Called automatically by `BootstrapSync` after `SYNC_DONE` — do not call manually |
| `stopBuffering()` | In the `catch` block when sync fails — clears the queue and stops capturing |

After `drainAndApply()` returns, the buffer is inactive. Subsequent `offer()` calls are no-ops; a new `startBuffering()` would be needed to start a second sync attempt.

---

## Thread model

| Thread name | Count | Role |
|---|---|---|
| `flaircache-bootstrap-sync` | 1 per active `SYNC_REQ` | Donor-side: reads the snapshot, encodes chunks, streams to joiner. Ephemeral — exits after `SYNC_DONE` is sent. |
| `flaircache-nio-selector` | shared with transport | Joiner-side: receives `SYNC_CHUNK` and `SYNC_DONE` frames from the donor TCP connection |

All threads are daemon threads created by `FlairCacheThreadFactory`. The joiner's `syncFromPeer()` call blocks on the calling thread (typically the application startup thread) until `SYNC_DONE` is received or the timeout fires.

---

## Rolling joins

In V1, the donor spawns one `flaircache-bootstrap-sync` thread per simultaneous `SYNC_REQ`, with no upper bound. Each thread holds a full point-in-time snapshot of all registered blocks in memory for the duration of the transfer.

**Strongly recommended:** bring nodes up one at a time (or in small batches of 2–3). Joining 20+ nodes simultaneously creates 20+ concurrent snapshot threads on the donor and proportional heap pressure.

This limit is tracked as a V2 enhancement — see [Known Limitations](../README.md#known-limitations).

---

## Known limitations

**No concurrency cap on the donor**
The donor spawns one `flaircache-bootstrap-sync` thread per incoming `SYNC_REQ` with no configurable upper bound. Joining many nodes simultaneously places proportional heap and thread pressure on the donor. Use rolling joins in V1. A semaphore-backed sync pool with a configurable cap is planned for V2.

**Full snapshot only**
Every join transfers the complete state of all registered blocks. There is no incremental or delta sync. For clusters with large stores (> 10 M entries), plan for the sync duration and size accordingly.

**No persistence**
FLAIR is a pure in-memory store. A node that restarts loses its local state and must complete a full bootstrap sync from a live peer before it can serve consistent reads.

---

## License

Apache License, Version 2.0. See [LICENSE](../LICENSE).
