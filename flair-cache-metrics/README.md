# flair-cache-metrics

**JMX metrics registry for the full FLAIR Cache stack — hit rates, replication lag, cluster health, and eviction counts in one place.**

Part of the [FLAIR Cache](../README.md) library. Wires into every other FLAIR module and publishes a unified set of JMX MBeans under the `com.simplj.flair.cache` domain, readable from any JMX client (JConsole, JVisualVM, your APM agent).

---

## What it does

- Registers four JMX MBean types: `CacheMetrics` (per cache block), `ReplicationMetrics`, `ClusterMetrics`, and `EvictionMetrics`
- Exposes a `MetricsRegistry` for programmatic access to the same values — no JMX client required
- All counters start from the moment a component is registered — pre-registration activity is excluded
- Registration is idempotent: calling `registerBlock()` or `withCluster()` more than once for the same component returns the existing bean and does nothing else
- `shutdown()` deregisters all MBeans and releases all block references

**What it does not do:**

- Produce metrics in any format other than JMX. Adapting to Micrometer, Prometheus, or other sinks is the caller's responsibility — read from `MetricsRegistry` programmatic accessors and push to your chosen sink.
- Expose any internal FLAIR implementation detail. All public API in this module is observability-only: read-only metrics and counters.

---

## Maven

```xml
<dependency>
    <groupId>com.simplj.flair</groupId>
    <artifactId>flair-cache-metrics</artifactId>
    <version>${flair.version}</version>
</dependency>
```

This module brings in all other FLAIR modules transitively — `flair-cache-serial`, `flair-cache-transport`, `flair-cache-gossip`, `flair-cache-hlc`, `flair-cache-store`, `flair-cache-replication`, `flair-cache-bootstrap`, `flair-cache-dsl`, and `flair-cache-watch`. No additional dependencies are needed.

---

## Quick start

```java
// 1. Create the registry — registers EvictionMetrics immediately
MetricsRegistry metrics = new MetricsRegistry();

// 2. Wire optional components
metrics.withReplication(replicationEngine);  // registers ReplicationMetrics
metrics.withCluster(gossipNode);             // BEFORE gossipNode.start() — see Wiring constraints

// 3. Register each cache block — registers CacheMetrics per block
metrics.registerBlock("products",  productsBlock);
metrics.registerBlock("orders",    ordersBlock);

// 4. Start the rest of your application ...

// 5. Read metrics programmatically at any time
long   hits  = metrics.cacheHits("products");       // cumulative since registration
long   lag   = metrics.avgReplicationLagMs();
int    alive = metrics.aliveNodeCount();

// 6. On shutdown — deregisters all MBeans
metrics.shutdown();
```

All four MBean types are now visible in JConsole under **`com.simplj.flair.cache`**.

---

## MetricsRegistry

The single entry point for this module. Create one instance per FLAIR deployment.

```java
MetricsRegistry metrics = new MetricsRegistry();
```

The constructor immediately registers the `EvictionMetrics` MBean, which aggregates eviction and expiration counts across all blocks registered later. No other MBeans are registered until you call the wiring methods.

### `registerBlock(String blockName, CacheBlock<?,?> block)` → `CacheMetricsMBean`

Registers a cache block and its per-block `CacheMetrics` MBean. Returns the bean so you can record additional events directly if needed.

```java
CacheMetricsMBean bean = metrics.registerBlock("products", productsBlock);
```

**First-registration wins.** Calling `registerBlock()` with the same `blockName` a second time — even with a different `CacheBlock` instance — returns the original bean without attaching new listeners or updating the backing block reference. This prevents dangling listeners and divergent eviction aggregates.

**Baseline.** All counters in the returned bean start from zero at the moment of registration. Operations performed on the block before `registerBlock()` is called are not counted.

### `deregisterBlock(String blockName)`

Removes the block from JMX and from the eviction aggregate. Safe to call with an unregistered name (no-op).

```java
metrics.deregisterBlock("products");
```

After deregistration, `cacheMetrics("products")` returns `null` and `cacheHits("products")` returns `0`. If you subsequently call `registerBlock("products", newBlock)`, a fresh bean is created with counters starting from zero relative to the new registration time.

> Note: `CacheBlock` has no listener-removal API. The `PutListener` and `DeleteListener` attached by the deregistered `CacheMetricsMBean` remain on the block until it is closed. They are harmless — they increment counters in an orphaned bean that nothing reads — but they cannot be removed.

### `withReplication(ReplicationEngine engine)` → `ReplicationMetricsMBean`

Wires the replication engine and registers the `ReplicationMetrics` MBean. Returns the bean so the engine or your write path can call `recordReplicationLag()` and `recordDroppedFrame()`.

```java
ReplicationMetricsMBean repBean = metrics.withReplication(replicationEngine);
```

Idempotent: a second call returns the existing bean without creating a new one or resetting accumulated history.

### `withCluster(GossipNode gossipNode)` → `ClusterMetricsMBean`

Wires the gossip node and registers the `ClusterMetrics` MBean. The registry registers itself as a `MembershipListener` on the node to count dead-node events.

```java
ClusterMetricsMBean clusterBean = metrics.withCluster(gossipNode);
clusterBean.setBootstrapSyncDurationMs(syncDurationMs); // called by BootstrapSync on join
```

**Ordering constraint:** call `withCluster()` after `GossipNode.build()` but before `GossipNode.start()`. Dead-node events that fire between `start()` and this call are missed permanently — `DeadNodeCount` will under-count. A `WARNING` is logged if you call `withCluster()` after the node is already started.

Idempotent: a second call returns the existing bean without adding a second `MembershipListener` (which would cause each dead-node event to be counted twice).

### Programmatic accessors

These read the same values available via JMX without requiring a JMX connection:

```java
metrics.cacheHits("products")    // long  — cumulative hits since registerBlock()
metrics.cacheMetrics("products") // CacheMetricsMBean — null if not registered
metrics.avgReplicationLagMs()    // long  — 0 if withReplication() not called
metrics.aliveNodeCount()         // int   — 0 if withCluster() not called
metrics.replicationMetrics()     // ReplicationMetricsMBean — null if not wired
metrics.clusterMetrics()         // ClusterMetricsMBean     — null if not wired
metrics.evictionMetrics()        // EvictionMetricsMBean    — always non-null
```

### `shutdown()`

Deregisters all JMX MBeans and clears all internal block references. After `shutdown()`:
- All programmatic accessors return `null` or `0`
- `evictionMetrics().getEvictedEntryCount()` returns `0` (no blocks remain in the aggregate map)
- The registry holds no strong references to `CacheBlock` instances — blocks may be GC'd once the application releases its own references

`shutdown()` is idempotent — safe to call more than once.

---

## JMX attribute reference

All MBeans are registered under the domain **`com.simplj.flair.cache`**.

### CacheMetrics — one per registered block

ObjectName: `com.simplj.flair.cache:type=CacheMetrics,block=<blockName>`

Block names that contain JMX-special characters (spaces, colons, asterisks, etc.) are automatically quoted — the ObjectName in JConsole will show the quoted form.

All counters are cumulative since `registerBlock()` was called — not since the block was created.

| Attribute | Type | Description |
|---|---|---|
| `HitCount` | `long` | Successful `get()` calls since registration |
| `MissCount` | `long` | `get()` calls that returned null (absent or expired) since registration |
| `HitRatePercent` | `double` | `hits / (hits + misses) * 100.0`; `0.0` if no accesses yet |
| `Size` | `long` | Current number of entries in the block (point-in-time) |
| `EvictionCount` | `long` | Entries removed by eviction policy since registration |
| `ExpirationCount` | `long` | Entries removed by TTL (lazy or sweep) since registration |
| `PutCount` | `long` | Total writes since registration — local `put()` and replicated `putRaw()` |
| `DeleteCount` | `long` | Explicit deletes of keys that were present since registration |

> **PutCount counts replicated writes too.** Both local `put()` and incoming replication writes via `putRaw()` fire the `PutListener`. This is intentional — `PutCount` represents the total number of times an entry was written to this block on this node, regardless of origin.

### ReplicationMetrics — one per registry (registered via `withReplication()`)

ObjectName: `com.simplj.flair.cache:type=ReplicationMetrics`

The `ReplicationMetricsMBean` exposes both pull-based attributes (delegated to the engine) and push-based counters (recorded explicitly by the engine or your write path). The engine records frames sent and received automatically — you record lag and drops from the points in your code where those events are known.

| Attribute | Type | Description |
|---|---|---|
| `PendingFrameCount` | `long` | Frames currently queued for outbound delivery (live read from engine) |
| `AvgReplicationLagMs` | `long` | Rolling average of replication lag in ms; `0` if no lag recorded yet |
| `MaxReplicationLagMs` | `long` | Maximum observed replication lag in ms since wiring |
| `DroppedFrameCount` | `long` | Frames dropped because the outbound queue was full |
| `AckTimeoutCount` | `long` | QUORUM/STRONG writes that timed out waiting for ACKs |
| `BytesSentTotal` | `long` | Total bytes written to outbound TCP streams since wiring |
| `BytesReceivedTotal` | `long` | Total bytes read from inbound TCP streams since wiring |

Recording lag and events from your engine:

```java
ReplicationMetricsMBean bean = metrics.withReplication(engine);

// Call from your ACK-tracking path when a round-trip is measured
bean.recordReplicationLag(observedLagMs);  // negative values are clamped to 0

// Call from your fanout path when a frame is dropped
bean.recordDroppedFrame();

// Call from your AckTracker when a QUORUM/STRONG write times out
bean.recordAckTimeout();

// Call from your TCP write path
bean.recordBytesSent(frameBytes);
bean.recordBytesReceived(frameBytes);
```

### ClusterMetrics — one per registry (registered via `withCluster()`)

ObjectName: `com.simplj.flair.cache:type=ClusterMetrics`

| Attribute | Type | Description |
|---|---|---|
| `AliveNodeCount` | `int` | Live nodes in the current membership list (excludes self) |
| `SuspectedNodeCount` | `int` | Nodes marked SUSPECTED — missed pings, not yet confirmed dead |
| `DeadNodeCount` | `long` | Cumulative count of nodes confirmed dead via SWIM failure detection |
| `GossipTickCount` | `long` | Number of SWIM gossip rounds completed since node start |
| `BootstrapSyncDurationMs` | `long` | Duration of the most recent bootstrap sync; `0` if no sync has occurred |

`AliveNodeCount` and `SuspectedNodeCount` are live reads delegated to the `MembershipList` on every JMX poll. `DeadNodeCount` is a cumulative counter incremented each time the gossip layer calls `onDead()` on the registry's listener — it does not decrement when a node later rejoins.

Call `setBootstrapSyncDurationMs()` from `BootstrapSync` when a join sync completes:

```java
ClusterMetricsMBean bean = metrics.withCluster(gossipNode);
// ...in BootstrapSync, after sync completes:
bean.setBootstrapSyncDurationMs(System.currentTimeMillis() - syncStartMs);
```

### EvictionMetrics — always registered (created in `MetricsRegistry` constructor)

ObjectName: `com.simplj.flair.cache:type=EvictionMetrics`

Aggregates eviction and expiration counts across all currently registered blocks by summing `CacheStats` on every read. Blocks added via `registerBlock()` are included immediately; blocks removed via `deregisterBlock()` or `shutdown()` are excluded on the next read.

| Attribute | Type | Description |
|---|---|---|
| `EvictedEntryCount` | `long` | Sum of evictions across all registered blocks |
| `ExpiredEntryCount` | `long` | Sum of expirations across all registered blocks |

> Both attributes are computed on demand by iterating all registered blocks. For deployments with many blocks, a JMX client that polls at high frequency will cause proportionally more work. Standard JMX poll intervals (10–30s) are fine.

---

## Wiring constraints

| Rule | Consequence if violated |
|---|---|
| Call `withCluster()` before `GossipNode.start()` | Dead-node events between `start()` and `withCluster()` are missed; `DeadNodeCount` permanently under-counts. A `WARNING` is logged. |
| Call `withReplication()` before the engine begins replicating | Lag and drop events before `withReplication()` are not counted. |
| Call `registerBlock()` before the block receives writes | Writes before registration are not counted in `PutCount`. |

All three wiring methods (`registerBlock`, `withReplication`, `withCluster`) are safe to call concurrently from multiple threads — each is internally synchronized to prevent double-registration.

---

## Reading metrics without JMX

The same values are available programmatically without a JMX client. This is useful for health-check endpoints, custom metrics bridges, or unit tests:

```java
// Null-safe helpers — return 0/0L for unwired components
long   hits      = metrics.cacheHits("products");
long   lag       = metrics.avgReplicationLagMs();
int    alive     = metrics.aliveNodeCount();

// Typed bean access — null if that component was not wired
CacheMetricsMBean     cache  = metrics.cacheMetrics("products");
ReplicationMetricsMBean rep  = metrics.replicationMetrics();
ClusterMetricsMBean cluster  = metrics.clusterMetrics();
EvictionMetricsMBean  evict  = metrics.evictionMetrics(); // always non-null

// Full attribute access on a bean
if (cache != null) {
    double rate = cache.getHitRatePercent();
    long   puts = cache.getPutCount();
}
```

### Pushing to Micrometer / Prometheus

```java
// Example: poll every 30s and push to Micrometer's MeterRegistry
Metrics.gauge("flair.cache.hits",    Tags.of("block", "products"),
              metrics, m -> m.cacheHits("products"));
Metrics.gauge("flair.replication.lag.avg.ms", metrics,
              m -> m.avgReplicationLagMs());
Metrics.gauge("flair.cluster.alive",          metrics,
              m -> m.aliveNodeCount());
Metrics.gauge("flair.cache.evictions.total",
              metrics, m -> m.evictionMetrics().getEvictedEntryCount());
```

---

## Viewing metrics in JConsole

1. Start your application.
2. Launch JConsole (`jconsole` from your JDK `bin/`) and connect to the process.
3. Open the **MBeans** tab.
4. Expand **`com.simplj.flair.cache`** in the left pane.
5. Browse `CacheMetrics`, `ReplicationMetrics`, `ClusterMetrics`, and `EvictionMetrics`.

For `CacheMetrics`, each registered block appears as a child node under `type=CacheMetrics` with its own `block=<name>` key property.

---

## Dependencies

| Module | Role |
|---|---|
| `flair-cache-store` | `CacheBlock` and `CacheStats` — source of hit/miss/eviction/size data |
| `flair-cache-replication` | `ReplicationEngine` — source of pending frame count |
| `flair-cache-gossip` | `GossipNode` and `MembershipListener` — source of alive/suspected/dead node counts |
| `flair-cache-hlc` | `HLCTimestamp` — available transitively via store and replication |
| `flair-cache-serial` | `Codec<T>` — available transitively via store |
| `flair-cache-transport` | NIO TCP layer — available transitively via replication |
| `flair-cache-bootstrap` | Bootstrap sync — available transitively via replication |
| `flair-cache-dsl` | Query DSL — declared for the full-module tier; not used by metrics directly |
| `flair-cache-watch` | Reactivity — declared for the full-module tier; not used by metrics directly |
