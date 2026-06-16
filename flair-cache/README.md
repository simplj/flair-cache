# flair-cache

The `flair-cache` artifact is the single entry point for the FlairCache distributed in-memory
cache. It wires all sub-modules — transport, gossip, replication, store, DSL, watch, metrics,
and bootstrap — into one cohesive API exposed through the `FlairCache` facade class.

Pull in this artifact and you have everything. No separate imports of sub-modules required.

---

## Dependency

**Maven**
```xml
<dependency>
    <groupId>com.simplj.flair</groupId>
    <artifactId>flair-cache</artifactId>
    <version>0.1.0</version>
</dependency>
```

**Gradle**
```groovy
implementation 'com.simplj.flair:flair-cache:0.1.0'
```

---

## Quick Start

```java
// 1. Build and start
FlairCache cache = FlairCache.builder()
    .bindPort(7890)
    .seedPeers(List.of("10.0.0.2:7890", "10.0.0.3:7890"))
    .consistency(ConsistencyMode.QUORUM)
    .build()
    .start();

// 2. Register a typed block (must be done after start())
CacheBlock<String, Product> products = cache.<String, Product>registerBlock("products")
    .keyCodec(Codecs.string())
    .valueCodec(productCodec)
    .ttl(Duration.ofMinutes(30))
    .eviction(EvictionPolicy.LRU)
    .maxEntries(100_000)
    .build();

// 3. Write on any node — replicates to all peers
products.put("p1", new Product("Laptop", 999.0));

// 4. Read on any node — always local, always sub-microsecond
Product p = products.get("p1");

// 5. Shut down cleanly
cache.shutdown();
```

---

## Lifecycle

```
FlairCache.builder()     ← configure
    .build()             ← creates instance (no I/O yet)
    .start()             ← binds TCP/UDP ports, joins cluster (may throw IOException)

registerBlock()          ← create and wire typed blocks (after start())
bootstrapSync()          ← pull state from a peer (after blocks are registered, joining nodes only)

// normal operation — get/put/query/watch

shutdown()               ← flush pending writes, broadcast LEAVE, release all resources
```

`FlairCache` implements `Closeable` — `try`-with-resources works.

---

## Builder Options

`FlairCache.builder()` returns a `FlairCacheBuilder`. All options have production-ready defaults;
typically only `bindPort` and `seedPeers` need to be set per deployment.

| Method | Default | Notes |
|---|---|---|
| `bindPort(int)` | `7890` | TCP and UDP port. Must be unique per node on the same host. |
| `bindAddress(String)` | `"0.0.0.0"` | NIC to bind; set to a specific IP in multi-homed hosts. |
| `seedPeers(List<String>)` | `[]` | `"host:port"` list. Omit for a standalone single-node instance. |
| `consistency(ConsistencyMode)` | `QUORUM` | Default for all blocks; overridable per block. |
| `tls(TlsConfig)` | disabled | Mutual TLS via `javax.net.ssl`. |
| `ackTimeoutMs(long)` | `500` | Milliseconds to wait for QUORUM/STRONG ACKs before throwing. |
| `batchWindowMs(long)` | `2` | Replication flush window in ms. Lower = lower latency; higher = higher throughput. |
| `batchMaxFrames(int)` | `64` | Flush immediately when this many frames are queued, regardless of window. |
| `selectorThreads(int)` | `1` | NIO selector thread count. See [sizing guidance](#nio-selector-threads). |
| `nodeId(UUID)` | auto-generated | Override to assign a stable identity across restarts. |

### NIO selector threads

One selector thread handles all TCP I/O across all peer connections. This is sufficient for
most deployments.

| Cluster size | `selectorThreads` |
|---|---|
| 1–20 nodes | `1` (default) |
| 20–50 nodes | `2` |
| 50–100+ nodes | `3–4` |

---

## `FlairCache` API

### Lifecycle

| Method | Description |
|---|---|
| `start()` | Starts all modules. Throws `IOException` if TCP/UDP bind fails. Returns `this` for chaining. |
| `shutdown()` | Flushes pending writes, broadcasts LEAVE, releases all resources. Idempotent. |
| `close()` | Delegates to `shutdown()` — enables try-with-resources. |
| `isRunning()` | `true` between a successful `start()` and `shutdown()`. |

### Configuration

| Method | Description |
|---|---|
| `config()` | Returns the immutable `FlairCacheConfig` snapshot. Available before `start()`. |

### Cache blocks

| Method | Description |
|---|---|
| `registerBlock(String)` | Returns a `BlockBuilder` to create and wire a new typed block. Requires `start()`. |
| `registeredBlock(String)` | Returns an already-registered block. Throws `IllegalArgumentException` if the name is unknown. |

### Queries

| Method | Description |
|---|---|
| `query()` | Returns a `QueryEngine` over a snapshot of all registered blocks. |
| `queryWith(String, Decoder<T>)` | Returns a typed `QueryEngine` over one block, applying `decoder` to every value. Throws `NullPointerException` if decoder returns `null` for any entry. |
| `queryWith(String, Decoder<T>, boolean skipNullResults)` | Same as above; when `skipNullResults = true`, entries that decode to `null` are silently excluded rather than causing an exception. |

### Reactivity

| Method | Description |
|---|---|
| `watchRegistry(String)` | Returns the `WatchRegistry` for the named block. Register listeners for PUT / DELETE / EXPIRE events. |

### Cluster and metrics

| Method | Description |
|---|---|
| `cluster()` | Returns the live `MembershipList` — current view of ALIVE, SUSPECTED, and DEAD nodes. |
| `metrics()` | Returns the `MetricsRegistry` for JMX access. Always non-null, even before `start()`. |
| `bootstrapSync(String...)` | Pulls state from a seed peer for the named blocks (or all blocks if no names are given). Call after registering blocks when joining a cluster. |

---

## Cache Blocks

`BlockBuilder` is returned by `registerBlock("name")`. Chain options and call `.build()` to
activate the block.

```java
CacheBlock<String, Order> orders = cache.<String, Order>registerBlock("orders")
    .keyCodec(Codecs.string())
    .valueCodec(orderCodec)
    .ttl(Duration.ofHours(1))      // entries expire 1h after last write
    .eviction(EvictionPolicy.LRU)  // LRU eviction when maxEntries is reached
    .maxEntries(50_000)            // 0 = unlimited (default)
    .consistency(ConsistencyMode.EVENTUAL)  // overrides the FlairCache default for this block
    .build();
```

| Option | Default | Notes |
|---|---|---|
| `keyCodec(Codec<K>)` | required | Serializes the key type to/from `byte[]`. |
| `valueCodec(Codec<V>)` | required | Serializes the value type to/from `byte[]`. |
| `ttl(Duration)` | `Duration.ZERO` | `ZERO` means no expiry. Set to a positive duration for TTL-based expiry. |
| `eviction(EvictionPolicy)` | `NONE` | `LRU`, `LFU`, or `SIZE_BASED`. Has no effect when `maxEntries` is `0`. |
| `maxEntries(int)` | `0` (unlimited) | Eviction triggers when this many entries are in the block. `0` = never evict. |
| `consistency(ConsistencyMode)` | inherits FlairCache default | Overrides the instance-level default for this block only. |

Each block name must be unique within the `FlairCache` instance. Registering a second block
under the same name throws `IllegalStateException`.

---

## Consistency Modes

Configured on the builder (default for all blocks) or overridden per block via `BlockBuilder`.

| Mode | Write returns after | Tradeoff |
|---|---|---|
| `EVENTUAL` | Local write is committed. Replication is async. | Lowest latency. Peers may lag behind briefly. |
| `QUORUM` | N/2 + 1 nodes (including local) have acknowledged. | Balanced. Throws `ReplicationTimeoutException` if quorum is not reached within `ackTimeoutMs`. |
| `STRONG` | All live nodes have acknowledged. | Highest durability. Sensitive to slow or lagging peers. |

Reads are always local regardless of consistency mode.

---

## DSL Queries

`query()` returns a `QueryEngine` backed by point-in-time snapshots of all registered blocks.

```java
// Filter within a block
List<Product> cheap = cache.query()
    .from("products", Product.class, Decoder.typed(Product.class))
    .where(p -> p.getPrice() < 100)
    .orderBy(Comparator.comparing(Product::getPrice))
    .limit(20)
    .fetch();

// Join two blocks — zero network, entirely in-JVM
List<OrderSummary> summaries = cache.query()
    .from("orders", Order.class, orderDecoder)
    .join("customers", Customer.class, customerDecoder)
    .on((o, c) -> o.getCustomerId().equals(c.getId()))
    .select((o, c) -> new OrderSummary(o.getId(), c.getName()))
    .fetch();
```

Use `queryWith(blockName, decoder)` when the block's stored value type (e.g. `byte[]`) differs
from the type you want to query over:

```java
QueryEngine engine = cache.queryWith("products", bytes -> productCodec.deserialize(bytes));
List<Product> results = engine.from("products", Product.class, Decoder.identity())
    .where(p -> p.getCategory().equals("electronics"))
    .fetch();
```

---

## Watch / Reactivity

Subscribe to change events on any registered block. Events carry a `ChangeEvent.Source` that
distinguishes writes originating on this node from writes that arrived via replication.

```java
WatchRegistry<String, Product> watch = cache.watchRegistry("products");

watch.onEvent(event -> {
    if (event instanceof ChangeEvent.PutEvent<String, Product> put) {
        if (put.source() == ChangeEvent.Source.LOCAL) {
            // Write originated on this node
        } else {
            // Write arrived via replication from a peer
        }
    }
});
```

| Event type | Trigger |
|---|---|
| `ChangeEvent.PutEvent<K,V>` | Entry created or updated (local write or replicated write) |
| `ChangeEvent.DeleteEvent<K,V>` | Entry deleted (local delete or replicated delete) |
| `ChangeEvent.ExpireEvent<K,V>` | Entry removed by TTL expiry (always local — expiry is not replicated as an event) |

Listener dispatch is asynchronous — slow listeners never block the write path.

---

## Bootstrap Sync

When a new node joins an existing cluster it starts with an empty local store. Call
`bootstrapSync()` after registering all blocks to pull a point-in-time snapshot from
a seed peer.

```java
FlairCache cache = FlairCache.builder()
    .bindPort(7891)
    .seedPeers(List.of("10.0.0.1:7890"))
    .build()
    .start();

CacheBlock<String, Product> products = cache.<String, Product>registerBlock("products")
    .keyCodec(Codecs.string())
    .valueCodec(productCodec)
    .build();

// Pull full state from the seed peer before serving traffic
cache.bootstrapSync();  // syncs all registered blocks

// Or sync specific blocks only
cache.bootstrapSync("products", "inventory");
```

`bootstrapSync()` is a no-op when no seed peers are configured (standalone node).

During sync, live replication frames from peers are buffered internally and re-applied after
the snapshot transfer completes — no updates are lost during the sync window.

| Exception | Meaning |
|---|---|
| `SyncTimeoutException` | The donor did not complete the transfer within 30 seconds. |
| `IOException` | All configured seed peers were unreachable. |

---

## JMX Metrics

`FlairCache.metrics()` returns a `MetricsRegistry` with four MBean groups under the domain
`com.simplj.flair.cache`.

| MBean | Key attributes |
|---|---|
| `CacheMetrics` | `HitCount`, `MissCount`, `HitRatePercent` |
| `ReplicationMetrics` | `AvgReplicationLagMs`, `PendingFrameCount`, `DroppedFrameCount` |
| `ClusterMetrics` | `AliveNodeCount`, `SuspectedNodeCount` |
| `EvictionMetrics` | `EvictedEntryCount`, `ExpiredEntryCount` |

Access via any JMX client (JConsole, Java Mission Control, Prometheus JMX Exporter).

---

## Cluster Membership

```java
MembershipList members = cache.cluster();

members.alive();     // Set<NodeInfo> — nodes responding to SWIM pings
members.suspected(); // Set<NodeInfo> — nodes that missed a ping cycle
members.dead();      // Set<NodeInfo> — nodes that have been declared dead
```

Membership is maintained by SWIM gossip over UDP. Changes propagate cluster-wide within
a few hundred milliseconds.

---

## Spring Boot Integration

`FlairCache` is a plain Java object — use it as a `@Bean`. No auto-configuration module
is required.

```java
@Configuration
public class CacheConfig {

    @Bean
    public FlairCache flairCache() throws IOException {
        return FlairCache.builder()
            .bindPort(7890)
            .seedPeers(List.of("10.0.0.2:7890"))
            .consistency(ConsistencyMode.QUORUM)
            .build()
            .start();
    }

    @Bean
    public SmartLifecycle flairCacheLifecycle(FlairCache cache) {
        return new SmartLifecycle() {
            public void start()        { /* already started by bean factory */ }
            public void stop()         { cache.shutdown(); }
            public boolean isRunning() { return cache.isRunning(); }
            public boolean isAutoStartup() { return false; }
        };
    }
}
```

Inject `FlairCache` anywhere:

```java
@Service
public class ProductService {

    private final CacheBlock<String, Product> products;

    public ProductService(FlairCache cache) {
        this.products = cache.<String, Product>registerBlock("products")
            .keyCodec(Codecs.string())
            .valueCodec(productCodec)
            .ttl(Duration.ofMinutes(30))
            .build();
    }

    public Product find(String id) { return products.get(id); }
    public void save(Product p)    { products.put(p.getId(), p); }
}
```

---

## Non-Spring: FlairCacheFactory

For applications without dependency injection, `FlairCacheFactory` provides a static registry
so any class in the application can retrieve a named `FlairCache` instance.

```java
// At startup — once
FlairCacheFactory.getOrCreate("main",
    () -> FlairCache.builder()
            .bindPort(7890)
            .seedPeers(List.of("10.0.0.2:7890"))
            .build()
            .start());

// Anywhere in the application — zero argument passing
FlairCache cache = FlairCacheFactory.get("main");

// At shutdown
FlairCacheFactory.shutdownAll();
```

| Method | Description |
|---|---|
| `get(String)` | Returns the registered instance. Throws `IllegalArgumentException` if not found. |
| `getOrCreate(String, Supplier<FlairCache>)` | Returns the instance, calling the supplier to create it if it does not exist yet. Supplier is called at most once. |
| `shutdownAndRemove(String)` | Shuts down and deregisters the named instance. Returns `true` if found, `false` if not. |
| `shutdownAll()` | Shuts down and deregisters all instances. Safe to call on empty registry. |
| `size()` | Returns the current number of registered instances. |

All methods are thread-safe. `getOrCreate` guarantees the supplier is called at most once even
under concurrent access.

---

## Integration Testing: FlairCluster

`FlairCluster` spins up an embedded multi-node cluster entirely within the test JVM. No Docker,
no external ports beyond loopback, no configuration files.

```java
FlairCluster cluster = FlairCluster.builder()
    .nodes(5)
    .basePort(17890)
    .consistency(ConsistencyMode.QUORUM)
    .build()
    .start();

// Register the same block on every node
for (int i = 0; i < cluster.size(); i++) {
    cluster.node(i).<String, byte[]>registerBlock("data")
        .keyCodec(Codecs.string())
        .valueCodec(Codecs.bytes())
        .build();
}

// Write on node 0
cluster.node(0).registeredBlock("data").put("k", payload);

// Wait for replication to settle
cluster.awaitReplication(Duration.ofSeconds(2));

// Read on any other node — always local
assertNotNull(cluster.node(3).<String, byte[]>registeredBlock("data").get("k"));

cluster.shutdown();
```

| Option | Default | Notes |
|---|---|---|
| `nodes(int)` | `3` | Number of nodes to create. |
| `basePort(int)` | `17890` | Nodes bind `basePort`, `basePort+1`, …, `basePort+nodes-1`. Upper bound: `65535 − nodes + 1`. |
| `consistency(ConsistencyMode)` | `QUORUM` | Applied to all nodes. |

Node 0 is the seed — it starts with no peer list. Nodes 1 through N−1 each use node 0 as
their seed peer; gossip propagates full membership to every node within a few hundred
milliseconds.

`awaitReplication(Duration)` polls `ReplicationMetrics.getPendingFrameCount()` across all
nodes and returns as soon as all queues reach zero. It throws `IllegalStateException` if
replication has not drained before the timeout expires.

---

## Known Limitations

These are V1 scope constraints. Each is tracked for V2.

**QUORUM/STRONG exceptions are not propagated through `attachBlock`**
When a block is registered via `registerBlock()`, consistency failures (`ReplicationTimeoutException`)
are caught and logged internally — they do not reach callers of `CacheBlock.put()`. QUORUM and
STRONG modes are best-effort in V1: write delivery is attempted with the configured consistency,
but a failure downgrades silently to eventual delivery from the caller's perspective.

**Delete conflict resolution is unconditional**
`DELETE` operations are not subject to HLC-based Last-Write-Wins. A delete always wins when
it is applied, regardless of concurrent `PUT` timestamps. Tombstone-based LWW for deletes is
planned for V2.

**No persistence**
FLAIR is a pure in-memory store. A restarted node starts empty and must perform a bootstrap sync
from a live peer before serving consistent reads. Snapshot-to-disk recovery is planned for V2.

**Replication queue is fixed at 65,536 frames**
Under a very high write burst, this limit may be reached. In `EVENTUAL` mode the frame is dropped
with a `WARNING` log. In `QUORUM`/`STRONG` mode a `ReplicationTimeoutException` is thrown.
Configurable queue capacity is planned for V2.

**Bootstrap has no concurrency limit for simultaneous joins**
When many nodes join at the same time, the donor spawns one transfer thread per joiner with no
upper bound. Bring nodes up in small batches to avoid heap pressure on the donor.

---

## See Also

- [Root README](../README.md) — overview, architecture, comparison with Redis/Hazelcast
- [flair-cache-store](../flair-cache-store/README.md) — `CacheBlock` and local store details
- [flair-cache-dsl](../flair-cache-dsl/README.md) — full DSL query reference
- [flair-cache-watch](../flair-cache-watch/README.md) — watch registry and event types
- [flair-cache-replication](../flair-cache-replication/README.md) — replication engine and consistency modes
- [flair-cache-metrics](../flair-cache-metrics/README.md) — JMX MBeans reference
