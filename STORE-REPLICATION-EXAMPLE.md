# Store + Replication — Standalone Usage Example

This example shows how to use `flair-cache-store` and `flair-cache-replication` together
to build a replicated in-memory cache without pulling in the full `flair-cache` facade.

---

## Dependencies

Add only `flair-cache-replication` to your build file. It transitively pulls in
`flair-cache-store`, `flair-cache-serial`, `flair-cache-transport`, `flair-cache-gossip`,
and `flair-cache-hlc` — no additional declarations needed.

**Maven**
```xml
<dependency>
    <groupId>com.simplj.flair</groupId>
    <artifactId>flair-cache-replication</artifactId>
    <version>${flair.version}</version>
</dependency>
```

**Gradle**
```groovy
implementation "com.simplj.flair:flair-cache-replication:${flairVersion}"
```

---

## Complete Example

### 1. Implement a `Codec` for your key and value types

`Codec<T>` is the contract for binary serialization. Implement `sizeOf`, `serialize`,
and `deserialize` for each type you store.

```java
import com.simplj.flair.cache.serial.Codec;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

Codec<String> stringCodec = new Codec<String>() {

    @Override
    public int sizeOf(String v) {
        return 2 + v.getBytes(StandardCharsets.UTF_8).length;
    }

    @Override
    public void serialize(String v, ByteBuffer buf) {
        byte[] bytes = v.getBytes(StandardCharsets.UTF_8);
        buf.putShort((short) bytes.length);
        buf.put(bytes);
    }

    @Override
    public String deserialize(ByteBuffer buf) {
        int len = buf.getShort() & 0xFFFF;
        byte[] bytes = new byte[len];
        buf.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }
};
```

### 2. Build a `CacheBlock`

```java
import com.simplj.flair.cache.store.CacheBlock;
import com.simplj.flair.cache.store.EvictionPolicy;
import java.time.Duration;

CacheBlock<String, String> block = CacheBlock.<String, String>builder()
        .name("items")
        .keyCodec(stringCodec)
        .valueCodec(stringCodec)
        .ttl(Duration.ofMinutes(10))           // optional: 0 = immortal
        .eviction(EvictionPolicy.LRU)          // optional
        .maxEntries(100_000)                   // required when eviction != NONE
        .build();
```

### 3. Set up a `GossipNode` for peer discovery

Each node in the cluster needs a `GossipNode`. Provide the UDP port and the addresses
of at least one other cluster member as seed peers.

```java
import com.simplj.flair.cache.gossip.GossipNode;
import java.util.List;
import java.util.UUID;

UUID nodeId = UUID.randomUUID();

GossipNode gossip = GossipNode.builder()
        .nodeId(nodeId)
        .bindAddress("0.0.0.0")
        .bindPort(7891)                                         // UDP port for gossip
        .seedPeers(List.of("peer2host:7891", "peer3host:7891")) // at least one other node
        .build();
```

### 4. Wire `ReplicationEngine` and `TcpServer`

The engine builder must provide its `FrameHandler` to the `TcpServer` before either is
built. This is the required wiring order:

```java
import com.simplj.flair.cache.replication.ConsistencyMode;
import com.simplj.flair.cache.replication.ReplicationEngine;
import com.simplj.flair.cache.transport.TcpServer;

// 1. Create the engine builder first
ReplicationEngine.Builder engineBuilder = ReplicationEngine.builder()
        .localNodeId(nodeId)
        .cluster(gossip);

// 2. Hand the engine's FrameHandler to TcpServer before building either
TcpServer server = TcpServer.builder()
        .bindAddress("0.0.0.0")
        .port(7890)                             // TCP port for replication frames
        .handler(engineBuilder.frameHandler())  // must come before engineBuilder.build()
        .build();

// 3. Finish building the engine
ReplicationEngine engine = engineBuilder
        .transport(server)
        .build();
```

### 5. Start everything and attach the block

Start in this order: gossip → TCP server → replication engine → attach blocks.

```java
gossip.start();
server.start();
engine.start();

engine.attachBlock("items", block, ConsistencyMode.QUORUM);
```

`ConsistencyMode` options:

| Mode | Behaviour |
|---|---|
| `EVENTUAL` | Returns immediately after local write; replication is async |
| `QUORUM` | Waits until a majority of nodes have applied the write (recommended default) |
| `STRONG` | Waits until every alive peer has applied the write |

### 6. Use the cache

```java
block.put("key1", "value1");          // local write, replicated automatically
String value = block.get("key1");     // pure in-JVM read — no network
block.delete("key1");                 // local delete, replicated automatically
```

### 7. Shut down (reverse order)

```java
engine.shutdown();
server.shutdown();
gossip.shutdown();
block.close();
```

---

## Configuration reference

### `GossipNode.Builder`

| Option | Default | Notes |
|---|---|---|
| `bindPort` | — | UDP port. Must be unique per node on the host. |
| `seedPeers` | `[]` | `"host:port"` list. One seed is enough to join the cluster. |
| `tickIntervalMs` | 500 | How often to send gossip pings. |
| `probeTimeoutMs` | 2000 | Direct PING timeout before indirect probe. |
| `indirectTimeoutMs` | 2000 | Indirect probe timeout before SUSPECTED. |
| `suspicionTimeoutMs` | 10000 | Time before SUSPECTED becomes DEAD. |
| `fanout` | 3 | Number of peers pinged per tick. |

### `TcpServer.Builder`

| Option | Default | Notes |
|---|---|---|
| `port` | — | TCP port for replication frames. |
| `workerThreads` | `2 × cores` | Thread pool for frame processing. |
| `selectorThreads` | 1 | NIO selector threads. Increase for > 50 peers. |
| `maxPayloadBytes` | 1 MB | Maximum frame payload. Cap at 64 MB. |

### `ReplicationEngine.Builder`

| Option | Default | Notes |
|---|---|---|
| `batchWindowMs` | 2 | Flush interval in milliseconds. |
| `batchMaxFrames` | 64 | Flush when this many frames are queued, whichever comes first. |
| `ackTimeoutMs` | 500 | How long QUORUM/STRONG will wait for peer ACKs before throwing. |
| `keepaliveIntervalMs` | 5000 | How often to send TCP keepalive pings. |
| `keepalivePongTimeoutMs` | 15000 | Disconnect a peer that does not respond within this window. |

---

## Consistency mode note

When using `attachBlock`, replication is triggered automatically on every `block.put()`
and `block.delete()`. For `EVENTUAL` mode this behaves exactly as expected.

For `QUORUM` and `STRONG` modes, if a replication timeout occurs the exception is absorbed
by the listener path — it does not propagate back to the caller of `block.put()`. If your
application needs to observe and react to replication failures (e.g. circuit-break or retry
logic), call `engine.replicate()` directly after performing the local write instead of
relying on `attachBlock` for those modes.
