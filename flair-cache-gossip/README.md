# flair-cache-gossip

**SWIM gossip — peer discovery and failure detection over UDP. Zero dependencies.**

Part of the [FLAIR Cache](../README.md) project, but fully standalone. You can embed this module in any Java project that needs cluster membership, liveness tracking, and failure detection — no Consul, no Zookeeper, no external process, nothing outside the JDK.

---

## What it does

- Maintains a live view of which nodes are in a cluster and whether each one is reachable
- Detects node failures within seconds using direct probing + indirect probing (no false positives from transient packet loss)
- Propagates membership changes to every node in the cluster via epidemic dissemination — piggybacked on every PING and PONG
- Notifies your application instantly via `MembershipListener` callbacks: `onJoin`, `onSuspect`, `onRecover`, `onLeave`, `onDead`
- Handles graceful leaves (node broadcasts LEAVE before shutdown) and hard crashes (timeout-based detection)

**What it does not do:**

Gossip handles membership only. It does not replicate data, maintain consensus, or make any guarantees about total ordering of events. Data replication over TCP is `flair-cache-replication`'s job — this module only answers "which nodes are alive?"

---

## Maven

```xml
<dependency>
    <groupId>com.simplj.flair</groupId>
    <artifactId>flair-cache-gossip</artifactId>
    <version>${flair.version}</version>
</dependency>
```

Zero transitive dependencies. JDK 11+ only.

---

## Quick start

### Single node (seed)

```java
GossipNode node = GossipNode.builder()
    .bindAddress("10.0.0.1")
    .bindPort(7891)
    .seedPeers(List.of())          // no seeds — this node is the first
    .listener(new MembershipListener() {
        public void onJoin(NodeInfo n)    { System.out.println("joined:    " + n.addressString()); }
        public void onSuspect(NodeInfo n) { System.out.println("suspected: " + n.addressString()); }
        public void onRecover(NodeInfo n) { System.out.println("recovered: " + n.addressString()); }
        public void onLeave(NodeInfo n)   { System.out.println("left:      " + n.addressString()); }
        public void onDead(NodeInfo n)    { System.out.println("dead:      " + n.addressString()); }
    })
    .build();

node.start();
```

### Joining an existing cluster

```java
GossipNode node = GossipNode.builder()
    .bindAddress("10.0.0.2")
    .bindPort(7891)
    .seedPeers(List.of("10.0.0.1:7891"))  // one or more known peers
    .build();

node.start();
```

At startup the node sends a JOIN to each seed. The seed replies with its full membership snapshot (JOIN_ACK), and from there epidemic dissemination spreads the new node's existence to the rest of the cluster.

### Reading the current membership

```java
// All ALIVE nodes (snapshot — lock-free)
List<NodeInfo> alive = node.members().alive();

// All SUSPECTED nodes
List<NodeInfo> suspected = node.members().suspected();

// Look up a specific node by UUID
Optional<NodeInfo> info = node.members().find(someNodeId);
```

### Graceful shutdown

```java
node.shutdown();   // broadcasts LEAVE to all live peers, then stops
```

Peers receive the LEAVE and immediately remove the node from their membership list without waiting for the failure detection timeout.

---

## How it works: SWIM in 90 seconds

SWIM (Scalable Weakly-consistent Infection-style Membership) is the protocol underlying most production service meshes and distributed databases (Consul, Cassandra, Serf). The key ideas:

### 1. Periodic probing

Every `tickIntervalMs` (default 500 ms), each node picks `fanout` (default 3) random live peers and sends each a PING over UDP.

```
A ──PING──► B
A ◄──PONG── B    ← B is alive
```

### 2. Indirect probing

If no PONG arrives within `probeTimeoutMs` (default 2 s), the node does **not** immediately declare the target dead. Instead it asks 2 random other nodes to probe it indirectly (PING_REQ). This distinguishes a slow target from a broken network path between the two endpoints.

```
A ──PING──► B          (no PONG after 2s)
A ──PING_REQ──► C
              C ──PING──► B
              C ◄──PONG── B    ← B replied to C, just not to A
C ──PONG(B)──► A        ← A clears suspicion
```

If no indirect PONG arrives within `indirectTimeoutMs` (default 4 s from when PING_REQ was sent), the target is marked **SUSPECTED**.

### 3. Suspicion and death

A SUSPECTED node has `suspicionTimeoutMs` (default 10 s) to **refute**. If the node is actually alive it will receive a PING carrying the SUSPECTED delta, see itself labeled suspect, increment its incarnation number, and immediately broadcast ALIVE — cancelling the suspicion on all peers.

```
A sends PING to B piggybacking SUSPECTED(B, inc=3)
B receives it → sees self is SUSPECTED → incarnation.increment() → 4
B piggybacks ALIVE(B, inc=4) onto its PONG
A receives PONG + ALIVE(B, inc=4) → clears suspicion → onRecover fires
```

If no refutation arrives within the suspicion window, the node is declared **DEAD** and removed from the membership list.

### 4. Epidemic dissemination (piggybacking)

Every PING and PONG carries recent membership deltas in its payload — up to `MAX_PIGGYBACKED` entries (≈ 31 for IPv4, 29 for IPv6, tuned to fit within one UDP MTU). Each delta is transmitted `floor(log₂(N))` times across the cluster, where N is the cluster size. This ensures that every node learns about every membership event within a small number of gossip rounds without any dedicated broadcast mechanism.

### 5. Incarnation numbers

Each node maintains a monotonically increasing **incarnation number**. A higher incarnation always wins. This is the mechanism that allows a node to refute SUSPECTED (increment and broadcast ALIVE) and that prevents stale membership state from overwriting fresher state.

---

## Failure detection timing

The table below shows the default timeline from crash to DEAD declaration for a 3-node cluster, and how to tune it.

| Event | Default time since crash | Parameter |
|---|---|---|
| PING sent to target | 0 – 500 ms | `tickIntervalMs` |
| PING timeout → PING_REQ sent | + 2 000 ms | `probeTimeoutMs` |
| PING_REQ timeout → SUSPECTED | + 4 000 ms | `indirectTimeoutMs` |
| Suspicion window expires → DEAD | + 10 000 ms | `suspicionTimeoutMs` |
| **Total worst-case** | **≈ 16.5 s** | |

For a tighter cluster (e.g. internal LAN with reliable networking), aggressive settings are reasonable:

```java
GossipNode.builder()
    .tickIntervalMs(200)
    .probeTimeoutMs(500)
    .indirectTimeoutMs(1000)
    .suspicionTimeoutMs(3000)
    ...
```

For a WAN cluster or a highly loaded environment where occasional packet loss is normal, use the defaults or increase them.

---

## API reference

### `GossipNode`

```java
GossipNode node = GossipNode.builder()
    .nodeId(UUID.randomUUID())       // default — generated automatically
    .bindAddress("0.0.0.0")          // default
    .bindPort(7891)                  // required — UDP port to bind
    .seedPeers(List.of("h:p", ...))  // "host:port" peers to contact on startup
    .tickIntervalMs(500)             // how often to run one probe round (default 500ms)
    .probeTimeoutMs(2000)            // direct PING timeout before PING_REQ (default 2s)
    .indirectTimeoutMs(4000)         // indirect PING_REQ timeout before SUSPECTED (default 4s)
    .suspicionTimeoutMs(10000)       // suspicion window before DEAD (default 10s)
    .fanout(3)                       // peers to probe per tick (default 3)
    .listener(membershipListener)    // optional — receives membership events
    .build();                        // throws IOException if address parse fails

node.start();                        // binds UDP socket, starts recv + tick threads
node.shutdown();                     // broadcasts LEAVE, stops threads
node.members();                      // MembershipList — live snapshot access
node.localPort();                    // actual bound port (useful when bindPort=0)
```

`start()` throws `IOException` if the UDP socket cannot bind. `shutdown()` is idempotent.

---

### `MembershipListener`

Implement this interface to receive real-time membership events. All callbacks are delivered on the `flaircache-gossip-recv` thread — keep them short or hand off to your own executor.

```java
public interface MembershipListener {
    /** A new node has joined and been accepted into the membership list. */
    void onJoin(NodeInfo node);

    /** A node has been marked SUSPECTED after failing its probe window. */
    void onSuspect(NodeInfo node);

    /** A SUSPECTED node successfully refuted — it is alive again. */
    void onRecover(NodeInfo node);

    /** A node sent a graceful LEAVE before shutting down. */
    void onLeave(NodeInfo node);

    /** A SUSPECTED node failed to refute within the suspicion window. */
    void onDead(NodeInfo node);
}
```

`onRecover` fires on both paths: a node that refuted via SWIM piggybacking, and a node that sent a JOIN after being SUSPECTED (e.g. after a restart). In both cases the `NodeInfo` passed to `onRecover` carries the current address and incarnation.

---

### `MembershipList`

The live view of the cluster. All read methods are lock-free — safe to call from any thread at any time.

```java
MembershipList ml = node.members();

Optional<NodeInfo> n  = ml.find(uuid);     // O(1) — ConcurrentHashMap lookup
List<NodeInfo> alive  = ml.alive();        // snapshot of ALIVE members only
List<NodeInfo> susp   = ml.suspected();    // snapshot of SUSPECTED members only
List<NodeInfo> all    = ml.all();          // snapshot of all tracked members
int size              = ml.size();         // total tracked count (ALIVE + SUSPECTED)
```

Snapshots are point-in-time and immutable. Calling `alive()` twice may return different results if a membership change happened in between.

---

### `NodeInfo`

Immutable record representing one cluster member.

```java
record NodeInfo(
    UUID        id,               // stable node identity — survives address changes
    InetAddress address,          // current bind address
    int         port,             // current UDP port
    NodeStatus  status,           // ALIVE, SUSPECTED, or DEAD
    long        incarnation,      // monotonically increasing — higher always wins
    long        lastSeenEpochMs   // wall-clock time of last confirmed liveness
)
```

```java
node.addressString()  // "host:port" — convenience for logging
```

---

### `NodeStatus`

```java
enum NodeStatus {
    ALIVE,      // responding to probes; in the live membership list
    SUSPECTED,  // missed probe window; still in the list; may recover
    DEAD        // suspicion window expired; removed from list; tombstoned
}
```

DEAD nodes are never in `MembershipList.alive()` or `MembershipList.suspected()`. A dead node that restarts re-enters as ALIVE after a new JOIN.

---

## UDP packet format

All gossip messages are sent as single UDP datagrams. Packets are capped at **1400 bytes** (safely below the standard 1500-byte Ethernet MTU — no IP fragmentation).

```
┌─ TYPE(1) ─┬─ SENDER_UUID(16) ─┬─ INCARNATION(8) ─┬─[TARGET_UUID(16)]─┬─ NUM_DELTAS(2) ─┬─ DELTAS ─┐
│  see below │  sender node ID   │  sender's clock  │  PING_REQ only    │  0..MAX (~31)   │  see below│
└────────────┴──────────────────┴──────────────────┴───────────────────┴─────────────────┴──────────┘
```

**Message types:**

| Byte | Type | Description |
|---|---|---|
| `0x01` | `PING` | Liveness probe. Carries piggybacked membership deltas. |
| `0x02` | `PONG` | Liveness acknowledgment. Carries piggybacked membership deltas. |
| `0x03` | `PING_REQ` | Indirect probe request. Carries target UUID + piggybacked deltas. |
| `0x04` | `JOIN` | Node startup announcement sent to seeds. |
| `0x05` | `JOIN_ACK` | Seed reply carrying full membership snapshot. |
| `0x06` | `LEAVE` | Graceful departure announcement. |

**Delta encoding** (one per piggybacked membership entry):

```
nodeId(16) + status(1) + incarnation(8) + addrLen(1: 4=IPv4 or 16=IPv6) + addr(4 or 16) + port(2)
```

All integers are **big-endian**. UUIDs are 16 bytes (MSB 8 bytes + LSB 8 bytes).

---

## Thread model

| Thread name | Count | Role |
|---|---|---|
| `flaircache-gossip-recv` | 1 | UDP receive loop — decodes packets, drives all message handlers |
| `flaircache-gossip-tick` | 1 | Periodic tick — sends PINGs, checks probe timeouts, checks suspicion timeouts |

Both are daemon threads created by `FlairCacheThreadFactory`. All state mutations (membership list, probe state, suspicion timers) are driven from these two threads. The recv thread handles all inbound messages; the tick thread drives all outbound probes and timeout transitions.

`MembershipListener` callbacks are always delivered on `flaircache-gossip-recv`. Keep callbacks non-blocking or dispatch to your own thread pool.

---

## Configuration reference

All parameters are set on `GossipNode.builder()`. There is no separate `GossipConfig` object in the public API.

| Parameter | Default | Description |
|---|---|---|
| `bindAddress` | `"0.0.0.0"` | Local address to bind the UDP socket |
| `bindPort` | `7891` | Local UDP port. Use `0` for OS-assigned. |
| `seedPeers` | `[]` | `"host:port"` list of peers to contact on startup. Can be a subset of the cluster — epidemic dissemination handles the rest. |
| `tickIntervalMs` | `500` | Probe round interval in milliseconds |
| `probeTimeoutMs` | `2000` | How long to wait for a direct PONG before issuing indirect probes |
| `indirectTimeoutMs` | `4000` | How long to wait for an indirect PONG before marking SUSPECTED |
| `suspicionTimeoutMs` | `10000` | How long a SUSPECTED node has to refute before being declared DEAD |
| `fanout` | `3` | Number of peers probed per tick. Bounded by live cluster size − 1. |
| `nodeId` | `UUID.randomUUID()` | Stable identity for this node. Override if you persist node identity across restarts. |

---

## Tombstones and restarts

When a node is declared DEAD, the cluster records a **tombstone** — the node's UUID mapped to its last known incarnation. Tombstones block a stale ALIVE delta from the dead node's previous life from ghost-resurrecting it in the membership list.

When a restarted node sends JOIN, `handleJoin` checks the tombstone and automatically bumps the joiner's effective incarnation to `tombstone + 1` if necessary. This means:

- A node that crashes and restarts (even at incarnation 0) can always rejoin the cluster cleanly
- Stale `DEAD` gossip from the previous life cannot kill the restarted node because the old DEAD incarnation is strictly lower than the new effective incarnation

No manual intervention is needed. Restarts are transparent to the application.

---

## Internal components

These types are package-private. They are implementation details, not part of the public API.

| Type | Role |
|---|---|
| `GossipReceiver` | UDP receive loop; decodes packets and dispatches to `GossipNode.onMessage()` |
| `GossipTick` | Scheduled executor that calls `GossipNode.onTick()` every `tickIntervalMs` |
| `FailureDetector` | Tracks per-peer probe state: PROBING → INDIRECT_PROBING → SUSPECTED |
| `PiggybackQueue` | Priority queue of pending membership deltas; evicts after `floor(log₂(N))` transmits |
| `IncarnationClock` | Monotonically increasing counter; thread-safe increment on refutation |
| `GossipProtocol` | Stateless encode/decode for UDP packets |
| `GossipConfig` | Immutable snapshot of all timing and topology parameters |
| `FlairCacheThreadFactory` | Creates named daemon threads |

---

## Design constraints

- **UDP only. No TCP.** Gossip uses `DatagramSocket` / `DatagramPacket`. TCP is used by `flair-cache-transport` for data replication — two separate concerns using the right transport for each.
- **Membership only. No data.** This module tracks which nodes are alive. It does not know about cache blocks, keys, values, or replication. That separation is intentional — `flair-cache-gossip` is independently useful in any distributed Java application, not just in FLAIR Cache.
- **No dependency on other FLAIR modules.** This module has zero compile-time dependencies on any other FLAIR artifact. You can add it to any Java project.
- **No external dependencies.** `DatagramSocket`, `ConcurrentHashMap`, `ScheduledExecutorService` — pure JDK.
- **Packets always fit in one datagram.** The `MAX_PIGGYBACKED` constant is computed so that a fully-packed packet never exceeds 1400 bytes. No fragmentation, no reassembly.

---

## License

Apache License, Version 2.0. See [LICENSE](../LICENSE).
