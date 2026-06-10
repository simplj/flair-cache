# flair-cache-hlc

Zero-dependency Hybrid Logical Clock (HLC) for causal timestamps and Last-Write-Wins conflict resolution in distributed Java systems.

Combines physical wall-clock time with a logical counter to produce monotonically increasing timestamps that remain consistent across nodes despite NTP drift or clock skew.
Part of the [FlairCache](https://github.com/simplj/flair-cache) family, but fully usable as a standalone JAR in any distributed Java system that needs causally consistent timestamps without external dependencies.

---

## Contents

- [What this module does](#what-this-module-does)
- [Requirements](#requirements)
- [Installation](#installation)
- [Quick start](#quick-start)
- [Core concepts](#core-concepts)
  - [HLCTimestamp](#hlctimestamp)
  - [HybridLogicalClock](#hybridlogicalclock)
  - [LWWConflictResolver](#lwwconflictresolver)
  - [ClockDriftMonitor](#clockdriftmonitor)
- [How HLC works](#how-hlc-works)
- [Wire encoding](#wire-encoding)
- [Thread safety](#thread-safety)
- [JMX observability](#jmx-observability)
- [Package structure](#package-structure)

---

## What this module does

`flair-cache-hlc` gives you three things:

- **Causal timestamps** — `HybridLogicalClock` generates and receives `HLCTimestamp` values that are monotonically increasing even when wall clocks drift or jump.
- **Last-Write-Wins resolution** — `LWWConflictResolver` uses those timestamps to deterministically pick a winner when two nodes write the same key concurrently.
- **Clock drift monitoring** — `ClockDriftMonitor` tracks how far your wall clock has drifted from the logical clock and exposes that via JMX.

### Why use this instead of …

| Alternative | Why it doesn't fit |
|---|---|
| `System.currentTimeMillis()` | Wall clocks can go backward (NTP correction, leap seconds). Two nodes can produce the same millisecond value. No causality. |
| `System.nanoTime()` | Not comparable across JVM instances. No relation to wall time. |
| Lamport clocks | No connection to physical time — timestamps are opaque integers, not human-readable. |
| Vector clocks | O(n) space per event; expensive to compare for LWW. HLC is O(1). |

---

## Requirements

- **Java 11** or later (source is Java 21, target bytecode is Java 11)
- **Zero runtime dependencies** — JDK classes only

---

## Installation

### Maven

```xml
<dependency>
    <groupId>com.simplj.flair</groupId>
    <artifactId>flair-cache-hlc</artifactId>
    <version>0.1.0-SNAPSHOT</version>
</dependency>
```

### Gradle

```groovy
implementation 'com.simplj.flair:flair-cache-hlc:0.1.0-SNAPSHOT'
```

---

## Quick start

```java
// One HLC per node. Create it at startup and keep it for the lifetime of the process.
HybridLogicalClock hlc = new HybridLogicalClock();

// --- Sending / writing a local event ---
HLCTimestamp ts = hlc.now();
// Attach ts to your message or store entry before sending.

// --- Receiving a remote event ---
HLCTimestamp remoteTs = ...; // decoded from the incoming frame
hlc.update(remoteTs);        // advance the local clock; always call this before applying the event

// --- Comparing timestamps ---
if (incoming.isAfter(existing)) {
    // incoming wins
}

// --- LWW conflict resolution ---
UUID localNodeId  = UUID.randomUUID(); // stable per node
UUID remoteNodeId = ...; // decoded from the incoming frame

if (LWWConflictResolver.shouldReplace(existingTs, localNodeId, incomingTs, remoteNodeId)) {
    // apply the incoming value
}

// --- Wire encode / decode ---
byte[] encoded = ts.encodeToBytes();         // 16 bytes: logical(8) + counter(8)
HLCTimestamp decoded = HLCTimestamp.decode(encoded);

// --- Into an existing ByteBuffer (zero extra allocation) ---
ByteBuffer buf = ByteBuffer.allocate(256);
ts.encode(buf);                              // writes 16 bytes at current position
buf.flip();
HLCTimestamp ts2 = HLCTimestamp.decode(buf); // reads 16 bytes and advances position
```

---

## Core concepts

### HLCTimestamp

```java
public record HLCTimestamp(long logical, long counter) implements Comparable<HLCTimestamp> {}
```

An immutable value type representing a point in causal time.

| Field | Meaning |
|---|---|
| `logical` | Wall-clock milliseconds (or the highest observed logical time, whichever is greater) |
| `counter` | Tie-breaker: incremented when two events happen within the same logical millisecond |

Both fields are non-negative. The record compact constructor validates this at construction time.

**Comparison and ordering**

`HLCTimestamp` implements `Comparable<HLCTimestamp>`. Comparison is lexicographic: `logical` first, `counter` second.

```java
ts.compareTo(other)    // standard Comparable contract
ts.isAfter(other)      // compareTo(other) > 0
ts.isBefore(other)     // compareTo(other) < 0
```

**Encoding**

```java
// Into a ByteBuffer at the current write position — no allocation.
ts.encode(ByteBuffer buf)

// Returns a flipped ByteBuffer backed by a fresh 16-byte array.
ByteBuffer buf = ts.encode()

// Returns the raw 16-byte array directly.
byte[] bytes = ts.encodeToBytes()

// Decode from a ByteBuffer — reads and advances 16 bytes.
HLCTimestamp ts = HLCTimestamp.decode(ByteBuffer buf)

// Decode from a raw byte array.
HLCTimestamp ts = HLCTimestamp.decode(byte[] bytes)
```

`HLCTimestamp.BYTES = 16` — use this constant when sizing buffers.

---

### HybridLogicalClock

```java
public final class HybridLogicalClock {
    public synchronized HLCTimestamp now()
    public synchronized void update(HLCTimestamp remote)
    public ClockDriftMonitor driftMonitor()
}
```

One instance per node. Both methods are `synchronized` — the clock is safe to call from multiple threads, but the lock is on the instance. In high-throughput systems, keep clock calls off the critical path if possible.

**`now()` — generate a timestamp for a local event**

Call this immediately before you apply or send an event. It advances the clock and returns a new `HLCTimestamp`.

Rules applied:
- If wall time is ahead of the current logical time: reset `logical = wall`, `counter = 0`.
- Otherwise (same logical millisecond or clock went backward): keep `logical`, increment `counter`.

**`update(remote)` — advance the clock on receiving a remote event**

Call this immediately before applying an incoming event. Pass the timestamp decoded from the remote frame.

Rules applied (Kulkarni & Demirbas algorithm):
- `newLogical = max(local.logical, remote.logical, wall)`
- Counter resolution:
  - If all three agree on `newLogical`: `counter = max(local.counter, remote.counter) + 1`
  - If only `local.logical == newLogical`: `counter = local.counter + 1`
  - If only `remote.logical == newLogical`: `counter = remote.counter + 1`
  - If wall time is ahead of both: `counter = 0`

**Counter overflow**

If `counter` reaches `Long.MAX_VALUE` within a single logical millisecond (more events than can fit), `now()` throws `IllegalStateException`. This is a safety guard; in practice it is unreachable.

**Custom wall clock (testing)**

The package-private constructor accepts a `LongSupplier` for the wall clock and a `ClockDriftMonitor` instance, making it straightforward to inject a controlled clock in tests:

```java
// In tests — inject a controllable wall clock
long[] fakeWall = {1_000L};
HybridLogicalClock hlc = new HybridLogicalClock(() -> fakeWall[0], new ClockDriftMonitor());

HLCTimestamp t1 = hlc.now();  // logical=1000, counter=0
HLCTimestamp t2 = hlc.now();  // logical=1000, counter=1 (same ms)

fakeWall[0] = 1_001L;
HLCTimestamp t3 = hlc.now();  // logical=1001, counter=0 (wall advanced)
```

---

### LWWConflictResolver

```java
public final class LWWConflictResolver {
    public static boolean shouldReplace(
        HLCTimestamp existingTs, UUID existingNode,
        HLCTimestamp incomingTs, UUID incomingNode)
}
```

A pure utility class for Last-Write-Wins conflict resolution. Stateless — no instantiation needed.

**Decision rules (in order)**

1. `incomingTs` is strictly after `existingTs` → **replace** (`true`)
2. `incomingTs` is strictly before `existingTs` → **keep existing** (`false`)
3. Timestamps are identical → **lexicographically higher node UUID wins** (deterministic tiebreak, no coordination required)

The UUID tiebreak ensures that two nodes which independently apply the same conflicting pair of updates converge to the same winner, without any communication.

```java
boolean replace = LWWConflictResolver.shouldReplace(
    entry.hlc(), entry.nodeId(),
    incoming.hlc(), incoming.nodeId()
);
if (replace) {
    store.put(key, incoming);
}
```

---

### ClockDriftMonitor

Tracks the gap between your wall clock and the HLC's logical time. It is created automatically by `HybridLogicalClock` and is checked on every `now()` and `update()` call.

**Drift types tracked**

- **Local drift** (`checkDrift`): how far the wall clock has fallen behind the logical clock — indicates your system time was briefly in the future or the clock advanced slowly.
- **Remote drift** (`checkRemoteDrift`): how far ahead a remote node's logical time is relative to your wall clock — indicates a peer node is running with a significantly faster or unsynchronized clock.

**Thresholds and alerts**

The default threshold is **60 seconds** (`DEFAULT_MAX_DRIFT_MS`). When drift exceeds the threshold a `WARNING` is logged via JUL and the alert counter is incremented.

```java
ClockDriftMonitor monitor = hlc.driftMonitor();

long maxDrift = monitor.maxObservedDriftMs();   // highest single drift seen, in ms
long alerts   = monitor.alertCount();           // number of times threshold was exceeded
long threshold = monitor.maxDriftThresholdMs(); // configured threshold in ms
```

**Custom threshold**

```java
ClockDriftMonitor monitor = new ClockDriftMonitor(30_000L); // alert at 30s
HybridLogicalClock hlc = new HybridLogicalClock(System::currentTimeMillis, monitor);
```

---

## How HLC works

Wall clocks in distributed systems are unreliable: NTP can step the clock backward, two machines in the same datacenter can disagree by tens of milliseconds, and leap seconds cause real-world surprises.

**Lamport clocks** solve ordering but abandon physical time entirely. **HLC** gives you both:

```
Event on Node A           Event on Node B
wall=1000, log=1000       wall=1001, log=1001
  → ts = (1000, 0)          → ts = (1001, 0)

Two events on Node A in the same ms:
  → ts1 = (1000, 0)
  → ts2 = (1000, 1)   ← counter increments, not logical

Node A receives (1001, 0) from Node B:
  wall=999 (clock drifted back), log=1000
  newLogical = max(1000, 1001, 999) = 1001
  remote.logical wins → counter = remote.counter + 1 = 1
  → ts = (1001, 1)    ← now causally after the remote event
```

The result: timestamps are always monotonically increasing on each node, they reflect physical time closely enough to be human-readable and to detect anomalies, and they track causality correctly across nodes regardless of clock drift.

---

## Wire encoding

`HLCTimestamp` encodes to exactly **16 bytes**, big-endian.

```
┌──────────────────────────┬──────────────────────────┐
│   logical  (8B int64)    │   counter  (8B int64)    │
└──────────────────────────┴──────────────────────────┘
 0                        7 8                       15
```

Both fields are signed 64-bit big-endian integers. Negative values are rejected at construction time, so the sign bit is always 0 in practice.

In the FlairCache wire protocol, the HLC timestamp occupies the first 16 bytes of every `PUT` and `DELETE` payload:

```
PUT payload:  hlc(16) + blockLen(2) + block + keyLen(2) + key + valLen(4) + val
DELETE payload: hlc(16) + blockLen(2) + block + keyLen(2) + key
```

---

## Thread safety

| Component | Thread safety |
|---|---|
| `HLCTimestamp` | Immutable record. Safe to share across threads. |
| `HybridLogicalClock` | `now()` and `update()` are `synchronized`. Safe from any thread. |
| `LWWConflictResolver` | Stateless static utility. Safe from any thread. |
| `ClockDriftMonitor` | `maxObservedDriftMs` uses `AtomicLong`; `alertCount` uses `LongAdder`. Read methods are lock-free. |

The `synchronized` on `HybridLogicalClock` means that under extreme throughput, clock calls can become a contention point. If you have multiple writer threads, consider sharding into multiple `HybridLogicalClock` instances — one per writer thread — and merging the maximum timestamp when needed.

---

## JMX observability

Call `ClockDriftMonitor.registerJmx(nodeId)` during node startup to expose drift metrics via JMX.

```java
hlc.driftMonitor().registerJmx("node-1");
```

This registers an MBean under:

```
com.simplj.flair.cache:type=ClockDriftMonitor,node="node-1"
```

| Attribute | Type | Description |
|---|---|---|
| `MaxObservedDriftMs` | `long` | Highest single drift value observed, in milliseconds |
| `DriftAlertThresholdMs` | `long` | Configured threshold; drift above this triggers a `WARNING` log and increments the alert counter |
| `DriftAlertCount` | `long` | Number of times drift exceeded the threshold |

If the MBean name is already registered (e.g., duplicate startup), registration is silently skipped.

---

## Package structure

```
com.simplj.flair.cache.hlc
│
├── HLCTimestamp          Immutable record — logical(long) + counter(long), 16-byte wire encoding
├── HybridLogicalClock    Clock singleton — now() + update(remote); synchronized
├── LWWConflictResolver   Static LWW utility — shouldReplace(existingTs, existingNode, incomingTs, incomingNode)
└── ClockDriftMonitor     Drift tracking — thresholds, JUL warnings, JMX MBean
```

---

*Part of [FlairCache](https://github.com/simplj/flair-cache) — Fast Local Access with In-memory Replication.*
*Apache License 2.0.*
