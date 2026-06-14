# flair-cache-store

**Fast, typed, embedded in-memory cache with TTL, pluggable eviction, and reactive listener hooks.**

Part of the [FLAIR Cache](../README.md) library — but fully usable as a standalone module in any Java project that needs a local in-memory cache with zero external dependencies.

---

## Overview

`flair-cache-store` is the local store layer of FLAIR. It provides:

- A typed `CacheBlock<K, V>` with configurable TTL and eviction
- A raw `LocalStore` backed by `ConcurrentHashMap` for lock-free reads
- Background TTL sweep on a dedicated daemon thread
- Approximate O(1) eviction for LRU, LFU, and size-based policies
- Typed listener hooks (`PutListener`, `DeleteListener`, `ExpireListener`, `EvictListener`) so the replication and watch layers can observe every mutation
- Raw `putRaw` / `getRaw` access for the replication layer to bypass type codecs

Reads are designed to be sub-200ns: a pure `ConcurrentHashMap.get()` with no locks, no I/O, and no serialization on the hot path.

---

## Standalone use

This module has no dependency on any other FLAIR module except `flair-cache-serial` (for the `Codec<T>` type used to serialize keys and values) and `flair-cache-hlc` (for the `HybridLogicalClock` timestamp on each entry). It can be used independently in any Java project as a fast embedded local cache.

```xml
<dependency>
    <groupId>com.simplj.flair</groupId>
    <artifactId>flair-cache-store</artifactId>
    <version><!-- latest --></version>
</dependency>
```

---

## Quick start

```java
// Build a typed cache block
CacheBlock<String, Product> products = CacheBlock.<String, Product>builder()
    .name("products")
    .keyCodec(StringCodec.INSTANCE)
    .valueCodec(new ProductCodec())
    .ttl(Duration.ofMinutes(30))
    .eviction(EvictionPolicy.LRU)
    .maxEntries(10_000)
    .build();

// Write
products.put("p42", product);

// Read — pure ConcurrentHashMap lookup, no I/O
Product p = products.get("p42");   // null if missing or expired

// Check existence without updating access time
boolean exists = products.contains("p42");

// Delete
products.delete("p42");

// Always close to stop the background sweep thread
products.close();
// or: try (CacheBlock<...> b = ...) { ... }
```

---

## Core concepts

### `CacheBlock<K, V>`

The public API. Fully typed — keys and values are Java objects. Internally, everything is stored as `byte[]` so the underlying `LocalStore` has no type information.

`CacheBlock` is thread-safe. All methods may be called concurrently from any number of threads.

### `LocalStore`

The raw, untyped store. `CacheBlock` is a typed facade over it. `LocalStore` operates entirely in `byte[]` — it has no knowledge of codecs, types, or TTL policy. TTL expiry is computed by `CacheBlock` when entries are inserted and stored as an `expiryEpochMs` on the `CacheEntry`.

`LocalStore` is package-private; use `CacheBlock` from application code.

### `CacheEntry`

An immutable record holding a single entry's raw bytes and metadata:

```java
public record CacheEntry(
    byte[]       value,         // serialized value bytes
    HLCTimestamp hlc,           // HLC timestamp of the write (monotonic, cluster-wide)
    long         expiryEpochMs, // 0 = no expiry
    long         accessEpochMs, // epoch ms of last access (for LRU/LFU)
    long         hitCount,      // number of times accessed (for LFU)
    UUID         originNodeId   // null for local writes; set by replication layer
)
```

`CacheEntry` is intentionally small — it lives across millions of entries. No boxing, no collections.

### TTL and expiry

TTL is configured once per `CacheBlock` and applied uniformly to every `put()`. Two mechanisms enforce expiry:

**Lazy expiry (hot path):** `get()` and `contains()` check `isExpired()` on every call. If the entry has expired, it is atomically removed from the map and `onExpire` is fired before returning null. This is the primary mechanism for frequently accessed keys.

**Background sweep (memory reclaim):** The `flaircache-expiry-sweep` daemon thread runs every `sweepIntervalMs` (default 500ms) and removes expired entries that were never read again. This is the only mechanism that reclaims memory for keys that were written but never accessed after expiry.

If no TTL is set (`ttl(Duration.ZERO)` or omitted), the background sweep is never started.

---

## Builder reference

```java
CacheBlock.<K, V>builder()
    .name("my-block")             // logical name (default: "default")
    .keyCodec(Codec<K>)           // required — serializes keys to/from ByteBuffer
    .valueCodec(Codec<V>)         // required — serializes values to/from ByteBuffer
    .ttl(Duration.ofMinutes(10))  // TTL per entry (default: none / immortal)
    .eviction(EvictionPolicy.LRU) // eviction policy (default: NONE)
    .maxEntries(50_000)           // capacity limit; required when eviction != NONE
    .onEvict((k, v) -> ...)       // callback when an entry is evicted by capacity
    .sweepIntervalMs(500)         // background sweep interval in ms (default: 500)
    .hlc(sharedHlc)               // inject a shared HLC (for full FLAIR integration)
    .build();
```

> **Builder validation:** `keyCodec` and `valueCodec` are required — `build()` throws `NullPointerException` if either is missing. Setting `eviction` to anything other than `NONE` without also calling `maxEntries(N)` logs a `WARNING` — eviction will never fire if `maxEntries` is 0.

---

## Eviction policies

Eviction fires after every `put()` when `store.size() > maxEntries`. It uses **approximate sampling**: 5 random entries are evaluated, and the worst candidate is evicted. This keeps eviction O(1) regardless of store size.

| Policy | Evicts | Use when |
|---|---|---|
| `NONE` | Never | Unbounded cache; you control size externally |
| `LRU` | Entry with the oldest `accessEpochMs` | General-purpose; you want to keep hot data |
| `LFU` | Entry with the lowest `hitCount` | Long-lived stores; frequently accessed entries should survive |
| `SIZE_BASED` | Entry with the largest `value.length` | You care about memory bytes, not entry count |

**LRU/LFU access tracking:** `accessEpochMs` and `hitCount` are updated lazily on `get()` — only when `now - entry.accessEpochMs > 1000ms`. This caps the write rate to at most once per second per key under constant reads, preventing `compute()` contention on hot keys at the cost of 1-second coarse granularity in LRU/LFU scores. For most eviction use cases this is the correct trade-off.

---

## API reference

### Read path

```java
V   get(K key)         // null if missing or expired; increments hit/miss counter
boolean contains(K key)// lazy expiry; does NOT update access time or hit/miss counters
```

### Write path

```java
void put(K key, V value)          // write with block TTL; fires onPut, then maybeEvict
void putAll(Map<K, V> map)        // batch write; all entries share one expiry epoch
void delete(K key)                // remove; fires onDelete if key was present
void clear()                      // remove all entries; no listener notification
```

### Inspection

```java
Map<K, V>   snapshot()  // point-in-time copy of all non-expired entries; safe under concurrent writes
CacheStats  stats()     // current hit/miss/eviction/expiration counts and store size
String      name()      // logical name set at build time
```

### Raw access (for replication/bootstrap layers)

```java
void       putRaw(byte[] key, CacheEntry entry)  // bypass codec; advance HLC; fire onPut; no eviction
CacheEntry getRaw(byte[] key)                    // bypass codec and expiry check; null if key absent
```

`putRaw` is intended for the replication layer. It does not trigger eviction — bootstrap sync may push the store temporarily beyond `maxEntries`, and evicting during sync would corrupt the state transfer. The caller is responsible for flow control.

`getRaw` does not check `isExpired()`. The replication layer may need to read and re-propagate entries even after they have technically expired on the local node.

### Listener registration

Each event type has its own `@FunctionalInterface` and its own registration method. Register only the events you care about — unregistered event types incur zero overhead.

```java
void addPutListener(PutListener listener)       // fires on put() and putRaw()
void addDeleteListener(DeleteListener listener) // fires on delete() when key was present
void addExpireListener(ExpireListener listener) // fires on lazy expiry (get/contains) or sweep
void addEvictListener(EvictListener listener)   // fires on capacity eviction (LRU / LFU / SIZE_BASED)
```

---

## Listener types

| Interface | When fired |
|---|---|
| `PutListener` | After every `put()` or `putRaw()` |
| `DeleteListener` | After `delete()` when the key was present; silent deletes of absent keys do not fire |
| `ExpireListener` | On TTL expiry — either lazy (inside `get()` / `contains()`) or from the background sweep |
| `EvictListener` | After capacity eviction; always fires *after* `PutListener` for the same `put()` call |

**Contract (all listener types):**
- The `key` byte array is the live backing array of the internal map key. **Do not mutate it** — doing so corrupts future key equality checks in the store.
- `PutListener`, `DeleteListener`, and `EvictListener` are invoked on the calling thread. `ExpireListener` is invoked on `flaircache-expiry-sweep` when fired from the background sweep, or on the calling thread when fired from lazy expiry.
- **Do not block inside a callback.** The put caller and the sweep thread are blocked for the duration of all listener dispatches.
- A listener that throws does not prevent subsequent listeners from receiving the event. Each call is wrapped in an individual `try/catch` — exceptions are logged at `WARNING` level.
- Multiple listeners may be registered per event type. All receive events in registration order.

### Event ordering

When a `put()` triggers an eviction, the `PutListener` always fires before the `EvictListener`. The new entry is fully committed to the store before the eviction candidate is removed.

### Example: selective subscription

```java
// Only register for the events you actually need — no empty stubs required.

// Replication layer: replicate writes and deletes
products.addPutListener((key, entry) -> replication.enqueue(PUT, key, entry));
products.addDeleteListener((key, entry) -> replication.enqueue(DELETE, key, entry));

// Watch layer: notify subscribers
products.addPutListener((key, entry) -> watchRegistry.dispatch(PUT, key, entry));
products.addDeleteListener((key, entry) -> watchRegistry.dispatch(DELETE, key, entry));
products.addExpireListener((key, entry) -> watchRegistry.dispatch(EXPIRE, key, entry));

// Evict callback: user-facing notification
products.addEvictListener((key, entry) -> {
    String k = keyCodec.deserialize(ByteBuffer.wrap(key));
    onEvictedCallback.accept(k);
});
```

---

## Stats

```java
CacheStats s = products.stats();

s.hits()        // total successful get() calls
s.misses()      // total get() calls that returned null (absent + expired)
s.evictions()   // total entries removed by eviction policy
s.expirations() // total entries removed by TTL (lazy or sweep)
s.size()        // current number of entries in the store
s.hitRate()     // hits / (hits + misses) * 100.0 — 0.0 if no accesses yet
```

Counters are `LongAdder`-backed and safe for concurrent reads and writes. They are never reset — they reflect totals for the lifetime of the `CacheBlock`.

---

## Thread safety

| Operation | Thread safety |
|---|---|
| `get()` | Lock-free. Pure `ConcurrentHashMap.get()`. |
| `put()` / `delete()` | Lock-free map write. Brief `compute()` for LRU/LFU update (at most once per second per key). |
| `putAll()` | Same as `put()` per entry; no batch atomicity guarantee. |
| `snapshot()` | Weakly consistent point-in-time copy. Never throws `ConcurrentModificationException`. |
| `addListener()` | Safe at any time, including while sweep is running. |
| Listener dispatch | All listeners for a given event are called sequentially on the triggering thread. |
| TTL sweep | Runs on `flaircache-expiry-sweep`, a daemon thread. Uses conditional `remove(key, value)` to prevent double-expiration with concurrent lazy expiry. |
| `close()` | Safe to call exactly once from any thread. If called from within a listener callback, the sweep thread is interrupted but `awaitTermination()` is skipped to avoid a self-join deadlock. |

---

## Performance

| Operation | Design target (p99) | How |
|---|---|---|
| `get()` local hit | < 200ns | Pure `ConcurrentHashMap.get()`, no locks, no I/O |
| `put()` + async replication enqueue | < 500ns | Map write + `LongAdder` increment |
| Eviction per `put()` | O(1) | 5-entry random sample; bounded skip ≤ 16 |
| LRU/LFU update on hot key | ≤ 1 per second per key | Granularity guard: `now - accessEpochMs > 1000ms` |
| Thread-local serialization buffer | 0 allocations steady-state | Per-thread `ByteBuffer`, grown on demand, never shrunk |

**Hot path rules observed in this module:**
- No logging inside `get()` or `put()`.
- No `synchronized` — `StampedLock` is not needed here because `ConcurrentHashMap` provides the right guarantees for individual operations; cross-operation atomicity is handled via `compute()` and conditional `remove()`.
- `LongAdder` for all counters — never `AtomicLong`, which degrades under contention.
- No per-frame object allocation. Key and value serialization reuse a thread-local `ByteBuffer` that doubles in capacity on overflow.

---

## Internal design

```
CacheBlock<K,V>  (public typed facade)
│
│  serializeKey(K)  / serializeValue(V)    — thread-local ByteBuffer, zero alloc steady-state
│  deserializeValue(byte[])                — ByteBuffer.wrap(), no copy
│
▼
LocalStore  (package-private, untyped)
│
├── ConcurrentHashMap<ByteArrayKey, CacheEntry>   — the actual store
│     ByteArrayKey: byte[] wrapper with pre-computed Arrays.hashCode for fast map ops
│
├── CopyOnWriteArrayList<PutListener>             — one list per event type; each notify method
├── CopyOnWriteArrayList<DeleteListener>          —   iterates only the listeners registered for
├── CopyOnWriteArrayList<ExpireListener>          —   that specific event — no empty-impl overhead
├── CopyOnWriteArrayList<EvictListener>           —   safe under concurrent addXxxListener()
│
├── LongAdder × 4                                  — hits, misses, evictions, expirations
│
├── HybridLogicalClock                             — stamps every put(); advanced by putRaw()
│
└── maybeEvict()                                   — delegates to EvictionSampler.sample()
      EvictionSampler: random skip + fixed 5-entry window → O(1) victim selection

ExpiryManager  (package-private)
│
└── ScheduledExecutorService (flaircache-expiry-sweep, daemon)
      → LocalStore.sweepExpired(nowMs)
         conditional store.remove(key, entry) — exactly one of sweep or lazy expiry wins
```

---

## Thread naming

All threads in this module are daemon threads created by `FlairCacheThreadFactory`:

| Thread name | Role |
|---|---|
| `flaircache-expiry-sweep` | Background TTL sweep |

---

## Dependencies

| Dependency | Artifact | Why |
|---|---|---|
| `flair-cache-serial` | `com.simplj.flair:flair-cache-serial` | `Codec<T>` interface for key/value serialization |
| `flair-cache-hlc` | `com.simplj.flair:flair-cache-hlc` | `HybridLogicalClock` and `HLCTimestamp` on `CacheEntry` |

No other dependencies. No Guava, no Caffeine, no external libraries of any kind.

---

## What this module does not do

- **No replication.** Replication is handled by `flair-cache-replication`, which registers `PutListener` and `DeleteListener` on each block to observe mutations and fan them out over TCP.
- **No peer discovery.** That is `flair-cache-gossip` (SWIM/UDP).
- **No query DSL.** `flair-cache-dsl` accepts any `Map<K,V>` — it does not import store types.
- **No JMX metrics.** `flair-cache-metrics` wraps this module's `CacheStats` and publishes via MBeans.
- **No conflict resolution across nodes.** LWW conflict resolution using HLC timestamps is a replication concern, not a store concern. The store always applies the last write.
