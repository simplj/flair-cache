# flair-cache-watch

**Typed, key-filtered, in-process pub/sub for data change events (PUT, DELETE, EXPIRE).**

Part of the [FLAIR Cache](../README.md) library — but fully usable as a standalone module in any Java project that needs typed asynchronous event notification over an in-memory data store. Zero external dependencies.

---

## Overview

`flair-cache-watch` is a lightweight event-dispatch engine that lets you subscribe to mutations in a data store. When a key is written, deleted, or expires, matching subscribers are notified.

- Subscribe to PUT, DELETE, and EXPIRE events — independently or in any combination
- Filter by key predicate before any value deserialization
- Async dispatch (default) keeps the write path at < 500ns regardless of subscriber count
- Sync dispatch for latency-critical callbacks that complete in < 1µs
- One-shot subscriptions that auto-cancel after the first matching event
- Monitor replication lag for replicated events with optional threshold alerting
- Cancellation and lifecycle management via `WatchHandle`

The registry is standalone — it has no dependency on any FLAIR store type. It accepts any `ChangeEvent` you construct and dispatches to matching subscribers. `CacheBlock` wires it in at a higher layer.

---

## Standalone use

This module has zero dependencies on other FLAIR modules. Use it independently in any Java project that needs typed in-process event notification.

```xml
<dependency>
    <groupId>com.simplj.flair</groupId>
    <artifactId>flair-cache-watch</artifactId>
    <version><!-- latest --></version>
</dependency>
```

---

## Quick start

```java
// One registry per data store (or per logical scope)
WatchRegistry<String, Product> registry = new WatchRegistry<>();

// Subscribe to PUT events — runs on a dedicated async thread
WatchHandle handle = registry.watch()
    .onPut((key, product) -> rebuildIndex(key, product))
    .register();

// Publish an event from your store's write path
registry.dispatch(new ChangeEvent.PutEvent<>(key, newProduct, oldProduct, ChangeEvent.Source.LOCAL));

// Cancel when done
handle.cancel();

// Shut down the registry (cancels all subscriptions)
registry.shutdown();
```

---

## Core concepts

### `WatchRegistry<K, V>`

The central dispatch hub. Create one registry per data store or logical scope. Internally, the subscriber list is a `CopyOnWriteArrayList` — reads (dispatch) vastly outnumber writes (register/cancel), making this the correct trade-off.

```java
WatchRegistry<String, Order> orders = new WatchRegistry<>();
```

`dispatch(event)` returns immediately. All work beyond the key filter check and a non-blocking queue offer is handed off to per-subscription drain threads.

### `WatchAPI<K, V>` — fluent builder

Obtain a builder from the registry, configure your callbacks, then call `register()` or `once()`:

```java
WatchHandle handle = registry.watch()
    .onPut((k, v)  -> ...)    // PUT callback
    .onDelete(k    -> ...)    // explicit DELETE callback
    .onExpire(k    -> ...)    // TTL expiry callback
    .onEvent(event -> ...)    // raw event — access oldValue, source, lastValue
    .filter(k      -> ...)    // key predicate — tested before value deserialization
    .async(true)              // true = dedicated thread (default); false = calling thread
    .register();              // persistent; or .once() for one-shot
```

Any combination of callbacks is valid. At least one callback must be registered — `register()` throws `IllegalStateException` if none are set.

Multiple `filter()` calls on the same builder compose with AND semantics: a key must pass all predicates to reach the subscriber.

### `WatchHandle`

The token returned by `register()` and `once()`. Use it to inspect state and cancel the subscription.

```java
handle.subscriptionId()          // unique ID assigned at registration
handle.isActive()                // true until cancel() or once() fires
handle.eventsReceived()          // events successfully delivered to the listener
handle.eventsDropped()           // events dropped because the async queue was full
handle.eventsSkipped()           // events that passed the key filter but had no matching typed listener
handle.cancel()                  // idempotent; safe to call multiple times
handle.awaitDone(3, SECONDS)     // block until the drain thread exits (async) or subscription is inactive (sync)
```

`cancel()` is idempotent. For async subscriptions, events already queued at cancel time may still be delivered before the drain thread exits — call `awaitDone()` for a hard quiescence guarantee.

### `ChangeEvent<K, V>`

A sealed interface with three record subtypes:

```java
// Key was written (local or replicated)
ChangeEvent.PutEvent<K, V>(K key, V newValue, V oldValue, Source source)

// Key was explicitly deleted
ChangeEvent.DeleteEvent<K, V>(K key, V lastValue, Source source)

// Key expired by TTL — source is always LOCAL
ChangeEvent.ExpireEvent<K, V>(K key, V lastValue)
```

`Source.LOCAL` means the write originated on this node. `Source.REPLICATED` means it arrived from a peer over TCP. TTL expiry is always `LOCAL` — it is driven by the local expiry sweep and never replicated directly.

Pattern-match in your listener with `instanceof`:

```java
.onEvent(event -> {
    if (event instanceof ChangeEvent.PutEvent<String, Order> put) {
        // put.key(), put.newValue(), put.oldValue(), put.source()
    } else if (event instanceof ChangeEvent.DeleteEvent<String, Order> del) {
        // del.key(), del.lastValue(), del.source()
    } else if (event instanceof ChangeEvent.ExpireEvent<String, Order> exp) {
        // exp.key(), exp.lastValue()
    }
})
```

---

## Key filtering

Register a `WatchFilter<K>` to restrict dispatch to keys that match a predicate. The predicate runs before value deserialization — non-matching keys cost only a predicate check:

```java
registry.watch()
    .onPut((k, v) -> updateRegionIndex(k, v))
    .filter(k -> k.startsWith("region:eu"))
    .register();
```

Chain multiple filters on the same builder — they compose with AND semantics:

```java
registry.watch()
    .onPut((k, v) -> ...)
    .filter(k -> k.startsWith("eu:"))
    .filter(k -> k.endsWith(":prod"))
    .register();
// key must match both predicates
```

Key-filter rejections are silent returns — they do not count as `eventsSkipped`. Only events that pass the key filter but have no matching typed callback (e.g., a DELETE reaching a PUT-only subscriber) increment `eventsSkipped`.

---

## Async vs sync dispatch

### Async (default — `async(true)`)

Each subscription gets a bounded `ArrayBlockingQueue` (capacity 1024) and a dedicated drain thread named `flaircache-watch-{subscriptionId}`. The write path performs only a filter check and a non-blocking `offer()` — it never waits for the listener to complete.

**When the queue is full:** the oldest event in the queue is evicted and a `WARNING` is logged. The new event is inserted in its place. `handle.eventsDropped()` is incremented for every evicted event. This is the correct behaviour for slow listeners — write throughput is never sacrificed for a subscriber that cannot keep up.

**Listener exception isolation:** an exception thrown inside an async listener is caught and logged at `WARNING` level. The drain thread survives and continues delivering subsequent events.

### Sync (`async(false)`)

The listener runs directly on the thread that called `dispatch()`, inline with the write path. Use only for callbacks that are guaranteed to complete in < 1µs (counter increments, flag sets). Blocking inside a sync listener stalls the write path.

```java
registry.watch()
    .onPut((k, v) -> counter.increment())  // must complete in < 1µs
    .async(false)
    .register();
```

**Exception isolation (sync):** an exception in a sync listener is caught by the registry's dispatch loop and logged at `WARNING`. Other subscriptions in the same dispatch loop still receive the event.

---

## One-shot subscriptions

Call `.once()` instead of `.register()` to auto-cancel after the first matching event:

```java
WatchHandle handle = registry.watch()
    .onPut((k, v) -> System.out.println("first write: " + k))
    .once();

// After the first PUT is delivered:
// handle.isActive() == false
// The subscription is removed from the registry automatically
```

**Correctness guarantee:** a `once()` subscription is not consumed by an event type it has no listener for. An `onPut().once()` subscription that receives a DELETE event ignores it and stays active — it will fire only when the first PUT arrives. The skipped event is counted in `handle.eventsSkipped()`.

---

## Replication lag monitoring

For replicated events, pass the origin node's write timestamp to `dispatch()`. The registry computes `lag = now - sourceTimestampMs` and forwards it to any registered lag callback on that subscription.

```java
// Dispatch a replicated event with its origin wall-clock timestamp
registry.dispatch(event, hlcTimestamp.logical());  // logical() is epoch ms at write time
```

Register a lag callback on a subscription:

```java
// Fires for every replicated event, regardless of lag
registry.watch()
    .onPut((k, v) -> ...)
    .onReplicationLag(lagMs -> metrics.record(lagMs))
    .register();

// Fires only when lag exceeds the threshold
registry.watch()
    .onPut((k, v) -> ...)
    .onReplicationLag(Duration.ofMillis(500), lagMs -> alerts.warnHighLag(lagMs))
    .register();
```

**Threading:** the lag callback always runs synchronously on the thread that called `dispatch()` — even when `async(true)` is set. Keep the lag callback O(1) and non-blocking (target < 1µs).

Local events dispatched without a source timestamp (via `dispatch(event)`) never trigger the lag callback.

**Lag-only subscriptions** (no typed listener, only `onReplicationLag`) are valid and pass the at-least-one-listener validation check.

---

## Multiple event types on one subscription

A single subscription can listen to any combination of PUT, DELETE, EXPIRE, and raw events. Register as many callbacks as needed — each fires independently:

```java
registry.watch()
    .onPut((k, v) -> puts.incrementAndGet())
    .onDelete(k   -> deletes.incrementAndGet())
    .onExpire(k   -> expires.incrementAndGet())
    .register();
```

The `onEvent(Consumer<ChangeEvent<K,V>>)` callback receives all three event types and fires in addition to any typed callback registered on the same subscription:

```java
registry.watch()
    .onPut((k, v)  -> handlePut(k, v))       // fires for PutEvent only
    .onEvent(event -> audit(event))           // fires for ALL event types, including PUT
    .register();
// For a PUT: both onPut and onEvent fire
```

---

## Multiple subscriptions

Multiple subscriptions on the same registry all receive the same event independently. A failure in one subscription (filter exception, listener exception, queue full) never affects delivery to sibling subscriptions:

```java
WatchHandle h1 = registry.watch().onPut((k, v) -> cacheA.invalidate(k)).register();
WatchHandle h2 = registry.watch().onPut((k, v) -> cacheB.invalidate(k)).register();
// Both h1 and h2 receive every PUT event
```

---

## Lifecycle

### Cancelling a single subscription

```java
WatchHandle handle = registry.watch().onPut((k, v) -> ...).register();
// ...
handle.cancel();  // idempotent
```

For async subscriptions, call `handle.awaitDone(timeout, unit)` to wait for the drain thread to finish processing any queued events before proceeding.

### Shutting down the registry

```java
registry.shutdown();
// Cancels all subscriptions, stops all drain threads, clears the subscriber list.
// dispatch() becomes a no-op after shutdown.
// Calling register() or once() after shutdown throws IllegalStateException.
// shutdown() is idempotent.
```

---

## Wiring into a data store

`WatchRegistry` is a standalone event bus. Wire it into any data store by calling `dispatch()` from the store's mutation hooks:

```java
WatchRegistry<String, Order> watchRegistry = new WatchRegistry<>();

// In your store's write hook:
void onPut(String key, Order newValue, Order oldValue) {
    watchRegistry.dispatch(new ChangeEvent.PutEvent<>(
        key, newValue, oldValue, ChangeEvent.Source.LOCAL));
}

void onDelete(String key, Order lastValue) {
    watchRegistry.dispatch(new ChangeEvent.DeleteEvent<>(
        key, lastValue, ChangeEvent.Source.LOCAL));
}

void onExpire(String key, Order lastValue) {
    watchRegistry.dispatch(new ChangeEvent.ExpireEvent<>(key, lastValue));
}

// For replicated writes — pass the origin write timestamp for lag monitoring
void onReplicatedPut(String key, Order value, long originTimestampMs) {
    watchRegistry.dispatch(
        new ChangeEvent.PutEvent<>(key, value, null, ChangeEvent.Source.REPLICATED),
        originTimestampMs);
}
```

---

## Thread naming

All threads in this module are daemon threads:

| Thread name | Role |
|---|---|
| `flaircache-watch-{subscriptionId}` | Per-subscription async drain thread |

---

## API reference summary

| Class / Interface | Role |
|---|---|
| `WatchRegistry<K,V>` | Creates subscriptions, dispatches events, manages lifecycle |
| `WatchAPI<K,V>` | Fluent builder — obtained via `registry.watch()` |
| `WatchHandle` | Token for cancellation, counters, and quiescence waiting |
| `ChangeEvent<K,V>` | Sealed event type — `PutEvent`, `DeleteEvent`, `ExpireEvent` |
| `ChangeEvent.Source` | `LOCAL` or `REPLICATED` — set by the caller at construction |
| `WatchFilter<K>` | `@FunctionalInterface` key predicate — `boolean test(K key)` |

---

## Dependencies

This module has zero dependencies — no other FLAIR modules, no external libraries.

---

## What this module does not do

- **No store integration.** `WatchRegistry` is a pure event bus. It has no reference to `CacheBlock`, `LocalStore`, or any FLAIR store type. Wiring is the caller's responsibility (see above).
- **No persistence.** Events are in-process and in-memory. There is no durability or replay.
- **No distributed fan-out.** Events dispatched to a registry on Node A are not automatically forwarded to Node B. Cross-node event delivery is a replication concern, handled by `flair-cache-replication`.
- **No back-pressure.** Async queues drop oldest events when full. If your listener is consistently slower than the event rate, reduce the event rate or move processing off the drain thread.
- **No ordering guarantees across subscriptions.** All subscriptions in the registry's list receive events in registration order, but each async drain thread processes its own queue independently. Two async subscriptions are not guaranteed to process a given event at the same time.
