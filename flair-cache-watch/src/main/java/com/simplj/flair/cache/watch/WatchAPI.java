package com.simplj.flair.cache.watch;

import java.time.Duration;
import java.util.Objects;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Fluent builder for registering a watch subscription.
 *
 * <p>Obtain an instance via {@link WatchRegistry#watch()}, configure with chained calls,
 * then call {@link #register()} (persistent) or {@link #once()} (one-shot).</p>
 *
 * <pre>{@code
 * WatchHandle handle = registry.watch()
 *     .onPut((k, v) -> rebuildIndex(k, v))
 *     .onDelete(k   -> removeFromIndex(k))
 *     .filter(k     -> k.startsWith("region:eu"))
 *     .async(true)
 *     .register();
 * }</pre>
 */
public final class WatchAPI<K, V> {

    private final WatchRegistry<K, V> registry;

    private BiConsumer<K, V>            putListener;
    private Consumer<K>                 deleteListener;
    private Consumer<K>                 expireListener;
    private Consumer<ChangeEvent<K, V>> eventListener;
    private Consumer<Long>              lagListener;
    private long                        lagThresholdMs = 0L;  // 0 = always notify
    private WatchFilter<K>              filter;
    private boolean                     async = true;

    WatchAPI(WatchRegistry<K, V> registry) {
        this.registry = registry;
    }

    /** Registers a callback for PUT events. Receives the key and the new value. */
    public WatchAPI<K, V> onPut(BiConsumer<K, V> listener) {
        this.putListener = Objects.requireNonNull(listener, "listener");
        return this;
    }

    /** Registers a callback for explicit DELETE events. Receives the key. */
    public WatchAPI<K, V> onDelete(Consumer<K> listener) {
        this.deleteListener = Objects.requireNonNull(listener, "listener");
        return this;
    }

    /** Registers a callback for TTL-expiry events. Receives the key. */
    public WatchAPI<K, V> onExpire(Consumer<K> listener) {
        this.expireListener = Objects.requireNonNull(listener, "listener");
        return this;
    }

    /**
     * Registers a raw event callback that receives the full {@link ChangeEvent}.
     * Use this when you need access to {@link ChangeEvent.PutEvent#oldValue()},
     * {@link ChangeEvent.PutEvent#source()}, {@link ChangeEvent.DeleteEvent#lastValue()},
     * or other fields not exposed by the type-specific callbacks.
     * Called in addition to any {@link #onPut}/{@link #onDelete}/{@link #onExpire} listener
     * registered on the same subscription.
     */
    public WatchAPI<K, V> onEvent(Consumer<ChangeEvent<K, V>> listener) {
        this.eventListener = Objects.requireNonNull(listener, "listener");
        return this;
    }

    /**
     * Registers a replication-lag callback. Invoked for every replicated event regardless
     * of lag magnitude. Receives the lag in milliseconds.
     *
     * <p><strong>Threading:</strong> this callback always runs synchronously on the thread that
     * called {@code put()} on the store — even when {@link #async(boolean) async(true)} is set.
     * Keep the implementation O(1) and non-blocking (target &lt; 1 µs). For heavy processing,
     * hand off to your own executor inside the callback.</p>
     */
    public WatchAPI<K, V> onReplicationLag(Consumer<Long> listener) {
        this.lagListener    = Objects.requireNonNull(listener, "listener");
        this.lagThresholdMs = 0L;
        return this;
    }

    /**
     * Registers a replication-lag callback that fires only when {@code lag >= threshold}.
     * Receives the lag in milliseconds.
     *
     * <p>{@code threshold} must be positive. To receive every replicated event regardless
     * of lag use {@link #onReplicationLag(Consumer)} instead.</p>
     *
     * <p><strong>Threading:</strong> this callback always runs synchronously on the thread that
     * called {@code put()} on the store — even when {@link #async(boolean) async(true)} is set.
     * Keep the implementation O(1) and non-blocking (target &lt; 1 µs). For heavy processing,
     * hand off to your own executor inside the callback.</p>
     *
     * @throws IllegalArgumentException if {@code threshold} is zero or negative
     */
    public WatchAPI<K, V> onReplicationLag(Duration threshold, Consumer<Long> listener) {
        Objects.requireNonNull(threshold, "threshold");
        if (threshold.isNegative() || threshold.isZero()) {
            throw new IllegalArgumentException(
                    "threshold must be positive; use onReplicationLag(Consumer) to fire on every replicated event");
        }
        this.lagListener    = Objects.requireNonNull(listener, "listener");
        this.lagThresholdMs = threshold.toMillis();
        return this;
    }

    /**
     * Restricts dispatch to events whose key satisfies the predicate.
     * The predicate is applied before any value deserialization.
     *
     * <p>Calling {@code filter()} more than once composes the predicates with AND semantics:
     * the resulting filter accepts a key only if every supplied predicate accepts it.
     * The predicates are tested in the order they were registered.</p>
     */
    public WatchAPI<K, V> filter(WatchFilter<K> filter) {
        Objects.requireNonNull(filter, "filter");
        WatchFilter<K> existing = this.filter;
        this.filter = existing == null ? filter : (k -> existing.test(k) && filter.test(k));
        return this;
    }

    /**
     * Controls dispatch threading.
     * {@code true} (default): listener runs on a dedicated {@code flaircache-watch-{id}} thread.
     * {@code false}: listener runs on the thread that called {@code put()} — must complete in &lt; 1 µs.
     */
    public WatchAPI<K, V> async(boolean async) {
        this.async = async;
        return this;
    }

    /**
     * Registers a persistent subscription. The listener continues to receive events
     * until {@link WatchHandle#cancel()} is called.
     *
     * @throws IllegalStateException if no listener has been registered, or if the registry
     *                               has been shut down
     */
    public WatchHandle register() {
        return registry.register(build(false));
    }

    /**
     * Registers a one-shot subscription that auto-cancels after the first matching event.
     * The returned handle reflects the cancelled state immediately after the first delivery.
     *
     * @throws IllegalStateException if no listener has been registered, or if the registry
     *                               has been shut down
     */
    public WatchHandle once() {
        return registry.register(build(true));
    }

    // ── internal ──────────────────────────────────────────────────────────────

    private WatchSubscription<K, V> build(boolean once) {
        if (putListener == null && deleteListener == null && expireListener == null
                && eventListener == null && lagListener == null) {
            throw new IllegalStateException(
                    "WatchAPI: at least one listener must be registered before calling register() or once()");
        }
        ReplicationLagMonitor monitor = lagListener != null
                ? new ReplicationLagMonitor(lagThresholdMs, lagListener)
                : null;
        return new WatchSubscription<>(
                UUID.randomUUID().toString(),
                filter,
                putListener,
                deleteListener,
                expireListener,
                eventListener,
                monitor,
                async,
                once
        );
    }
}
