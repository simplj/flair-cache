package com.simplj.flair.cache.watch;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Per-data-store dispatch hub for change events.
 *
 * <p>{@link #dispatch(ChangeEvent)} is called by the store on every mutation.
 * It is designed to return in {@literal <} 500 ns regardless of subscriber count:
 * the only work per active subscription is a key filter check and a non-blocking
 * {@code offer()} to an {@link java.util.concurrent.ArrayBlockingQueue}.</p>
 *
 * <p>This class is standalone — it has no dependency on any FLAIR store type.
 * Wire it into a data store by calling {@link #dispatch} from the store's put/delete/expire hooks.</p>
 */
public final class WatchRegistry<K, V> {

    private static final Logger log = Logger.getLogger(WatchRegistry.class.getName());

    // CopyOnWriteArrayList: reads (dispatch) vastly outnumber writes (register/cancel)
    private final CopyOnWriteArrayList<WatchSubscription<K, V>> subscriptions =
            new CopyOnWriteArrayList<>();

    private final AtomicBoolean closed = new AtomicBoolean(false);

    /**
     * Dispatches a local event to all matching subscribers.
     * Returns immediately — all I/O-bound work is handed off to per-subscription drain threads.
     *
     * @throws IllegalArgumentException if {@code event} is null
     */
    public void dispatch(ChangeEvent<K, V> event) {
        dispatch(event, -1L);
    }

    /**
     * Dispatches a replicated event. {@code sourceTimestampMs} is the HLC logical time
     * (wall-clock epoch ms) at which the originating node wrote the entry.
     * If non-negative, lag is computed as {@code now - sourceTimestampMs} and forwarded
     * to any registered lag monitors. Pass a negative value to signal a local event
     * with no lag computation (the no-arg {@link #dispatch(ChangeEvent)} overload uses
     * {@code -1} for this purpose).
     *
     * @throws IllegalArgumentException if {@code event} is null
     */
    public void dispatch(ChangeEvent<K, V> event, long sourceTimestampMs) {
        if (event == null) throw new IllegalArgumentException("event must not be null");

        List<WatchSubscription<K, V>> subs = subscriptions;
        if (subs.isEmpty()) return;

        long lagMs = sourceTimestampMs >= 0
                ? Math.max(0, System.currentTimeMillis() - sourceTimestampMs)
                : -1L;

        for (WatchSubscription<K, V> sub : subs) {
            try {
                sub.accept(event, lagMs);
            } catch (Exception e) {
                // Safety net: accept() handles its own user-facing exceptions internally,
                // but this catch prevents any unexpected failure from aborting delivery
                // to the remaining subscriptions in the loop.
                if (log.isLoggable(Level.WARNING)) {
                    log.log(Level.WARNING,
                            "WatchRegistry: unhandled exception from subscription "
                                    + sub.subscriptionId + " — skipping", e);
                }
            }
        }
    }

    /**
     * Returns a fluent builder for registering a new subscription against this registry.
     */
    public WatchAPI<K, V> watch() {
        return new WatchAPI<>(this);
    }

    /**
     * Cancels all active subscriptions and stops their drain threads.
     * After this call the registry is empty and {@link #dispatch} is a no-op.
     * Subsequent calls to {@link WatchAPI#register()} or {@link WatchAPI#once()} on a
     * builder obtained from this registry will throw {@link IllegalStateException}.
     *
     * <p>In-flight events already being processed by async drain threads may still be
     * delivered during the brief period before each drain thread acknowledges the stop signal.
     * Call {@link WatchHandle#awaitDone} on individual handles after shutdown to wait for
     * full quiescence.</p>
     *
     * <p>This method is idempotent; subsequent calls are no-ops.</p>
     */
    public void shutdown() {
        if (!closed.compareAndSet(false, true)) return;
        for (WatchSubscription<K, V> sub : subscriptions) {
            sub.cancel();
        }
        subscriptions.clear();
    }

    // ── package-private ───────────────────────────────────────────────────────

    WatchHandle register(WatchSubscription<K, V> subscription) {
        if (closed.get()) {
            throw new IllegalStateException(
                    "WatchRegistry has been shut down — no new registrations are allowed");
        }
        // Set the auto-cancel hook before adding to the dispatch list.
        // This guarantees the callback is always set by the time dispatch() can see the subscription.
        subscription.setOnAutoCancel(() -> remove(subscription.subscriptionId));
        subscriptions.add(subscription);
        return new WatchHandleImpl<>(subscription, this);
    }

    void remove(String subscriptionId) {
        subscriptions.removeIf(s -> s.subscriptionId.equals(subscriptionId));
    }

    /** Returns {@code true} if at least one subscription is registered. O(1) — safe to call on every write. */
    public boolean hasSubscribers() {
        return !subscriptions.isEmpty();
    }

    /** Visible for testing. Returns subscriptions where isActive() is true. */
    int activeSubscriptionCount() {
        int count = 0;
        for (WatchSubscription<K, V> s : subscriptions) {
            if (s.isActive()) count++;
        }
        return count;
    }

    /** Visible for testing. Returns total subscriptions in the registry, including inactive once() entries not yet evicted. */
    int totalSubscriptionCount() {
        return subscriptions.size();
    }
}
