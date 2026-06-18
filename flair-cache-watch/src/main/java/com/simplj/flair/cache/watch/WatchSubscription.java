package com.simplj.flair.cache.watch;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Internal state for a single registered watch subscription.
 * Holds listeners, filter, dispatch queue, and counters.
 */
final class WatchSubscription<K, V> {

    private static final Logger log = Logger.getLogger(WatchSubscription.class.getName());

    final String subscriptionId;

    private final WatchFilter<K>              filter;          // null = accept all
    private final BiConsumer<K, V>            putListener;     // null = not subscribed
    private final Consumer<K>                 deleteListener;
    private final Consumer<K>                 expireListener;
    private final Consumer<ChangeEvent<K, V>> eventListener;   // raw event; receives full ChangeEvent
    private final ReplicationLagMonitor       lagMonitor;      // null = not subscribed
    private final boolean                     async;
    private final boolean                     once;

    private final AtomicBoolean active         = new AtomicBoolean(true);
    private final LongAdder     eventsReceived = new LongAdder();
    private final LongAdder     eventsDropped  = new LongAdder();
    private final LongAdder     eventsSkipped  = new LongAdder();

    // non-null only when async=true
    private final DispatchQueue<K, V> dispatchQueue;

    // Set by WatchRegistry.register() before adding to the dispatch list.
    // Called when a once() subscription auto-deactivates so the registry can evict it.
    private volatile Runnable onAutoCancel;

    WatchSubscription(String subscriptionId,
                      WatchFilter<K> filter,
                      BiConsumer<K, V> putListener,
                      Consumer<K> deleteListener,
                      Consumer<K> expireListener,
                      Consumer<ChangeEvent<K, V>> eventListener,
                      ReplicationLagMonitor lagMonitor,
                      boolean async,
                      boolean once) {
        this.subscriptionId  = subscriptionId;
        this.filter          = filter;
        this.putListener     = putListener;
        this.deleteListener  = deleteListener;
        this.expireListener  = expireListener;
        this.eventListener   = eventListener;
        this.lagMonitor      = lagMonitor;
        this.async           = async;
        this.once            = once;

        this.dispatchQueue = async
                ? new DispatchQueue<>(subscriptionId, eventsDropped, this::handleEvent)
                : null;
    }

    /**
     * Dispatches {@code event} to this subscription if active and key passes the filter.
     * For {@code once} subscriptions, deactivates after the first accepted event.
     *
     * @param event    the mutation event
     * @param lagMs    replication lag in ms; negative means not a replicated event
     */
    void accept(ChangeEvent<K, V> event, long lagMs) {
        if (!active.get()) return;

        K key = extractKey(event);

        if (filter != null) {
            boolean matches;
            try {
                matches = filter.test(key);
            } catch (Exception e) {
                // A throwing filter is treated as rejection — skip and count, but do not propagate,
                // so that sibling subscriptions in the registry's dispatch loop are not affected.
                if (log.isLoggable(Level.WARNING)) {
                    log.log(Level.WARNING,
                            "Watch[" + subscriptionId + "] key filter threw — event skipped", e);
                }
                eventsSkipped.increment();
                return;
            }
            if (!matches) return;
        }

        // Guard: skip (and count) events for which no callback would ever fire.
        // This must run before the once-CAS so that a once() subscription is not consumed
        // by an event type it has no listener for (e.g., onPut().once() eaten by a DELETE).
        if (!wouldDispatch(event, lagMs)) {
            eventsSkipped.increment();
            return;
        }

        if (once && !active.compareAndSet(true, false)) {
            // Another thread won the race for this once-subscription
            return;
        }

        if (lagMs >= 0 && lagMonitor != null) {
            try {
                lagMonitor.check(lagMs);
            } catch (Exception e) {
                // Lag-monitor failure must not abort event delivery to typed listeners.
                if (log.isLoggable(Level.WARNING)) {
                    log.log(Level.WARNING,
                            "Watch[" + subscriptionId + "] lag monitor threw", e);
                }
            }
        }

        if (async) {
            if (hasTypedListener(event)) {
                dispatchQueue.offer(event);
            } else if (once) {
                // Lag-only once: lag callback already fired; no typed listener to queue for.
                // Stop the idle drain thread and evict from registry.
                dispatchQueue.stop();
                runAutoCancel();
            }
            // else: lag-only non-once — lag callback already fired; nothing more to do.
        } else {
            try {
                if (hasTypedListener(event)) {
                    handleEvent(event);
                }
            } finally {
                if (once) runAutoCancel();
            }
        }
    }

    void setOnAutoCancel(Runnable r) {
        this.onAutoCancel = r;
    }

    void cancel() {
        if (active.compareAndSet(true, false)) {
            if (dispatchQueue != null) {
                dispatchQueue.stop();
            }
        }
    }

    boolean isActive() {
        return active.get();
    }

    boolean isAsync() {
        return async;
    }

    long eventsReceived() {
        return eventsReceived.sum();
    }

    long eventsDropped() {
        return eventsDropped.sum();
    }

    long eventsSkipped() {
        return eventsSkipped.sum();
    }

    boolean awaitDone(long timeout, TimeUnit unit) throws InterruptedException {
        if (dispatchQueue != null) {
            return dispatchQueue.awaitDone(timeout, unit);
        }
        // Sync subscription: no drain thread; done once the subscription is inactive.
        return !active.get();
    }

    // ── internal ──────────────────────────────────────────────────────────────

    private void handleEvent(ChangeEvent<K, V> event) {
        try {
            boolean dispatched = false;
            if (event instanceof ChangeEvent.PutEvent<K, V> put) {
                if (putListener != null) { putListener.accept(put.key(), put.newValue()); dispatched = true; }
            } else if (event instanceof ChangeEvent.DeleteEvent<K, V> del) {
                if (deleteListener != null) { deleteListener.accept(del.key()); dispatched = true; }
            } else if (event instanceof ChangeEvent.ExpireEvent<K, V> exp) {
                if (expireListener != null) { expireListener.accept(exp.key()); dispatched = true; }
            }
            if (eventListener != null) { eventListener.accept(event); dispatched = true; }
            if (dispatched) {
                eventsReceived.increment();
            } else {
                // Reached the drain thread but no typed listener matched — only possible for
                // lag-only subscriptions receiving replicated events (wouldDispatch passed via lagMonitor).
                eventsSkipped.increment();
            }
        } finally {
            // For async once: active is already false (CAS in accept()). Stop queue and evict from registry.
            // finally guarantees this runs even if a listener throws.
            if (once && async) {
                dispatchQueue.stop();
                runAutoCancel();
            }
        }
    }

    private void runAutoCancel() {
        Runnable r = onAutoCancel;
        if (r != null) r.run();
    }

    // Returns true iff at least one callback would be invoked for this event+lag combination.
    // Called before the once-CAS to prevent consuming the subscription on a no-op dispatch.
    private boolean wouldDispatch(ChangeEvent<K, V> event, long lagMs) {
        if (eventListener != null)                                              return true;
        if (event instanceof ChangeEvent.PutEvent    && putListener    != null) return true;
        if (event instanceof ChangeEvent.DeleteEvent && deleteListener != null) return true;
        if (event instanceof ChangeEvent.ExpireEvent && expireListener != null) return true;
        if (lagMs >= 0 && lagMonitor != null)                                   return true;
        return false;
    }

    // Returns true iff at least one TYPED listener (put/delete/expire/event) would be called.
    // Used after the lag monitor has already fired to decide whether to route the event to
    // the async dispatch queue or sync handleEvent. Lag-only subscriptions skip the queue
    // entirely since the lag callback already ran synchronously in accept().
    private boolean hasTypedListener(ChangeEvent<K, V> event) {
        if (eventListener != null)                                              return true;
        if (event instanceof ChangeEvent.PutEvent    && putListener    != null) return true;
        if (event instanceof ChangeEvent.DeleteEvent && deleteListener != null) return true;
        if (event instanceof ChangeEvent.ExpireEvent && expireListener != null) return true;
        return false;
    }

    private static <K, V> K extractKey(ChangeEvent<K, V> event) {
        if (event instanceof ChangeEvent.PutEvent<K, V> e)    return e.key();
        if (event instanceof ChangeEvent.DeleteEvent<K, V> e) return e.key();
        if (event instanceof ChangeEvent.ExpireEvent<K, V> e) return e.key();
        throw new IllegalArgumentException("Unknown event type: " + event.getClass());
    }
}
