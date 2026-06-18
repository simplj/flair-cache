package com.simplj.flair.cache.watch;

import com.simplj.flair.cache.commons.FlairCacheThreadFactory;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.LockSupport;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Per-data-store dispatch hub for change events.
 *
 * <p>{@link #dispatch(ChangeEvent)} is called by the store on every mutation.
 * Subscriptions registered with {@code async(false)} are delivered inline on the calling thread
 * — this preserves the contract that sync listeners observe the event on the dispatching thread.
 * Subscriptions registered with {@code async(true)} (the default) are routed through a single
 * inbox queue and delivered by a dedicated {@code flaircache-watch-dispatch} thread, making the
 * cost of {@code dispatch()} O(1) regardless of how many async subscribers are registered.</p>
 *
 * <p>This class is standalone — it has no dependency on any FLAIR store type.
 * Wire it into a data store by calling {@link #dispatch} from the store's put/delete/expire hooks.</p>
 *
 * <h3>Once-subscription auto-cancel under Fan-out</h3>
 * <p>Fan-out calls {@link WatchSubscription#accept(ChangeEvent, long)}, which checks
 * {@link WatchSubscription#isActive()} first. For {@code once()} subscriptions the CAS
 * {@code active: true→false} ensures exactly-once delivery: if {@link WatchHandle#cancel()} races
 * with fan-out, cancel sets active=false and fan-out's accept() skips; if two events for the same
 * once() subscription land in the inbox, the first CAS wins and the second sees active=false.</p>
 */
public final class WatchRegistry<K, V> {

    private static final Logger log = Logger.getLogger(WatchRegistry.class.getName());

    // Capacity of the inbox (one DispatchWork per dispatch() call — much coarser than per-event).
    private static final int INBOX_CAPACITY = DispatchQueue.CAPACITY;

    // Sync subscriptions: delivered inline on the calling thread.
    // CopyOnWriteArrayList: iteration (dispatch) vastly outnumbers mutation (register/cancel).
    private final CopyOnWriteArrayList<WatchSubscription<K, V>> syncSubscriptions =
            new CopyOnWriteArrayList<>();

    // Async subscriptions: delivered by the dedicated fan-out thread.
    private final CopyOnWriteArrayList<WatchSubscription<K, V>> asyncSubscriptions =
            new CopyOnWriteArrayList<>();

    // Single inbox: one DispatchWork entry per dispatch() call (not per subscriber).
    private final ConcurrentLinkedQueue<DispatchWork> inbox = new ConcurrentLinkedQueue<>();

    // Soft capacity guard. LongAdder is approximate under concurrent increment+decrement;
    // acceptable for a soft cap (we allow minor over/under counts, never correctness-critical).
    private final LongAdder inboxSize = new LongAdder();

    private final Thread       dispatchThread;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public WatchRegistry() {
        FlairCacheThreadFactory tf = new FlairCacheThreadFactory("flaircache-watch-dispatch");
        this.dispatchThread = tf.newThread(this::fanOutLoop);
        this.dispatchThread.start();
    }

    /**
     * Dispatches a local event to all matching subscribers.
     *
     * <p>Sync subscribers ({@code async=false}) are called inline before this method returns.
     * Async subscribers ({@code async=true}) receive the event via the fan-out thread — this
     * call only enqueues work and unparks the fan-out thread, cost O(1).</p>
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

        // ── Sync path: inline on calling thread ───────────────────────────────
        List<WatchSubscription<K, V>> syncs = syncSubscriptions;
        if (!syncs.isEmpty()) {
            long lagMs = sourceTimestampMs >= 0
                    ? Math.max(0, System.currentTimeMillis() - sourceTimestampMs)
                    : -1L;
            for (WatchSubscription<K, V> sub : syncs) {
                try {
                    sub.accept(event, lagMs);
                } catch (Exception e) {
                    if (log.isLoggable(Level.WARNING)) {
                        log.log(Level.WARNING,
                                "WatchRegistry: unhandled exception from sync subscription "
                                        + sub.subscriptionId + " — skipping", e);
                    }
                }
            }
        }

        // ── Async path: O(1) — enqueue work, unpark fan-out thread ────────────
        List<WatchSubscription<K, V>> asyncs = asyncSubscriptions;
        if (!asyncs.isEmpty()) {
            enqueueInbox(new DispatchWork(event, sourceTimestampMs, asyncs));
        }
    }

    /**
     * Returns a fluent builder for registering a new subscription against this registry.
     */
    public WatchAPI<K, V> watch() {
        return new WatchAPI<>(this);
    }

    /**
     * Cancels all active subscriptions, stops their drain threads, and shuts down the
     * fan-out thread. After this call the registry is empty and {@link #dispatch} is a no-op.
     * Subsequent calls to {@link WatchAPI#register()} or {@link WatchAPI#once()} on a
     * builder obtained from this registry will throw {@link IllegalStateException}.
     *
     * <p>In-flight events already queued in the inbox or in per-subscription drain queues
     * may still be delivered during shutdown. Call {@link WatchHandle#awaitDone} on individual
     * handles after shutdown to wait for full quiescence.</p>
     *
     * <p>This method is idempotent; subsequent calls are no-ops.</p>
     */
    public void shutdown() {
        if (!closed.compareAndSet(false, true)) return;
        LockSupport.unpark(dispatchThread);  // wake fan-out so it sees closed=true and exits
        for (WatchSubscription<K, V> sub : syncSubscriptions)  sub.cancel();
        for (WatchSubscription<K, V> sub : asyncSubscriptions) sub.cancel();
        syncSubscriptions.clear();
        asyncSubscriptions.clear();
    }

    // ── package-private ───────────────────────────────────────────────────────

    WatchHandle register(WatchSubscription<K, V> subscription) {
        if (closed.get()) {
            throw new IllegalStateException(
                    "WatchRegistry has been shut down — no new registrations are allowed");
        }
        // Set the auto-cancel hook before adding to the dispatch list so the callback is
        // always set by the time dispatch() can see the subscription.
        subscription.setOnAutoCancel(() -> remove(subscription.subscriptionId));
        if (subscription.isAsync()) {
            asyncSubscriptions.add(subscription);
        } else {
            syncSubscriptions.add(subscription);
        }
        return new WatchHandleImpl<>(subscription, this);
    }

    void remove(String subscriptionId) {
        syncSubscriptions.removeIf(s -> s.subscriptionId.equals(subscriptionId));
        asyncSubscriptions.removeIf(s -> s.subscriptionId.equals(subscriptionId));
    }

    /** Returns {@code true} if at least one subscription is registered. O(1) — safe to call on every write. */
    public boolean hasSubscribers() {
        return !syncSubscriptions.isEmpty() || !asyncSubscriptions.isEmpty();
    }

    /** Visible for testing. Returns subscriptions where isActive() is true. */
    int activeSubscriptionCount() {
        int count = 0;
        for (WatchSubscription<K, V> s : syncSubscriptions)  { if (s.isActive()) count++; }
        for (WatchSubscription<K, V> s : asyncSubscriptions) { if (s.isActive()) count++; }
        return count;
    }

    /** Visible for testing. Returns total subscriptions in the registry, including inactive once() entries not yet evicted. */
    int totalSubscriptionCount() {
        return syncSubscriptions.size() + asyncSubscriptions.size();
    }

    // ── private ───────────────────────────────────────────────────────────────

    private void enqueueInbox(DispatchWork work) {
        if (inboxSize.sum() >= INBOX_CAPACITY) {
            // Drop the oldest batch under overload — no per-eviction logging on this hot path.
            if (inbox.poll() != null) {
                inboxSize.decrement();
            }
        }
        inbox.offer(work);
        inboxSize.increment();
        LockSupport.unpark(dispatchThread);
    }

    private void fanOutLoop() {
        while (!closed.get()) {
            DispatchWork work = inbox.poll();
            if (work != null) {
                inboxSize.decrement();
                fanOut(work);
            } else {
                LockSupport.parkNanos(50_000L);  // 50µs; woken early by enqueueInbox() or shutdown()
            }
        }
        // Flush remaining work after close so no events silently vanish.
        DispatchWork work;
        while ((work = inbox.poll()) != null) {
            fanOut(work);
        }
    }

    private void fanOut(DispatchWork work) {
        long lagMs = work.sourceTimestampMs >= 0
                ? Math.max(0, System.currentTimeMillis() - work.sourceTimestampMs)
                : -1L;
        for (WatchSubscription<K, V> sub : work.subs) {
            try {
                sub.accept(work.event, lagMs);
            } catch (Exception e) {
                if (log.isLoggable(Level.WARNING)) {
                    log.log(Level.WARNING,
                            "WatchRegistry: unhandled exception from async subscription "
                                    + sub.subscriptionId + " — skipping", e);
                }
            }
        }
    }

    /** Carries one dispatch() call's worth of async work to the fan-out thread. */
    private final class DispatchWork {
        final ChangeEvent<K, V>              event;
        final long                           sourceTimestampMs;
        // Live list reference — fan-out thread gets a snapshot via CopyOnWriteArrayList.iterator().
        final List<WatchSubscription<K, V>>  subs;

        DispatchWork(ChangeEvent<K, V> event, long sourceTimestampMs,
                     List<WatchSubscription<K, V>> subs) {
            this.event            = event;
            this.sourceTimestampMs = sourceTimestampMs;
            this.subs             = subs;
        }
    }
}
