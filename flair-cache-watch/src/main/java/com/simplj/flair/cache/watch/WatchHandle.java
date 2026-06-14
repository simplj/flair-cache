package com.simplj.flair.cache.watch;

import java.util.concurrent.TimeUnit;

/**
 * Live token for a registered watch subscription.
 * Call {@link #cancel()} to deregister.
 * For sync subscriptions the listener is guaranteed not to be called after cancel returns.
 * For async subscriptions, events already queued at cancel time may still be delivered —
 * see {@link #cancel()} for the precise guarantee.
 */
public interface WatchHandle {

    /** Unique identifier assigned at registration time. */
    String subscriptionId();

    /** {@code true} until {@link #cancel()} is called or a {@code once()} subscription fires. */
    boolean isActive();

    /**
     * Deregisters this subscription. Idempotent — safe to call multiple times.
     * For async subscriptions, in-flight events already queued may still be delivered.
     */
    void cancel();

    /** Total events successfully delivered to this subscription's listener. Does not include dropped events. */
    long eventsReceived();

    /** Events dropped because the async dispatch queue was full (slow listener). */
    long eventsDropped();

    /**
     * Events that passed the key filter but had no matching typed listener for their event type,
     * so no callback was invoked. Counts both events rejected before queuing (event-type mismatch
     * with no corresponding listener) and events that reached the drain thread but matched no
     * put/delete/expire/event listener (e.g., lag-only subscriptions receiving non-replicated events).
     * Does not overlap with {@link #eventsDropped()}.
     */
    long eventsSkipped();

    /**
     * Blocks until this subscription is fully quiescent, or the timeout elapses.
     *
     * <ul>
     *   <li><strong>Async subscriptions</strong>: waits until the drain thread has processed all
     *       queued events and exited. For {@code once()} subscriptions this happens automatically
     *       after the first event is delivered; for persistent subscriptions it happens after
     *       {@link #cancel()} is called.</li>
     *   <li><strong>Sync subscriptions</strong>: returns {@code true} immediately once
     *       {@link #isActive()} is {@code false} (i.e., after cancel or once-fire); there is no
     *       drain thread to wait for.</li>
     * </ul>
     *
     * @return {@code true} if fully quiescent within the timeout; {@code false} if the timeout
     *         elapsed before the drain thread exited
     * @throws InterruptedException if the calling thread is interrupted while waiting
     */
    boolean awaitDone(long timeout, TimeUnit unit) throws InterruptedException;
}
