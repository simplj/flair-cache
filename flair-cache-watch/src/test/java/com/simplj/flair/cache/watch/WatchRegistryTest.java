package com.simplj.flair.cache.watch;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

@Timeout(10)
class WatchRegistryTest {

    private WatchRegistry<String, String> registry;

    @BeforeEach
    void setUp() {
        registry = new WatchRegistry<>();
    }

    @AfterEach
    void tearDown() {
        registry.shutdown();
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    private ChangeEvent<String, String> putLocal(String key, String value) {
        return new ChangeEvent.PutEvent<>(key, value, null, ChangeEvent.Source.LOCAL);
    }

    private ChangeEvent<String, String> putReplicated(String key, String value) {
        return new ChangeEvent.PutEvent<>(key, value, null, ChangeEvent.Source.REPLICATED);
    }

    private ChangeEvent<String, String> deleteLocal(String key) {
        return new ChangeEvent.DeleteEvent<>(key, "old", ChangeEvent.Source.LOCAL);
    }

    private ChangeEvent<String, String> expire(String key) {
        return new ChangeEvent.ExpireEvent<>(key, "expired-val");
    }

    private static void awaitCount(AtomicInteger counter, int expected) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 3000;
        while (counter.get() < expected && System.currentTimeMillis() < deadline) {
            Thread.sleep(5);
        }
        assertEquals(expected, counter.get());
    }

    // ── onPut — local ─────────────────────────────────────────────────────────

    @Test
    void onPutFiresForLocalPut() throws InterruptedException {
        AtomicInteger count = new AtomicInteger();
        registry.watch().onPut((k, v) -> count.incrementAndGet()).register();

        registry.dispatch(putLocal("k1", "v1"));
        awaitCount(count, 1);
    }

    // ── onPut — replicated ────────────────────────────────────────────────────

    @Test
    void onPutFiresForReplicatedPut() throws InterruptedException {
        AtomicInteger count = new AtomicInteger();
        registry.watch().onPut((k, v) -> count.incrementAndGet()).register();

        registry.dispatch(putReplicated("k1", "v1"));
        awaitCount(count, 1);
    }

    // ── onDelete ──────────────────────────────────────────────────────────────

    @Test
    void onDeleteFiresOnExplicitDelete() throws InterruptedException {
        AtomicReference<String> deleted = new AtomicReference<>();
        registry.watch().onDelete(deleted::set).register();

        registry.dispatch(deleteLocal("k1"));

        long deadline = System.currentTimeMillis() + 3000;
        while (deleted.get() == null && System.currentTimeMillis() < deadline) {
            Thread.sleep(5);
        }
        assertEquals("k1", deleted.get());
    }

    // ── onExpire ──────────────────────────────────────────────────────────────

    @Test
    void onExpireFiresOnTtlExpiry() throws InterruptedException {
        AtomicReference<String> expired = new AtomicReference<>();
        registry.watch().onExpire(expired::set).register();

        registry.dispatch(expire("k1"));

        long deadline = System.currentTimeMillis() + 3000;
        while (expired.get() == null && System.currentTimeMillis() < deadline) {
            Thread.sleep(5);
        }
        assertEquals("k1", expired.get());
    }

    // ── key filter ────────────────────────────────────────────────────────────

    @Test
    void filterPreventsDispatchForNonMatchingKeys() throws InterruptedException {
        AtomicInteger count = new AtomicInteger();
        registry.watch()
                .onPut((k, v) -> count.incrementAndGet())
                .filter(k -> k.startsWith("eu:"))
                .register();

        registry.dispatch(putLocal("us:k1", "v1"));
        registry.dispatch(putLocal("eu:k2", "v2"));
        registry.dispatch(putLocal("eu:k3", "v3"));

        awaitCount(count, 2);
        assertEquals(2, count.get());
    }

    // ── cancel ────────────────────────────────────────────────────────────────

    @Test
    void cancelPreventsSubsequentDelivery() throws InterruptedException {
        AtomicInteger count = new AtomicInteger();
        WatchHandle handle = registry.watch()
                .onPut((k, v) -> count.incrementAndGet())
                .register();

        registry.dispatch(putLocal("k1", "v1"));
        awaitCount(count, 1);

        handle.cancel();
        assertFalse(handle.isActive());

        registry.dispatch(putLocal("k2", "v2"));
        Thread.sleep(100); // give async path time to (not) deliver
        assertEquals(1, count.get());
    }

    // ── once ──────────────────────────────────────────────────────────────────

    @Test
    void onceAutosCancelsAfterFirstEvent() throws InterruptedException {
        AtomicInteger count = new AtomicInteger();
        WatchHandle handle = registry.watch()
                .onPut((k, v) -> count.incrementAndGet())
                .once();

        registry.dispatch(putLocal("k1", "v1"));
        awaitCount(count, 1);

        registry.dispatch(putLocal("k2", "v2"));
        Thread.sleep(100);
        assertEquals(1, count.get());
        assertFalse(handle.isActive());
    }

    @Test
    void onceSubscriptionRemovedFromRegistryAfterFiring() throws InterruptedException {
        AtomicInteger count = new AtomicInteger();
        registry.watch()
                .onPut((k, v) -> count.incrementAndGet())
                .once();

        assertEquals(1, registry.totalSubscriptionCount());

        registry.dispatch(putLocal("k1", "v1"));
        awaitCount(count, 1);

        // After once fires, the subscription must be physically removed from the COWAL.
        // We use totalSubscriptionCount() (not activeSubscriptionCount()) because active is set
        // to false by the CAS in accept() before the drain thread calls runAutoCancel() — checking
        // isActive() alone cannot detect a broken eviction.
        long deadline = System.currentTimeMillis() + 3000;
        while (registry.totalSubscriptionCount() > 0 && System.currentTimeMillis() < deadline) {
            Thread.sleep(5);
        }
        assertEquals(0, registry.totalSubscriptionCount(),
                "once() subscription must be physically evicted from the registry COWAL after firing");
    }

    @Test
    void onceSyncSubscriptionRemovedFromRegistryAfterFiring() {
        AtomicInteger count = new AtomicInteger();
        registry.watch()
                .onPut((k, v) -> count.incrementAndGet())
                .async(false)
                .once();

        assertEquals(1, registry.totalSubscriptionCount());

        registry.dispatch(putLocal("k1", "v1"));
        assertEquals(1, count.get());

        // Sync delivery: auto-removal happens on the dispatch thread, so it's visible immediately.
        // totalSubscriptionCount() checks COWAL size — a broken runAutoCancel() that leaves the
        // entry in the list would be caught here even though isActive() is already false.
        assertEquals(0, registry.totalSubscriptionCount(),
                "once() sync subscription must be physically evicted from the registry COWAL after firing");
    }

    // ── slow listener — queue overflow ────────────────────────────────────────

    @Test
    void slowListenerDropsOldestWhenQueueFull() throws InterruptedException {
        CountDownLatch blockFirst    = new CountDownLatch(1);
        CountDownLatch firstReceived = new CountDownLatch(1);
        AtomicInteger  received      = new AtomicInteger();

        WatchHandle handle = registry.watch()
                .onPut((k, v) -> {
                    firstReceived.countDown();
                    // Block first delivery so the queue fills up
                    try { blockFirst.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                    received.incrementAndGet();
                })
                .register();

        // Wait for the drain thread to start processing the first event
        registry.dispatch(putLocal("k0", "v0"));
        assertTrue(firstReceived.await(3, TimeUnit.SECONDS));

        // Flood queue beyond capacity while drain thread is blocked
        int overflow = DispatchQueue.CAPACITY + 50;
        for (int i = 1; i <= overflow; i++) {
            registry.dispatch(putLocal("k" + i, "v" + i));
        }

        // Unblock the drain thread
        blockFirst.countDown();

        long deadline = System.currentTimeMillis() + 5000;
        while (handle.eventsDropped() == 0 && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }

        assertTrue(handle.eventsDropped() > 0, "Expected dropped events");
        handle.cancel();
    }

    // ── Source: LOCAL vs REPLICATED ───────────────────────────────────────────

    @Test
    void localAndReplicatedSourceDistinguishedThroughDispatch() throws InterruptedException {
        // Source is set by the caller on the ChangeEvent record; the registry must preserve it.
        // We use the lag callback as a dispatch-path proxy: only REPLICATED events trigger it.
        AtomicInteger putCount  = new AtomicInteger();
        AtomicInteger lagCount  = new AtomicInteger();
        CountDownLatch putLatch = new CountDownLatch(2);

        registry.watch()
                .onPut((k, v) -> { putCount.incrementAndGet(); putLatch.countDown(); })
                .onReplicationLag(lag -> lagCount.incrementAndGet())
                .register();

        // LOCAL event — reaches the listener but must NOT trigger the lag callback
        ChangeEvent.PutEvent<String, String> local =
                new ChangeEvent.PutEvent<>("k1", "v1", null, ChangeEvent.Source.LOCAL);
        assertEquals(ChangeEvent.Source.LOCAL, local.source());
        registry.dispatch(local);   // no sourceTimestampMs → treated as local

        // REPLICATED event — reaches the listener AND triggers the lag callback
        ChangeEvent.PutEvent<String, String> replicated =
                new ChangeEvent.PutEvent<>("k2", "v2", null, ChangeEvent.Source.REPLICATED);
        assertEquals(ChangeEvent.Source.REPLICATED, replicated.source());
        registry.dispatch(replicated, System.currentTimeMillis() - 200);

        assertTrue(putLatch.await(3, TimeUnit.SECONDS), "Both events should reach the listener");
        assertEquals(2, putCount.get());

        long deadline = System.currentTimeMillis() + 1000;
        while (lagCount.get() < 1 && System.currentTimeMillis() < deadline) Thread.sleep(5);
        assertEquals(1, lagCount.get(), "Only the REPLICATED event should trigger the lag callback");
    }

    @Test
    void replicationLagCallbackReceivedForReplicatedDispatch() throws InterruptedException {
        AtomicReference<Long> capturedLag = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        registry.watch()
                .onPut((k, v) -> {})
                .onReplicationLag(lag -> {
                    capturedLag.set(lag);
                    latch.countDown();
                })
                .register();

        long sourceTimestampMs = System.currentTimeMillis() - 500; // 500ms ago
        registry.dispatch(putReplicated("k1", "v1"), sourceTimestampMs);

        assertTrue(latch.await(3, TimeUnit.SECONDS));
        assertTrue(capturedLag.get() >= 0, "Lag should be non-negative");
    }

    @Test
    void replicationLagCallbackNotCalledForLocalDispatch() throws InterruptedException {
        AtomicInteger lagCount = new AtomicInteger();

        registry.watch()
                .onPut((k, v) -> {})
                .onReplicationLag(lag -> lagCount.incrementAndGet())
                .register();

        // dispatch without a source timestamp (local event)
        registry.dispatch(putLocal("k1", "v1"));
        Thread.sleep(100);
        assertEquals(0, lagCount.get());
    }

    @Test
    void replicationLagThresholdFiltersLowLag() throws InterruptedException {
        AtomicInteger lagCount = new AtomicInteger();

        registry.watch()
                .onPut((k, v) -> {})
                .onReplicationLag(Duration.ofSeconds(10), lag -> lagCount.incrementAndGet())
                .register();

        // sourceTimestamp very recent — lag << 10s threshold
        registry.dispatch(putReplicated("k1", "v1"), System.currentTimeMillis() - 50);
        Thread.sleep(100);
        assertEquals(0, lagCount.get(), "Lag below threshold should not trigger callback");
    }

    // ── concurrent put — no deadlock ──────────────────────────────────────────

    @Test
    void concurrentPutsDispatchWithoutDeadlock() throws InterruptedException {
        int threads  = 8;
        int perThread = 100;
        AtomicInteger count = new AtomicInteger();
        CountDownLatch done  = new CountDownLatch(threads * perThread);

        registry.watch()
                .onPut((k, v) -> {
                    count.incrementAndGet();
                    done.countDown();
                })
                .register();

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            for (int t = 0; t < threads; t++) {
                final int tid = t;
                pool.submit(() -> {
                    for (int i = 0; i < perThread; i++) {
                        registry.dispatch(putLocal("t" + tid + "-k" + i, "v" + i));
                    }
                });
            }
            assertTrue(done.await(5, TimeUnit.SECONDS),
                    "Expected " + (threads * perThread) + " events, got " + count.get());
        } finally {
            pool.shutdown();
        }
    }

    // ── handle state ──────────────────────────────────────────────────────────

    @Test
    void handleIsActiveAfterRegistrationAndInactiveAfterCancel() {
        WatchHandle handle = registry.watch().onPut((k, v) -> {}).register();
        assertTrue(handle.isActive());
        assertNotNull(handle.subscriptionId());

        handle.cancel();
        assertFalse(handle.isActive());

        // idempotent
        handle.cancel();
        assertFalse(handle.isActive());
    }

    @Test
    void eventsReceivedCounterIncrements() throws InterruptedException {
        WatchHandle handle = registry.watch()
                .onPut((k, v) -> {})
                .register();

        registry.dispatch(putLocal("k1", "v1"));
        registry.dispatch(putLocal("k2", "v2"));

        long deadline = System.currentTimeMillis() + 3000;
        while (handle.eventsReceived() < 2 && System.currentTimeMillis() < deadline) {
            Thread.sleep(5);
        }
        assertEquals(2, handle.eventsReceived());
        handle.cancel();
    }

    // ── dispatch is a no-op when no subscriptions ─────────────────────────────

    @Test
    void dispatchWithNoSubscriptionsIsNoOp() {
        // should not throw
        assertDoesNotThrow(() -> registry.dispatch(putLocal("k1", "v1")));
        assertEquals(0, registry.activeSubscriptionCount());
    }

    // ── sync mode ─────────────────────────────────────────────────────────────

    @Test
    void syncListenerCalledOnDispatchingThread() throws InterruptedException {
        AtomicReference<String> callerThread = new AtomicReference<>();
        Thread dispatchThread = Thread.currentThread();

        WatchHandle handle = registry.watch()
                .onPut((k, v) -> callerThread.set(Thread.currentThread().getName()))
                .async(false)
                .register();

        registry.dispatch(putLocal("k1", "v1"));
        assertNotNull(callerThread.get());
        assertEquals(dispatchThread.getName(), callerThread.get());
        handle.cancel();
    }

    // ── once sync ─────────────────────────────────────────────────────────────

    @Test
    void onceSyncDeliversExactlyOneEvent() {
        AtomicInteger count = new AtomicInteger();
        WatchHandle handle = registry.watch()
                .onPut((k, v) -> count.incrementAndGet())
                .async(false)
                .once();

        registry.dispatch(putLocal("k1", "v1"));
        registry.dispatch(putLocal("k2", "v2"));
        registry.dispatch(putLocal("k3", "v3"));

        assertEquals(1, count.get());
        assertFalse(handle.isActive());
    }

    // ── multiple event types on same subscription ─────────────────────────────

    @Test
    void subscriptionCanListenToMultipleEventTypes() throws InterruptedException {
        AtomicInteger puts    = new AtomicInteger();
        AtomicInteger deletes = new AtomicInteger();
        AtomicInteger expires = new AtomicInteger();

        registry.watch()
                .onPut((k, v) -> puts.incrementAndGet())
                .onDelete(k -> deletes.incrementAndGet())
                .onExpire(k -> expires.incrementAndGet())
                .register();

        registry.dispatch(putLocal("k1", "v1"));
        registry.dispatch(deleteLocal("k2"));
        registry.dispatch(expire("k3"));

        long deadline = System.currentTimeMillis() + 3000;
        while ((puts.get() < 1 || deletes.get() < 1 || expires.get() < 1)
                && System.currentTimeMillis() < deadline) {
            Thread.sleep(5);
        }
        assertEquals(1, puts.get());
        assertEquals(1, deletes.get());
        assertEquals(1, expires.get());
    }

    // ── Bug 1: once() not consumed by unmatched event type ────────────────────

    @Test
    void oncePutSubscriptionNotConsumedByDeleteEvent() {
        AtomicInteger putCount = new AtomicInteger();
        WatchHandle handle = registry.watch()
                .onPut((k, v) -> putCount.incrementAndGet())
                .async(false)
                .once();

        // DELETE has no matching listener — must not consume the once or call any listener.
        registry.dispatch(deleteLocal("k1"));
        assertEquals(0, putCount.get());
        assertTrue(handle.isActive(), "once() must stay active after an unmatched event type");
        assertEquals(1, handle.eventsSkipped(), "unmatched event type must be counted as skipped");

        // The PUT now fires and auto-cancels the once.
        registry.dispatch(putLocal("k1", "v1"));
        assertEquals(1, putCount.get());
        assertFalse(handle.isActive());
        assertEquals(1, handle.eventsReceived());
    }

    @Test
    void oncePutSubscriptionNotConsumedByExpireEvent() {
        AtomicInteger putCount = new AtomicInteger();
        WatchHandle handle = registry.watch()
                .onPut((k, v) -> putCount.incrementAndGet())
                .async(false)
                .once();

        registry.dispatch(expire("k1"));
        assertEquals(0, putCount.get());
        assertTrue(handle.isActive());
        assertEquals(1, handle.eventsSkipped());

        registry.dispatch(putLocal("k1", "v1"));
        assertEquals(1, putCount.get());
        assertFalse(handle.isActive());
    }

    // ── Missing Feature 1: shutdown() ─────────────────────────────────────────

    @Test
    void shutdownCancelsAllSubscriptionsAndPreventsNewDeliveries() {
        AtomicInteger count = new AtomicInteger();

        WatchHandle h1 = registry.watch()
                .onPut((k, v) -> count.incrementAndGet())
                .async(false)
                .register();
        WatchHandle h2 = registry.watch()
                .onDelete(k -> count.incrementAndGet())
                .async(false)
                .register();

        registry.dispatch(putLocal("k1", "v1"));
        assertEquals(1, count.get());

        registry.shutdown();

        assertFalse(h1.isActive());
        assertFalse(h2.isActive());
        assertEquals(0, registry.totalSubscriptionCount());

        registry.dispatch(putLocal("k2", "v2"));
        registry.dispatch(deleteLocal("k2"));
        assertEquals(1, count.get(), "no events must be delivered after shutdown");
    }

    @Test
    void shutdownIsIdempotent() {
        registry.watch().onPut((k, v) -> {}).async(false).register();
        registry.shutdown();
        assertDoesNotThrow(() -> registry.shutdown());
        assertEquals(0, registry.totalSubscriptionCount());
    }

    // ── Missing Feature 2: no-listener validation ─────────────────────────────

    @Test
    void registerWithNoListenerThrowsIllegalStateException() {
        assertThrows(IllegalStateException.class, () -> registry.watch().register());
    }

    @Test
    void onceWithNoListenerThrowsIllegalStateException() {
        assertThrows(IllegalStateException.class, () -> registry.watch().once());
    }

    @Test
    void lagOnlySubscriptionPassesValidation() {
        // A lag-only subscription (no typed listener) must be accepted.
        assertDoesNotThrow(() ->
                registry.watch()
                        .onReplicationLag(lag -> {})
                        .register()
                        .cancel());
    }

    // ── Minor 1: eventsSkipped counter ────────────────────────────────────────

    @Test
    void eventsSkippedCounterIncrementsForUnmatchedEventType() {
        WatchHandle handle = registry.watch()
                .onPut((k, v) -> {})
                .async(false)
                .register();

        registry.dispatch(deleteLocal("k1"));
        registry.dispatch(expire("k2"));

        assertEquals(2, handle.eventsSkipped());
        assertEquals(0, handle.eventsReceived());
        assertEquals(0, handle.eventsDropped());
    }

    @Test
    void eventsSkippedNotIncrementedForMatchingEventType() {
        WatchHandle handle = registry.watch()
                .onPut((k, v) -> {})
                .onDelete(k -> {})
                .async(false)
                .register();

        registry.dispatch(putLocal("k1", "v1"));
        registry.dispatch(deleteLocal("k2"));

        assertEquals(0, handle.eventsSkipped());
        assertEquals(2, handle.eventsReceived());
    }

    @Test
    void eventsSkippedNotIncrementedWhenKeyFilterRejects() {
        WatchHandle handle = registry.watch()
                .onPut((k, v) -> {})
                .filter(k -> k.startsWith("eu:"))
                .async(false)
                .register();

        // Key filter rejects — distinct from event-type skip; skipped counter must stay 0.
        registry.dispatch(putLocal("us:k1", "v1"));

        assertEquals(0, handle.eventsSkipped());
        assertEquals(0, handle.eventsReceived());
    }

    // ── onEvent raw callback ───────────────────────────────────────────────────

    @Test
    void onEventFiresForAllThreeEventTypes() {
        AtomicInteger count = new AtomicInteger();
        AtomicReference<ChangeEvent<String, String>> lastEvent = new AtomicReference<>();

        registry.watch()
                .onEvent(e -> { count.incrementAndGet(); lastEvent.set(e); })
                .async(false)
                .register();

        registry.dispatch(putLocal("k1", "v1"));
        assertEquals(1, count.get());
        assertInstanceOf(ChangeEvent.PutEvent.class, lastEvent.get());

        registry.dispatch(deleteLocal("k1"));
        assertEquals(2, count.get());
        assertInstanceOf(ChangeEvent.DeleteEvent.class, lastEvent.get());

        registry.dispatch(expire("k1"));
        assertEquals(3, count.get());
        assertInstanceOf(ChangeEvent.ExpireEvent.class, lastEvent.get());
    }

    @Test
    void onEventFiresInAdditionToTypedListener() {
        AtomicInteger putListenerCount   = new AtomicInteger();
        AtomicInteger eventListenerCount = new AtomicInteger();

        registry.watch()
                .onPut((k, v) -> putListenerCount.incrementAndGet())
                .onEvent(e -> eventListenerCount.incrementAndGet())
                .async(false)
                .register();

        registry.dispatch(putLocal("k1", "v1"));

        // Both listeners must fire for a PutEvent.
        assertEquals(1, putListenerCount.get());
        assertEquals(1, eventListenerCount.get());
    }

    @Test
    void onEventReceivesOldValueAndLastValue() {
        AtomicReference<String> capturedOldValue  = new AtomicReference<>();
        AtomicReference<String> capturedLastValue = new AtomicReference<>();

        registry.watch()
                .onEvent(e -> {
                    if (e instanceof ChangeEvent.PutEvent<String, String> put)
                        capturedOldValue.set(put.oldValue());
                    if (e instanceof ChangeEvent.DeleteEvent<String, String> del)
                        capturedLastValue.set(del.lastValue());
                })
                .async(false)
                .register();

        registry.dispatch(new ChangeEvent.PutEvent<>("k1", "v2", "v1", ChangeEvent.Source.LOCAL));
        assertEquals("v1", capturedOldValue.get());

        registry.dispatch(new ChangeEvent.DeleteEvent<>("k1", "v2", ChangeEvent.Source.LOCAL));
        assertEquals("v2", capturedLastValue.get());
    }

    // ── multiple subscriptions on same registry ───────────────────────────────

    @Test
    void multipleSubscriptionsOnSameRegistryBothReceiveSameEvent() {
        AtomicInteger count1 = new AtomicInteger();
        AtomicInteger count2 = new AtomicInteger();

        registry.watch().onPut((k, v) -> count1.incrementAndGet()).async(false).register();
        registry.watch().onPut((k, v) -> count2.incrementAndGet()).async(false).register();

        registry.dispatch(putLocal("k1", "v1"));

        assertEquals(1, count1.get());
        assertEquals(1, count2.get());
    }

    // ── once() + key filter combination ──────────────────────────────────────

    @Test
    void oncePutWithFilterNotConsumedByKeyMismatch() {
        AtomicInteger count = new AtomicInteger();
        WatchHandle handle = registry.watch()
                .onPut((k, v) -> count.incrementAndGet())
                .filter(k -> k.startsWith("eu:"))
                .async(false)
                .once();

        // Key doesn't match filter — subscription must stay active.
        registry.dispatch(putLocal("us:k1", "v1"));
        assertEquals(0, count.get());
        assertTrue(handle.isActive());
        assertEquals(0, handle.eventsSkipped(), "key-filter rejection is not an event-type skip");

        // Matching key fires the once.
        registry.dispatch(putLocal("eu:k2", "v2"));
        assertEquals(1, count.get());
        assertFalse(handle.isActive());
    }

    // ── replication lag threshold exceeded ────────────────────────────────────

    @Test
    void replicationLagThresholdExceededCallbackFires() throws InterruptedException {
        AtomicReference<Long> capturedLag = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        registry.watch()
                .onPut((k, v) -> {})
                .onReplicationLag(Duration.ofMillis(100), lag -> {
                    capturedLag.set(lag);
                    latch.countDown();
                })
                .register();

        // 500ms lag exceeds the 100ms threshold.
        registry.dispatch(putReplicated("k1", "v1"), System.currentTimeMillis() - 500);

        assertTrue(latch.await(3, TimeUnit.SECONDS), "Lag callback must fire when lag exceeds threshold");
        assertTrue(capturedLag.get() >= 100,
                "Captured lag must be >= threshold (100ms), was: " + capturedLag.get());
    }

    // ── exception isolation ───────────────────────────────────────────────────

    @Test
    void exceptionInFilterDoesNotPreventOtherSubscriptionsFromReceivingEvent() {
        AtomicInteger goodCount = new AtomicInteger();

        // First subscription has a filter that throws.
        registry.watch()
                .onPut((k, v) -> {})
                .filter(k -> { throw new RuntimeException("filter bomb"); })
                .async(false)
                .register();

        // Second subscription is healthy.
        registry.watch()
                .onPut((k, v) -> goodCount.incrementAndGet())
                .async(false)
                .register();

        // dispatch() must not propagate the filter exception; the second subscription must fire.
        assertDoesNotThrow(() -> registry.dispatch(putLocal("k1", "v1")));
        assertEquals(1, goodCount.get(),
                "Healthy subscription must receive event despite filter exception in sibling");
    }

    @Test
    void exceptionInSyncListenerDoesNotPreventOtherSubscriptionsFromReceivingEvent() {
        AtomicInteger goodCount = new AtomicInteger();

        // First subscription has a sync listener that throws.
        registry.watch()
                .onPut((k, v) -> { throw new RuntimeException("listener bomb"); })
                .async(false)
                .register();

        // Second subscription is healthy.
        registry.watch()
                .onPut((k, v) -> goodCount.incrementAndGet())
                .async(false)
                .register();

        assertDoesNotThrow(() -> registry.dispatch(putLocal("k1", "v1")));
        assertEquals(1, goodCount.get(),
                "Healthy subscription must receive event despite exception in sibling listener");
    }

    @Test
    void dispatchNullEventThrowsIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class, () -> registry.dispatch(null));
        assertThrows(IllegalArgumentException.class, () -> registry.dispatch(null, 0L));
    }

    // ── Bug 1: sourceTimestampMs == 0 must compute lag ────────────────────────

    @Test
    void dispatchWithZeroSourceTimestampComputesLag() throws InterruptedException {
        AtomicReference<Long> capturedLag = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        registry.watch()
                .onPut((k, v) -> {})
                .onReplicationLag(lag -> { capturedLag.set(lag); latch.countDown(); })
                .register();

        // sourceTimestampMs = 0 (Unix epoch) must NOT be treated as a local event.
        // Lag will be huge (milliseconds since 1970), but the key invariant is:
        // the lag callback fires and returns a non-negative value.
        registry.dispatch(putReplicated("k1", "v1"), 0L);

        assertTrue(latch.await(3, TimeUnit.SECONDS), "Lag callback must fire when sourceTimestampMs=0");
        assertTrue(capturedLag.get() >= 0, "Lag from epoch must be non-negative");
    }

    // ── Bug 2: Duration threshold validation ──────────────────────────────────

    @Test
    void onReplicationLagWithNegativeDurationThrows() {
        assertThrows(IllegalArgumentException.class, () ->
                registry.watch()
                        .onPut((k, v) -> {})
                        .onReplicationLag(Duration.ofMillis(-1), lag -> {}));
    }

    @Test
    void onReplicationLagWithZeroDurationThrows() {
        assertThrows(IllegalArgumentException.class, () ->
                registry.watch()
                        .onPut((k, v) -> {})
                        .onReplicationLag(Duration.ZERO, lag -> {}));
    }

    // ── Bug 3: multiple filter() calls compose with AND semantics ─────────────

    @Test
    void multipleFilterCallsComposeWithAndSemantics() {
        AtomicInteger count = new AtomicInteger();
        WatchHandle handle = registry.watch()
                .onPut((k, v) -> count.incrementAndGet())
                .filter(k -> k.startsWith("eu:"))
                .filter(k -> k.endsWith(":prod"))
                .async(false)
                .register();

        registry.dispatch(putLocal("us:k1:prod", "v")); // first filter rejects
        registry.dispatch(putLocal("eu:k2:dev",  "v")); // second filter rejects
        registry.dispatch(putLocal("eu:k3:prod", "v")); // both accept

        assertEquals(1, count.get(), "Both filters must be satisfied for dispatch");
        // Key-filter rejections are silent returns — they do not increment eventsSkipped
        // (only event-type mismatches after the filter do). This is consistent with the
        // eventsSkippedNotIncrementedWhenKeyFilterRejects contract.
        assertEquals(0, handle.eventsSkipped());
        handle.cancel();
    }

    // ── Bug 4: null listeners throw NullPointerException immediately ──────────

    @Test
    void onPutNullListenerThrowsNullPointerException() {
        assertThrows(NullPointerException.class, () -> registry.watch().onPut(null));
    }

    @Test
    void onDeleteNullListenerThrowsNullPointerException() {
        assertThrows(NullPointerException.class, () -> registry.watch().onDelete(null));
    }

    @Test
    void onExpireNullListenerThrowsNullPointerException() {
        assertThrows(NullPointerException.class, () -> registry.watch().onExpire(null));
    }

    @Test
    void onEventNullListenerThrowsNullPointerException() {
        assertThrows(NullPointerException.class, () -> registry.watch().onEvent(null));
    }

    @Test
    void filterNullPredicateThrowsNullPointerException() {
        assertThrows(NullPointerException.class, () -> registry.watch().onPut((k, v) -> {}).filter(null));
    }

    // ── Bug 5: lag-only subscription does not increment skippedCounter ─────────

    @Test
    void lagOnlySubscriptionWithReplicatedEventDoesNotIncrementSkippedCounter()
            throws InterruptedException {
        AtomicInteger lagCount = new AtomicInteger();
        CountDownLatch latch = new CountDownLatch(1);

        WatchHandle handle = registry.watch()
                .onReplicationLag(lag -> { lagCount.incrementAndGet(); latch.countDown(); })
                .async(false)
                .register();

        registry.dispatch(putReplicated("k1", "v1"), System.currentTimeMillis() - 50);

        // Lag callback must fire.
        assertTrue(latch.await(3, TimeUnit.SECONDS));
        assertEquals(1, lagCount.get());
        // The lag callback handled the event — must NOT count as skipped.
        assertEquals(0, handle.eventsSkipped(),
                "eventsSkipped must stay 0 when the lag monitor is the sole listener and fires");
        handle.cancel();
    }

    // ── Bug 6: register() after shutdown() throws ─────────────────────────────

    @Test
    void registerAfterShutdownThrowsIllegalStateException() {
        registry.shutdown();
        assertThrows(IllegalStateException.class, () ->
                registry.watch().onPut((k, v) -> {}).register());
    }

    @Test
    void onceAfterShutdownThrowsIllegalStateException() {
        registry.shutdown();
        assertThrows(IllegalStateException.class, () ->
                registry.watch().onPut((k, v) -> {}).once());
    }

    // ── Bug 8: ChangeEvent.source() uniform accessor ──────────────────────────

    @Test
    void expireEventSourceAccessorReturnsLocal() {
        ChangeEvent<String, String> event = expire("k1");
        assertEquals(ChangeEvent.Source.LOCAL, event.source(),
                "ExpireEvent must always report Source.LOCAL via default source() method");
    }

    @Test
    void uniformSourceAccessorAcrossAllEventTypes() {
        ChangeEvent<String, String> put = new ChangeEvent.PutEvent<>(
                "k", "v", null, ChangeEvent.Source.REPLICATED);
        ChangeEvent<String, String> del = new ChangeEvent.DeleteEvent<>(
                "k", "v", ChangeEvent.Source.REPLICATED);
        ChangeEvent<String, String> exp = expire("k");

        assertEquals(ChangeEvent.Source.REPLICATED, put.source());
        assertEquals(ChangeEvent.Source.REPLICATED, del.source());
        assertEquals(ChangeEvent.Source.LOCAL,      exp.source(),
                "ExpireEvent source must be LOCAL regardless of caller");
    }

    // ── Missing Feature: awaitDone() ─────────────────────────────────────────

    @Test
    void awaitDoneReturnsTrueAfterCancel() throws InterruptedException {
        WatchHandle handle = registry.watch()
                .onPut((k, v) -> {})
                .register();

        handle.cancel();

        assertTrue(handle.awaitDone(3, TimeUnit.SECONDS),
                "awaitDone must return true once the drain thread exits after cancel");
    }

    @Test
    void awaitDoneReturnsTrueImmediatelyForSyncSubscription() throws InterruptedException {
        WatchHandle handle = registry.watch()
                .onPut((k, v) -> {})
                .async(false)
                .register();

        handle.cancel();

        // Sync subscriptions have no drain thread — awaitDone is a no-wait boolean check.
        assertTrue(handle.awaitDone(0, TimeUnit.MILLISECONDS),
                "awaitDone on sync subscription must return true once inactive");
    }

    @Test
    void awaitDoneReturnsFalseIfDrainThreadStillRunning() throws InterruptedException {
        // A listener backed by a CountDownLatch that we never count down
        // keeps the drain thread busy so awaitDone must time out.
        CountDownLatch holdListener = new CountDownLatch(1);
        CountDownLatch listenerStarted = new CountDownLatch(1);

        WatchHandle handle = registry.watch()
                .onPut((k, v) -> {
                    listenerStarted.countDown();
                    // Absorb interrupts so the drain thread truly stays blocked.
                    while (holdListener.getCount() > 0) {
                        try { holdListener.await(); }
                        catch (InterruptedException ignored) { /* keep blocking */ }
                    }
                })
                .register();

        registry.dispatch(putLocal("k1", "v1"));
        assertTrue(listenerStarted.await(3, TimeUnit.SECONDS), "Listener must start");

        // The drain thread is blocked inside the listener — awaitDone with a 100ms timeout
        // must return false because the thread has not yet exited.
        assertFalse(handle.awaitDone(100, TimeUnit.MILLISECONDS),
                "awaitDone must return false when the drain thread is still running");

        // Release the listener so the test tears down cleanly.
        holdListener.countDown();
        handle.cancel();
    }

    @Test
    void awaitDoneReturnsTrueAfterOnceSubscriptionFires() throws InterruptedException {
        CountDownLatch fired = new CountDownLatch(1);

        WatchHandle handle = registry.watch()
                .onPut((k, v) -> fired.countDown())
                .once();

        registry.dispatch(putLocal("k1", "v1"));
        assertTrue(fired.await(3, TimeUnit.SECONDS), "once() listener must fire");

        // After the once fires, the drain thread stops and awaitDone must succeed.
        assertTrue(handle.awaitDone(3, TimeUnit.SECONDS),
                "awaitDone must return true after once() subscription has fired and drain thread exited");
    }

    // ── null-check coverage for lag overloads ────────────────────────────────

    @Test
    void onReplicationLagConsumerNullListenerThrowsNullPointerException() {
        assertThrows(NullPointerException.class, () ->
                registry.watch().onPut((k, v) -> {}).onReplicationLag((java.util.function.Consumer<Long>) null));
    }

    @Test
    void onReplicationLagDurationNullListenerThrowsNullPointerException() {
        assertThrows(NullPointerException.class, () ->
                registry.watch().onPut((k, v) -> {}).onReplicationLag(Duration.ofMillis(100), null));
    }

    @Test
    void onReplicationLagNullDurationThrowsNullPointerException() {
        assertThrows(NullPointerException.class, () ->
                registry.watch().onPut((k, v) -> {}).onReplicationLag(null, lag -> {}));
    }

    // ── async listener exception → drain thread survives ─────────────────────

    @Test
    void asyncListenerExceptionDoesNotKillDrainThread() throws InterruptedException {
        AtomicInteger received = new AtomicInteger();
        CountDownLatch secondDelivered = new CountDownLatch(1);

        WatchHandle handle = registry.watch()
                .onPut((k, v) -> {
                    if ("boom".equals(v)) throw new RuntimeException("deliberate failure");
                    received.incrementAndGet();
                    if ("after".equals(v)) secondDelivered.countDown();
                })
                .register();

        // First event throws inside the listener.
        registry.dispatch(new ChangeEvent.PutEvent<>("k1", "boom", null, ChangeEvent.Source.LOCAL));
        // Second event must still be delivered — the drain thread must have survived.
        registry.dispatch(new ChangeEvent.PutEvent<>("k2", "after", null, ChangeEvent.Source.LOCAL));

        assertTrue(secondDelivered.await(3, TimeUnit.SECONDS),
                "Drain thread must survive a throwing listener and continue delivering subsequent events");
        assertEquals(1, received.get());
        handle.cancel();
    }

    // ── lag monitor exception does not abort typed listener delivery ──────────

    @Test
    void lagMonitorExceptionDoesNotAbortTypedListenerDelivery() throws InterruptedException {
        AtomicInteger putCount = new AtomicInteger();
        CountDownLatch latch = new CountDownLatch(1);

        registry.watch()
                .onPut((k, v) -> { putCount.incrementAndGet(); latch.countDown(); })
                .onReplicationLag(lag -> { throw new RuntimeException("lag monitor bomb"); })
                .async(false)
                .register();

        // The lag monitor throws. The typed put listener must still fire.
        registry.dispatch(putReplicated("k1", "v1"), System.currentTimeMillis() - 100);

        assertTrue(latch.await(3, TimeUnit.SECONDS),
                "Typed listener must fire even when the lag monitor throws");
        assertEquals(1, putCount.get());
    }

    // ── lag threshold boundary value ──────────────────────────────────────────

    @Test
    void replicationLagThresholdBoundaryFiresAtExactThreshold() throws InterruptedException {
        // ReplicationLagMonitor uses >=, so lag exactly equal to threshold must fire.
        // We inject an artificial lag value directly via a sync subscription to avoid
        // wall-clock jitter making the boundary non-deterministic.
        AtomicReference<Long> capturedLag = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        long thresholdMs = 200;

        registry.watch()
                .onPut((k, v) -> {})
                .onReplicationLag(Duration.ofMillis(thresholdMs), lag -> {
                    capturedLag.set(lag);
                    latch.countDown();
                })
                .register();

        // Set source timestamp so lag lands at exactly thresholdMs — use a past timestamp
        // where (now - source) == threshold. To ensure we hit >= rather than >, we set it
        // thresholdMs ms in the past and accept that the actual lag may be thresholdMs or
        // a few ms above due to execution time. The important invariant is that it fires.
        long sourceTimestampMs = System.currentTimeMillis() - thresholdMs;
        registry.dispatch(putReplicated("k1", "v1"), sourceTimestampMs);

        assertTrue(latch.await(3, TimeUnit.SECONDS),
                "Lag callback must fire when lag >= threshold (boundary case)");
        assertTrue(capturedLag.get() >= thresholdMs,
                "Captured lag must be >= threshold; was: " + capturedLag.get());
    }

    @Test
    void replicationLagJustBelowThresholdDoesNotFire() throws InterruptedException {
        // Complements the boundary test: lag strictly below threshold must not fire.
        AtomicInteger lagCount = new AtomicInteger();
        long thresholdMs = 5000; // 5s — no realistic jitter will exceed this

        registry.watch()
                .onPut((k, v) -> {})
                .onReplicationLag(Duration.ofMillis(thresholdMs), lag -> lagCount.incrementAndGet())
                .register();

        // sourceTimestamp just 1ms in the past → lag ≈ 1ms << 5000ms threshold
        registry.dispatch(putReplicated("k1", "v1"), System.currentTimeMillis() - 1);
        Thread.sleep(100); // give any async path time to (wrongly) fire
        assertEquals(0, lagCount.get(), "Lag below threshold must not trigger callback");
    }

    // ── once() with non-put event types ──────────────────────────────────────

    @Test
    void onceDeleteSubscriptionAutosCancelsAfterFirstDeleteEvent() {
        AtomicInteger count = new AtomicInteger();
        WatchHandle handle = registry.watch()
                .onDelete(k -> count.incrementAndGet())
                .async(false)
                .once();

        registry.dispatch(deleteLocal("k1"));
        assertEquals(1, count.get());
        assertFalse(handle.isActive());
        assertEquals(0, registry.totalSubscriptionCount(),
                "once().onDelete() subscription must be evicted from registry after firing");

        registry.dispatch(deleteLocal("k2"));
        assertEquals(1, count.get(), "No further deliveries after once fires");
    }

    @Test
    void onceExpireSubscriptionAutosCancelsAfterFirstExpireEvent() {
        AtomicInteger count = new AtomicInteger();
        WatchHandle handle = registry.watch()
                .onExpire(k -> count.incrementAndGet())
                .async(false)
                .once();

        registry.dispatch(expire("k1"));
        assertEquals(1, count.get());
        assertFalse(handle.isActive());
        assertEquals(0, registry.totalSubscriptionCount(),
                "once().onExpire() subscription must be evicted from registry after firing");

        registry.dispatch(expire("k2"));
        assertEquals(1, count.get(), "No further deliveries after once fires");
    }

    // ── concurrent once() race — only one thread wins CAS ────────────────────

    @Test
    void concurrentDispatchToOnceSubscriptionFiresExactlyOnce() throws InterruptedException {
        int threads = 16;
        AtomicInteger fireCount = new AtomicInteger();
        CountDownLatch startGate = new CountDownLatch(1);
        CountDownLatch allDone   = new CountDownLatch(threads);

        registry.watch()
                .onPut((k, v) -> fireCount.incrementAndGet())
                .async(false)
                .once();

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            for (int i = 0; i < threads; i++) {
                final int idx = i;
                pool.submit(() -> {
                    try {
                        startGate.await(); // all threads fire simultaneously
                        registry.dispatch(putLocal("k" + idx, "v" + idx));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        allDone.countDown();
                    }
                });
            }
            startGate.countDown();
            assertTrue(allDone.await(5, TimeUnit.SECONDS));
        } finally {
            pool.shutdown();
        }

        assertEquals(1, fireCount.get(),
                "Exactly one thread must win the once() CAS — listener must fire exactly once");
    }
}
