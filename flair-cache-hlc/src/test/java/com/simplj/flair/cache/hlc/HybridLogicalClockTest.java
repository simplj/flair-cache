package com.simplj.flair.cache.hlc;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

class HybridLogicalClockTest {

    // ── Monotonicity ───────────────────────────────────────────────────────────

    @Test
    void monotonicity_1M_sequentialCalls_strictlyIncreasing() {
        HybridLogicalClock hlc = new HybridLogicalClock();
        HLCTimestamp prev = hlc.now();
        for (int i = 0; i < 1_000_000; i++) {
            HLCTimestamp next = hlc.now();
            assertTrue(next.isAfter(prev), "Not strictly increasing at iteration " + i);
            prev = next;
        }
    }

    // ── Causality ──────────────────────────────────────────────────────────────

    @Test
    void causality_threeNodeChain_orderingPreserved() {
        AtomicLong wallA = new AtomicLong(1000L);
        AtomicLong wallB = new AtomicLong(998L);
        AtomicLong wallC = new AtomicLong(999L);

        HybridLogicalClock hlcA = new HybridLogicalClock(wallA::get, new ClockDriftMonitor());
        HybridLogicalClock hlcB = new HybridLogicalClock(wallB::get, new ClockDriftMonitor());
        HybridLogicalClock hlcC = new HybridLogicalClock(wallC::get, new ClockDriftMonitor());

        // A sends to B; B sends to C
        HLCTimestamp sendA = hlcA.now();
        hlcB.update(sendA);
        HLCTimestamp sendB = hlcB.now();
        hlcC.update(sendB);
        HLCTimestamp sendC = hlcC.now();

        assertTrue(sendB.isAfter(sendA), "sendB must be causally after sendA");
        assertTrue(sendC.isAfter(sendB), "sendC must be causally after sendB");
        assertTrue(sendC.isAfter(sendA), "sendC must be causally after sendA (transitivity)");
    }

    // ── Backward wall-clock jump ───────────────────────────────────────────────

    @Test
    void backwardClockJump_logicalFloorHolds() {
        AtomicLong wall = new AtomicLong(1000L);
        HybridLogicalClock hlc = new HybridLogicalClock(wall::get, new ClockDriftMonitor());

        HLCTimestamp before = hlc.now();
        assertEquals(1000L, before.logical());

        // Simulate NTP backward correction
        wall.set(800L);
        HLCTimestamp after = hlc.now();

        assertEquals(1000L, after.logical(), "Logical floor must not regress after backward jump");
        assertTrue(after.isAfter(before), "Clock must remain monotonically increasing");
    }

    @Test
    void backwardClockJump_noDriftAlert() {
        AtomicLong wall = new AtomicLong(1000L);
        ClockDriftMonitor monitor = new ClockDriftMonitor(5_000L);
        HybridLogicalClock hlc = new HybridLogicalClock(wall::get, monitor);

        hlc.now();
        wall.set(500L);
        hlc.now();

        assertEquals(0, monitor.alertCount(), "Backward jump must not trigger drift alert");
    }

    // ── Forward wall-clock jump ────────────────────────────────────────────────

    @Test
    void forwardClockJump_absorbed_driftAlertFired() {
        AtomicLong wall = new AtomicLong(1_000_000L);
        ClockDriftMonitor monitor = new ClockDriftMonitor(5_000L);
        HybridLogicalClock hlc = new HybridLogicalClock(wall::get, monitor);

        hlc.now(); // initialize logical = 1_000_000

        // NTP jumps wall forward by 10s (> 5s threshold)
        wall.set(1_010_000L);
        HLCTimestamp after = hlc.now();

        assertEquals(1_010_000L, after.logical(), "Logical must absorb forward jump");
        assertTrue(monitor.alertCount() > 0, "Forward jump beyond threshold must fire drift alert");
        assertTrue(monitor.maxObservedDriftMs() >= 10_000L, "Max drift must be recorded");
    }

    @Test
    void forwardClockJump_belowThreshold_noAlert() {
        AtomicLong wall = new AtomicLong(1_000_000L);
        ClockDriftMonitor monitor = new ClockDriftMonitor(5_000L);
        HybridLogicalClock hlc = new HybridLogicalClock(wall::get, monitor);

        hlc.now();
        wall.set(1_003_000L); // 3s jump, below 5s threshold
        hlc.now();

        assertEquals(0, monitor.alertCount(), "Jump below threshold must not alert");
    }

    // ── update() semantics ─────────────────────────────────────────────────────

    @Test
    void update_remoteFarAhead_localMovesForward() {
        AtomicLong wall = new AtomicLong(500L);
        HybridLogicalClock hlc = new HybridLogicalClock(wall::get, new ClockDriftMonitor());

        HLCTimestamp remote = new HLCTimestamp(2000L, 3L);
        hlc.update(remote);
        HLCTimestamp local = hlc.now();

        assertTrue(local.isAfter(remote), "Local event after update must be causally after remote");
    }

    @Test
    void update_sameLogical_counterExceedsRemote() {
        AtomicLong wall = new AtomicLong(1000L);
        HybridLogicalClock hlc = new HybridLogicalClock(wall::get, new ClockDriftMonitor());

        hlc.now(); // logical = 1000, counter = 0

        HLCTimestamp remote = new HLCTimestamp(1000L, 10L);
        hlc.update(remote);
        HLCTimestamp next = hlc.now();

        assertTrue(next.counter() > 10L, "Counter must exceed remote counter after update on same logical");
    }

    @Test
    void update_multipleRemotes_monotonicity() {
        AtomicLong wall = new AtomicLong(1000L);
        HybridLogicalClock hlc = new HybridLogicalClock(wall::get, new ClockDriftMonitor());

        HLCTimestamp r1 = new HLCTimestamp(900L, 0L);
        HLCTimestamp r2 = new HLCTimestamp(1200L, 5L);
        HLCTimestamp r3 = new HLCTimestamp(800L,  9L);

        hlc.update(r1);
        HLCTimestamp t1 = hlc.now();
        hlc.update(r2);
        HLCTimestamp t2 = hlc.now();
        hlc.update(r3);
        HLCTimestamp t3 = hlc.now();

        assertTrue(t2.isAfter(t1));
        assertTrue(t3.isAfter(t2));
    }

    // ── Counter overflow guard ─────────────────────────────────────────────────

    @Test
    void now_counterOverflow_throws() {
        AtomicLong wall = new AtomicLong(1000L);
        HybridLogicalClock hlc = new HybridLogicalClock(wall::get, new ClockDriftMonitor());

        // Seed logical=1000 then push counter to Long.MAX_VALUE via update
        hlc.now(); // logical=1000, counter=0
        hlc.update(new HLCTimestamp(1000L, Long.MAX_VALUE - 1)); // counter=Long.MAX_VALUE

        // The next now() would increment Long.MAX_VALUE — must throw
        assertThrows(IllegalStateException.class, hlc::now,
                "Counter overflow must throw IllegalStateException");
    }

    @Test
    void update_counterOverflow_throws() {
        AtomicLong wall = new AtomicLong(1000L);
        HybridLogicalClock hlc = new HybridLogicalClock(wall::get, new ClockDriftMonitor());
        hlc.now(); // logical = 1000, counter = 0

        // Remote has same logical and counter at MAX_VALUE → update would need MAX_VALUE + 1
        HLCTimestamp remote = new HLCTimestamp(1000L, Long.MAX_VALUE);
        assertThrows(IllegalStateException.class, () -> hlc.update(remote),
                "Counter overflow in update() must throw IllegalStateException");
    }

    // ── Drift alert from update() ──────────────────────────────────────────────

    @Test
    void update_remoteFarAhead_driftAlertFired() {
        AtomicLong wall = new AtomicLong(500L);
        ClockDriftMonitor monitor = new ClockDriftMonitor(5_000L);
        HybridLogicalClock hlc = new HybridLogicalClock(wall::get, monitor);

        // Remote logical is 10s ahead of our wall — should fire drift alert
        HLCTimestamp remote = new HLCTimestamp(10_500L, 0L);
        hlc.update(remote);

        assertTrue(monitor.alertCount() > 0,
                "Remote-driven forward leap must fire drift alert in update()");
    }

    @Test
    void update_remoteSlightlyAhead_noDriftAlert() {
        AtomicLong wall = new AtomicLong(1_000_000L);
        ClockDriftMonitor monitor = new ClockDriftMonitor(5_000L);
        HybridLogicalClock hlc = new HybridLogicalClock(wall::get, monitor);

        // Remote is only 2s ahead — below threshold
        HLCTimestamp remote = new HLCTimestamp(1_002_000L, 0L);
        hlc.update(remote);

        assertEquals(0, monitor.alertCount(),
                "Remote slightly ahead (below threshold) must not fire drift alert");
    }

    // ── Thread safety ──────────────────────────────────────────────────────────

    @Test
    void threadSafety_noDuplicates() throws InterruptedException {
        HybridLogicalClock hlc = new HybridLogicalClock();
        int threads = 8;
        int callsPerThread = 100_000;
        CyclicBarrier barrier = new CyclicBarrier(threads);
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        List<Future<List<HLCTimestamp>>> futures = new ArrayList<>(threads);

        for (int t = 0; t < threads; t++) {
            futures.add(executor.submit(() -> {
                barrier.await();
                List<HLCTimestamp> results = new ArrayList<>(callsPerThread);
                for (int i = 0; i < callsPerThread; i++) {
                    results.add(hlc.now());
                }
                return results;
            }));
        }

        executor.shutdown();
        assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));

        Set<HLCTimestamp> all = new HashSet<>(threads * callsPerThread);
        for (Future<List<HLCTimestamp>> f : futures) {
            try {
                for (HLCTimestamp ts : f.get()) {
                    assertTrue(all.add(ts), "Duplicate timestamp: " + ts);
                }
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
        }

        assertEquals(threads * callsPerThread, all.size());
    }
}
