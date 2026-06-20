package com.simplj.flair.cache.hlc;

import java.util.concurrent.locks.StampedLock;
import java.util.function.LongSupplier;

public final class HybridLogicalClock {

    private final LongSupplier wallClock;
    private final ClockDriftMonitor driftMonitor;
    // StampedLock.writeLock() spin-then-parks under contention, eliminating the OS monitor
    // inflation that caused 88–100µs p99 spikes under 8-thread load with synchronized.
    // Wall-clock read and drift check (Fix A) occur outside the lock to shrink the critical
    // section to pure state mutation — no object allocation per retry, one HLCTimestamp per call.
    // Chosen over AtomicReference+CAS (Fix B) to avoid per-retry HLCTimestamp allocation.
    private final StampedLock lock = new StampedLock();
    private long logical;
    private long counter;

    public HybridLogicalClock() {
        this(System::currentTimeMillis, new ClockDriftMonitor());
    }

    HybridLogicalClock(LongSupplier wallClock, ClockDriftMonitor driftMonitor) {
        this.wallClock = wallClock;
        this.driftMonitor = driftMonitor;
    }

    /**
     * Generates a new timestamp for a local event (write / send).
     * Must be called before the event is applied or sent.
     */
    public HLCTimestamp now() {
        long wall = wallClock.getAsLong();
        // Drift check reads logical without holding the lock. The read is non-volatile but
        // monitoring-only: a transiently stale value may cause a missed or spurious alert,
        // never a correctness issue. Acceptable on 64-bit JVMs where long reads are atomic in practice.
        driftMonitor.checkDrift(wall, logical);
        long stamp = lock.writeLock();
        try {
            if (wall > logical) {
                logical = wall;
                counter = 0;
            } else {
                counter = safeIncrement(counter);
            }
            return new HLCTimestamp(logical, counter);
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    /**
     * Updates the clock upon receiving a remote event. Must be called before applying the event.
     */
    public void update(HLCTimestamp remote) {
        long wall = wallClock.getAsLong();
        driftMonitor.checkRemoteDrift(remote.logical(), wall);
        long stamp = lock.writeLock();
        try {
            long newLog = Math.max(Math.max(logical, remote.logical()), wall);
            if (newLog == logical && newLog == remote.logical()) {
                counter = safeIncrement(Math.max(counter, remote.counter()));
            } else if (newLog == logical) {
                counter = safeIncrement(counter);
            } else if (newLog == remote.logical()) {
                counter = safeIncrement(remote.counter());
            } else {
                counter = 0;
            }
            logical = newLog;
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    public ClockDriftMonitor driftMonitor() {
        return driftMonitor;
    }

    private static long safeIncrement(long value) {
        if (value == Long.MAX_VALUE) {
            throw new IllegalStateException(
                    "HLC counter overflow: too many events within a single logical millisecond");
        }
        return value + 1;
    }
}
