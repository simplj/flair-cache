package com.simplj.flair.cache.hlc;

import java.util.function.LongSupplier;

public final class HybridLogicalClock {

    private final LongSupplier wallClock;
    private final ClockDriftMonitor driftMonitor;
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
    public synchronized HLCTimestamp now() {
        long wall = wallClock.getAsLong();
        driftMonitor.checkDrift(wall, logical);
        if (wall > logical) {
            logical = wall;
            counter = 0;
        } else {
            counter = safeIncrement(counter);
        }
        return new HLCTimestamp(logical, counter);
    }

    /**
     * Updates the clock upon receiving a remote event. Must be called before applying the event.
     */
    public synchronized void update(HLCTimestamp remote) {
        long wall   = wallClock.getAsLong();
        driftMonitor.checkRemoteDrift(remote.logical(), wall);
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
