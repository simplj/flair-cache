package com.simplj.flair.cache.store;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Runs on thread {@code flaircache-expiry-sweep}. Periodically removes TTL-expired
 * entries from the store. Lazy expiry on get() handles the hot path; this sweep
 * reclaims memory for entries that are never read again.
 */
final class ExpiryManager {

    private static final Logger log = Logger.getLogger(ExpiryManager.class.getName());

    static final long DEFAULT_SWEEP_INTERVAL_MS = 500L;

    private final LocalStore store;
    private final long sweepIntervalMs;
    private final ScheduledExecutorService scheduler;

    ExpiryManager(LocalStore store, long sweepIntervalMs) {
        this.store          = store;
        this.sweepIntervalMs = sweepIntervalMs;
        this.scheduler      = Executors.newSingleThreadScheduledExecutor(
                new FlairCacheThreadFactory("flaircache-expiry-sweep"));
    }

    void start() {
        scheduler.scheduleWithFixedDelay(
                this::sweep, sweepIntervalMs, sweepIntervalMs, TimeUnit.MILLISECONDS);
    }

    void stop() {
        scheduler.shutdown();
        // If stop() is called from within a sweep callback (e.g. an ExpireListener fires
        // cacheBlock.close()), awaitTermination() would deadlock — the sweep thread waiting for itself.
        // Detect by name; the thread name is stable (set by FlairCacheThreadFactory).
        if (Thread.currentThread().getName().startsWith("flaircache-expiry-sweep")) {
            return;
        }
        try {
            if (!scheduler.awaitTermination(2, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private void sweep() {
        try {
            store.sweepExpired(System.currentTimeMillis());
        } catch (Exception e) {
            log.log(Level.WARNING, "Expiry sweep failed", e);
        }
    }
}
