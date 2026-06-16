package com.simplj.flair.cache.replication;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

final class AckTracker {

    private static final Logger log = Logger.getLogger(AckTracker.class.getName());
    private static final long SWEEP_INTERVAL_MS = 100L;

    private final ConcurrentHashMap<Long, PendingWrite> pending = new ConcurrentHashMap<>();
    private final ScheduledExecutorService sweeper;

    AckTracker() {
        sweeper = Executors.newSingleThreadScheduledExecutor(
                new FlairCacheThreadFactory("flaircache-ack-sweep"));
        sweeper.scheduleAtFixedRate(this::sweep, SWEEP_INTERVAL_MS, SWEEP_INTERVAL_MS,
                TimeUnit.MILLISECONDS);
    }

    void track(PendingWrite pw) {
        pending.put(pw.frameId, pw);
    }

    void onAck(long frameId) {
        PendingWrite pw = pending.get(frameId);
        if (pw == null) return;
        if (pw.receivedAcks.incrementAndGet() >= pw.requiredAcks) {
            if (pending.remove(frameId, pw)) {
                pw.future.complete(null);
            }
        }
    }

    void cancel(long frameId) {
        PendingWrite pw = pending.remove(frameId);
        if (pw != null) {
            pw.future.cancel(false);
        }
    }

    int pendingCount() {
        return pending.size();
    }

    void shutdown() {
        sweeper.shutdownNow();
        for (PendingWrite pw : pending.values()) {
            pw.future.completeExceptionally(
                    new ReplicationTimeoutException("Replication engine shutting down"));
        }
        pending.clear();
    }

    private void sweep() {
        long now = System.currentTimeMillis();
        for (Map.Entry<Long, PendingWrite> e : pending.entrySet()) {
            PendingWrite pw = e.getValue();
            if (now >= pw.expiryMs) {
                if (pending.remove(e.getKey(), pw)) {
                    if (log.isLoggable(Level.FINE)) {
                        log.fine("ACK timeout for frameId=" + pw.frameId
                                + " after waiting for " + pw.requiredAcks + " ACKs,"
                                + " received=" + pw.receivedAcks.get());
                    }
                    pw.future.completeExceptionally(
                            new ReplicationTimeoutException("ACK timeout for frameId=" + pw.frameId));
                }
            }
        }
    }
}
