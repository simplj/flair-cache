package com.simplj.flair.cache.replication;

import com.simplj.flair.cache.commons.FlairCacheThreadFactory;
import java.util.Map;
import java.util.UUID;
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

    /**
     * Re-evaluates every in-flight write when a peer is declared DEAD. Without this, a QUORUM or
     * STRONG write blocks for the full ACK timeout even when the surviving peers already constitute
     * (or can never constitute) quorum.
     *
     * <p>The replication model broadcasts each frame to all alive peers and counts ACKs rather than
     * tracking which specific peer ACKed, so a peer death is applied to every pending write: each
     * loses one of the peers it was waiting on. For each write we then either:</p>
     * <ul>
     *   <li>complete it successfully if the cluster has shrunk enough that the ACKs already received
     *       satisfy the required count ({@code received + dead >= required}); or</li>
     *   <li>fail it immediately with {@link ReplicationTimeoutException} if the still-alive peers can
     *       no longer reach the required count ({@code expected - dead < required}).</li>
     * </ul>
     */
    void onPeerDead(UUID peerId) {
        for (PendingWrite pw : pending.values()) {
            int dead = pw.deadPeers.incrementAndGet();
            int received = pw.receivedAcks.get();

            if (received + dead >= pw.requiredAcks) {
                // Surviving cluster shrank enough — the ACKs in hand now satisfy quorum.
                if (pending.remove(pw.frameId, pw)) {
                    pw.future.complete(null);
                    if (log.isLoggable(Level.FINE)) {
                        log.fine("Peer " + peerId + " dead — completed frameId=" + pw.frameId
                                + " (received=" + received + " dead=" + dead
                                + " required=" + pw.requiredAcks + ")");
                    }
                }
            } else if (pw.expectedPeers - dead < pw.requiredAcks) {
                // Not enough live peers remain to ever reach the required ACK count.
                if (pending.remove(pw.frameId, pw)) {
                    pw.future.completeExceptionally(new ReplicationTimeoutException(
                            "Quorum unreachable for frameId=" + pw.frameId + " — peer " + peerId
                            + " dead (received=" + received + " required=" + pw.requiredAcks
                            + " expectedPeers=" + pw.expectedPeers + " dead=" + dead + ")"));
                    if (log.isLoggable(Level.FINE)) {
                        log.fine("Peer " + peerId + " dead — failed frameId=" + pw.frameId
                                + " (quorum unreachable)");
                    }
                }
            }
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
