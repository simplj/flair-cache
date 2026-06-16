package com.simplj.flair.cache.replication;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

final class PendingWrite {

    final long frameId;
    final int requiredAcks;
    final AtomicInteger receivedAcks = new AtomicInteger();
    final CompletableFuture<Void> future = new CompletableFuture<>();
    final long expiryMs;

    /**
     * Number of peers this write was broadcast to and expects ACKs from — i.e. the alive-peer
     * count at enqueue time. Fixed for the life of the write.
     */
    final int expectedPeers;

    /**
     * Peers declared DEAD while this write was in flight. Used by {@link AckTracker#onPeerDead}
     * to re-evaluate the write the moment cluster membership shrinks instead of blocking for the
     * full ACK timeout:
     * <ul>
     *   <li>If {@code receivedAcks + deadPeers >= requiredAcks}, the surviving cluster has shrunk
     *       enough that the remaining ACKs already satisfy quorum — complete successfully.</li>
     *   <li>If {@code expectedPeers - deadPeers < requiredAcks}, no combination of the still-alive
     *       peers can ever reach the required ACK count — fail immediately.</li>
     * </ul>
     */
    final AtomicInteger deadPeers = new AtomicInteger();

    PendingWrite(long frameId, int requiredAcks, long expiryMs) {
        this(frameId, requiredAcks, requiredAcks, expiryMs);
    }

    PendingWrite(long frameId, int requiredAcks, int expectedPeers, long expiryMs) {
        this.frameId = frameId;
        this.requiredAcks = requiredAcks;
        this.expectedPeers = expectedPeers;
        this.expiryMs = expiryMs;
    }
}
