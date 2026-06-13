package com.simplj.flair.cache.replication;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

final class PendingWrite {

    final long frameId;
    final int requiredAcks;
    final AtomicInteger receivedAcks = new AtomicInteger();
    final CompletableFuture<Void> future = new CompletableFuture<>();
    final long expiryMs;

    PendingWrite(long frameId, int requiredAcks, long expiryMs) {
        this.frameId = frameId;
        this.requiredAcks = requiredAcks;
        this.expiryMs = expiryMs;
    }
}
