package com.simplj.flair.cache.watch;

import java.util.function.Consumer;

/**
 * Checks replication lag against a threshold and invokes a callback when exceeded.
 * A threshold of zero means the callback is always invoked for every replicated event.
 */
final class ReplicationLagMonitor {

    private final long             thresholdMs;
    private final Consumer<Long>   listener;

    ReplicationLagMonitor(long thresholdMs, Consumer<Long> listener) {
        this.thresholdMs = thresholdMs;
        this.listener    = listener;
    }

    void check(long lagMs) {
        if (lagMs >= thresholdMs) {
            listener.accept(lagMs);
        }
    }
}
