package com.simplj.flair.cache.transport;

import java.net.InetAddress;
import java.util.UUID;

public interface Connection {
    UUID id();
    InetAddress remoteAddress();
    void send(RawFrame frame);
    void close();
    boolean isAlive();

    /** Number of frames enqueued for this peer but not yet written to the socket. */
    default int pendingWrites() {
        return 0;
    }

    /**
     * Attempts a bounded flush of any frames still queued for this peer, then closes the
     * connection. The flush blocks at most {@code flushTimeoutMs}; if it elapses, the
     * connection is closed regardless of remaining frames. Intended for graceful peer leave.
     */
    default void closeGraceful(long flushTimeoutMs) {
        close();
    }

    /**
     * Discards any frames still queued for this peer without attempting to send them, then
     * closes the connection. Returns the number of frames discarded. Intended for an
     * unreachable (dead) peer where flushing would only block.
     */
    default int closeAndDiscard() {
        close();
        return 0;
    }
}
