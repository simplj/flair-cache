package com.simplj.flair.cache.replication;

import com.simplj.flair.cache.transport.Connection;
import com.simplj.flair.cache.transport.RawFrame;
import com.simplj.flair.cache.transport.TcpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.net.InetAddress;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Section 4: graceful leave performs a bounded flush of a peer's pending frames; a dead peer's
 * queue is drained-and-discarded with each discarded frame counted via the dropped-frame sink.
 */
@Timeout(15)
class PeerRegistryDisconnectTest {

    private TcpServer server;
    private PeerRegistry registry;

    @AfterEach
    void tearDown() {
        if (registry != null) try { registry.shutdown(); } catch (Exception ignored) {}
        if (server != null)   try { server.shutdown();   } catch (Exception ignored) {}
    }

    private PeerRegistry newRegistry() throws IOException {
        server = TcpServer.builder().port(0).handler((c, f) -> {}).build();
        return new PeerRegistry(UUID.randomUUID(), server.eventLoop(), null, 50L);
    }

    @Test
    void leave_attempts_bounded_flush_then_closes() throws IOException {
        registry = newRegistry();
        UUID peer = UUID.randomUUID();
        FakeConnection conn = new FakeConnection(3); // 3 frames pending
        registry.installConnectionForTest(peer, conn);

        registry.disconnectPeerGraceful(peer);

        assertTrue(conn.gracefulClosed.get(), "leave must flush-and-close (closeGraceful)");
        assertFalse(conn.discarded.get(), "leave must NOT discard frames");
    }

    @Test
    void dead_discards_queue_and_increments_dropped_counter() throws IOException {
        registry = newRegistry();
        AtomicLong dropped = new AtomicLong();
        registry.onFramesDropped(dropped::addAndGet);

        UUID peer = UUID.randomUUID();
        FakeConnection conn = new FakeConnection(4); // 4 frames pending, never sent
        registry.installConnectionForTest(peer, conn);

        registry.disconnectPeerDead(peer);

        assertTrue(conn.discarded.get(), "dead peer queue must be drained-and-discarded");
        assertFalse(conn.gracefulClosed.get(), "dead peer must NOT attempt a graceful flush");
        assertEquals(4L, dropped.get(), "every discarded frame must be counted as dropped");
    }

    @Test
    void dead_with_empty_queue_does_not_increment_counter() throws IOException {
        registry = newRegistry();
        AtomicLong dropped = new AtomicLong();
        registry.onFramesDropped(dropped::addAndGet);

        UUID peer = UUID.randomUUID();
        FakeConnection conn = new FakeConnection(0); // nothing queued
        registry.installConnectionForTest(peer, conn);

        registry.disconnectPeerDead(peer);

        assertTrue(conn.discarded.get());
        assertEquals(0L, dropped.get(), "no frames queued → no dropped-frame increments");
    }

    /** A Connection stub that records which teardown path was taken and reports a pending count. */
    private static final class FakeConnection implements Connection {
        final AtomicBoolean gracefulClosed = new AtomicBoolean(false);
        final AtomicBoolean discarded      = new AtomicBoolean(false);
        private final int pending;

        FakeConnection(int pending) { this.pending = pending; }

        @Override public UUID id()                   { return UUID.randomUUID(); }
        @Override public InetAddress remoteAddress() { return null; }
        @Override public void send(RawFrame frame)   {}
        @Override public void close()                {}
        @Override public boolean isAlive()           { return true; }
        @Override public int pendingWrites()         { return pending; }

        @Override public void closeGraceful(long flushTimeoutMs) {
            gracefulClosed.set(true);
        }

        @Override public int closeAndDiscard() {
            discarded.set(true);
            return pending; // simulate discarding the queued frames
        }
    }
}
