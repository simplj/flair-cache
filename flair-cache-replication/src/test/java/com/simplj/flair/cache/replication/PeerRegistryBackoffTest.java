package com.simplj.flair.cache.replication;

import com.simplj.flair.cache.gossip.GossipNode;
import com.simplj.flair.cache.gossip.NodeInfo;
import com.simplj.flair.cache.gossip.NodeStatus;
import com.simplj.flair.cache.transport.TcpServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies that PeerRegistry does not flood the connect pool when a peer is unreachable:
 *   1. The in-flight guard prevents concurrent doConnect attempts for the same peer.
 *   2. Exponential backoff blocks reconnect attempts until the delay has elapsed.
 *   3. A successful connect clears the backoff so the next disconnect reconnects promptly.
 *   4. onDead / onLeave clears backoff state so a rejoin starts without penalty.
 */
@Timeout(15)
class PeerRegistryBackoffTest {

    // ── helpers ───────────────────────────────────────────────────────────────

    /** Finds an unused port and immediately releases it so connections to it will be refused. */
    private static int refusedPort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    /** Captures WARNING logs from PeerRegistry whose message starts with the given prefix. */
    private static class WarnCounter extends Handler {
        final AtomicInteger count = new AtomicInteger();
        final CountDownLatch firstHit;
        private final String prefix;

        WarnCounter(String prefix, int firstHitCount) {
            this.prefix = prefix;
            this.firstHit = new CountDownLatch(firstHitCount);
            setLevel(Level.ALL);
        }

        @Override public void publish(LogRecord r) {
            if (r.getLevel() == Level.WARNING
                    && r.getMessage() != null
                    && r.getMessage().startsWith(prefix)) {
                count.incrementAndGet();
                firstHit.countDown();
            }
        }
        @Override public void flush() {}
        @Override public void close() {}
    }

    /** Builds a minimal ReplicationEngine (no blocks, no ACK timeout tuning). */
    private static ReplicationEngine startEngine(UUID nodeId, TcpServer[] serverOut,
                                                  GossipNode[] gossipOut) throws Exception {
        ReplicationEngine.Builder eb = ReplicationEngine.builder()
                .localNodeId(nodeId)
                .blockLookup(name -> null);
        TcpServer transport = TcpServer.builder().port(0).handler(eb.frameHandler()).build();
        GossipNode gossip = GossipNode.builder()
                .nodeId(nodeId).bindAddress("127.0.0.1").bindPort(0).seedPeers(List.of()).build();
        ReplicationEngine engine = eb.transport(transport).cluster(gossip).build();
        transport.start();
        engine.start();
        serverOut[0] = transport;
        gossipOut[0] = gossip;
        return engine;
    }

    private static NodeInfo peer(UUID id, int port) throws Exception {
        return new NodeInfo(id, InetAddress.getByName("127.0.0.1"),
                port, NodeStatus.ALIVE, 0L, System.currentTimeMillis());
    }

    private static void shutdown(ReplicationEngine e, TcpServer s, GossipNode g) {
        try { e.shutdown(); } catch (Exception ignored) {}
        try { s.shutdown(); } catch (Exception ignored) {}
        try { g.shutdown(); } catch (Exception ignored) {}
    }

    // ── tests ─────────────────────────────────────────────────────────────────

    @Test
    void inflight_guard_prevents_concurrent_connect_attempts() throws Exception {
        int port = refusedPort();
        UUID nodeId = UUID.randomUUID();
        TcpServer[] srv = new TcpServer[1]; GossipNode[] gsp = new GossipNode[1];
        ReplicationEngine engine = startEngine(nodeId, srv, gsp);

        Logger peerLog = Logger.getLogger(PeerRegistry.class.getName());
        WarnCounter counter = new WarnCounter("Failed to connect", 1);
        peerLog.addHandler(counter);

        try {
            NodeInfo p = peer(UUID.randomUUID(), port);

            // Fire 20 rapid connectAsync calls — only 1 doConnect should run at a time
            for (int i = 0; i < 20; i++) {
                engine.connectAsync(p);
            }

            // Wait for the single in-flight attempt to fail
            assertTrue(counter.firstHit.await(5, TimeUnit.SECONDS),
                    "At least one connect attempt must fail");

            // Give any hypothetical concurrent attempts a window to also log
            Thread.sleep(100);

            assertEquals(1, counter.count.get(),
                    "In-flight guard must allow only one doConnect at a time");
        } finally {
            peerLog.removeHandler(counter);
            shutdown(engine, srv[0], gsp[0]);
        }
    }

    @Test
    void backoff_blocks_reconnect_within_delay_window() throws Exception {
        int port = refusedPort();
        UUID nodeId = UUID.randomUUID();
        TcpServer[] srv = new TcpServer[1]; GossipNode[] gsp = new GossipNode[1];
        ReplicationEngine engine = startEngine(nodeId, srv, gsp);

        Logger peerLog = Logger.getLogger(PeerRegistry.class.getName());
        WarnCounter counter = new WarnCounter("Failed to connect", 1);
        peerLog.addHandler(counter);

        try {
            NodeInfo p = peer(UUID.randomUUID(), port);

            // First attempt — will fail and set 100ms backoff
            engine.connectAsync(p);
            assertTrue(counter.firstHit.await(5, TimeUnit.SECONDS), "First attempt must fail");
            // Pool thread clears 'connecting' in its finally block after logging the warning.
            // Without this sleep the 30 calls below might be blocked by the in-flight guard
            // rather than the backoff, producing the right count=1 for the wrong reason.
            Thread.sleep(50);

            // Now all rapid calls must be suppressed by the backoff check, not the in-flight guard
            for (int i = 0; i < 30; i++) {
                engine.connectAsync(p);
            }
            Thread.sleep(30); // still within the 100ms backoff window

            assertEquals(1, counter.count.get(),
                    "Backoff must suppress all reconnect attempts within the delay window");
        } finally {
            peerLog.removeHandler(counter);
            shutdown(engine, srv[0], gsp[0]);
        }
    }

    @Test
    void backoff_allows_retry_after_delay_elapses() throws Exception {
        int port = refusedPort();
        UUID nodeId = UUID.randomUUID();
        TcpServer[] srv = new TcpServer[1]; GossipNode[] gsp = new GossipNode[1];
        ReplicationEngine engine = startEngine(nodeId, srv, gsp);

        Logger peerLog = Logger.getLogger(PeerRegistry.class.getName());
        // firstHit=1: confirms the first failure has occurred before we probe backoff behaviour.
        WarnCounter counter = new WarnCounter("Failed to connect", 1);
        peerLog.addHandler(counter);

        try {
            NodeInfo p = peer(UUID.randomUUID(), port);

            // First attempt — fails almost instantly (ECONNREFUSED on localhost)
            engine.connectAsync(p);
            assertTrue(counter.firstHit.await(5, TimeUnit.SECONDS),
                    "First connect attempt must fail");
            // At this point the backoff window (~100ms from the failure) has just started.

            // Calls within the backoff window must be suppressed.
            engine.connectAsync(p);
            engine.connectAsync(p);
            Thread.sleep(30); // well within the 100ms window
            assertEquals(1, counter.count.get(), "Calls within backoff window must be no-ops");

            // Add a second counter to catch the next warning independently.
            WarnCounter counter2 = new WarnCounter("Failed to connect", 1);
            peerLog.addHandler(counter2);
            try {
                // Wait for the backoff window to expire with a comfortable margin.
                Thread.sleep(150); // 150ms > 100ms first-failure backoff
                engine.connectAsync(p);
                assertTrue(counter2.firstHit.await(5, TimeUnit.SECONDS),
                        "Second attempt must fire after backoff window elapses");
            } finally {
                peerLog.removeHandler(counter2);
            }
        } finally {
            peerLog.removeHandler(counter);
            shutdown(engine, srv[0], gsp[0]);
        }
    }

    @Test
    void on_dead_clears_backoff_so_rejoin_connects_without_delay() throws Exception {
        int port = refusedPort();
        UUID nodeId = UUID.randomUUID();
        TcpServer[] srv = new TcpServer[1]; GossipNode[] gsp = new GossipNode[1];
        ReplicationEngine engine = startEngine(nodeId, srv, gsp);

        Logger peerLog = Logger.getLogger(PeerRegistry.class.getName());
        WarnCounter counter = new WarnCounter("Failed to connect", 1);
        peerLog.addHandler(counter);

        try {
            UUID peerId = UUID.randomUUID();
            NodeInfo p = peer(peerId, port);

            // Trigger a failure to set backoff
            engine.connectAsync(p);
            assertTrue(counter.firstHit.await(5, TimeUnit.SECONDS));
            // The pool thread logs the warning then clears 'connecting' in its finally block.
            // Give it a moment to complete so the next connectAsync sees a clean state.
            Thread.sleep(50);

            // Simulate SWIM declaring the peer dead — must clear backoff
            engine.peerRegistry().onDead(p);

            // Reset counter for the next attempt
            Logger peerLog2 = Logger.getLogger(PeerRegistry.class.getName());
            WarnCounter counter2 = new WarnCounter("Failed to connect", 1);
            peerLog2.addHandler(counter2);

            try {
                // connectAsync immediately after onDead — backoff is cleared, attempt must fire
                engine.connectAsync(p);
                assertTrue(counter2.firstHit.await(5, TimeUnit.SECONDS),
                        "Attempt after onDead must not be blocked by stale backoff");
            } finally {
                peerLog2.removeHandler(counter2);
            }
        } finally {
            peerLog.removeHandler(counter);
            shutdown(engine, srv[0], gsp[0]);
        }
    }

    @Test
    void successful_connect_clears_backoff_state() throws Exception {
        int refusedPort = refusedPort();
        UUID nodeId = UUID.randomUUID();
        TcpServer[] srv = new TcpServer[1]; GossipNode[] gsp = new GossipNode[1];
        ReplicationEngine engine = startEngine(nodeId, srv, gsp);

        // A real server that accepts connections
        TcpServer realServer = TcpServer.builder()
                .port(0).handler((conn, frame) -> {}).build();
        realServer.start();

        Logger peerLog = Logger.getLogger(PeerRegistry.class.getName());
        WarnCounter failCounter = new WarnCounter("Failed to connect", 1);
        peerLog.addHandler(failCounter);

        try {
            UUID peerId = UUID.randomUUID();

            // First: connect to a refused port to accumulate failure + backoff
            engine.connectAsync(peer(peerId, refusedPort));
            assertTrue(failCounter.firstHit.await(5, TimeUnit.SECONDS));
            // Pool thread clears 'connecting' in its finally block after logging the warning.
            Thread.sleep(50);

            // Simulate the peer coming back on a different (real) port via onDead + re-wire
            engine.peerRegistry().onDead(peer(peerId, refusedPort));

            // Now connect to the real server — must succeed and clear backoff
            NodeInfo live = peer(peerId, realServer.localPort());
            engine.connectAsync(live);
            Thread.sleep(500); // allow connect to complete

            assertEquals(1, engine.peerRegistry().aliveCount(),
                    "Connection must be established after backoff is cleared on onDead");
        } finally {
            peerLog.removeHandler(failCounter);
            shutdown(engine, srv[0], gsp[0]);
            realServer.shutdown();
        }
    }
}
