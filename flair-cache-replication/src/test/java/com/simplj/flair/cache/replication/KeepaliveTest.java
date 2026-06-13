package com.simplj.flair.cache.replication;

import com.simplj.flair.cache.gossip.GossipNode;
import com.simplj.flair.cache.gossip.NodeInfo;
import com.simplj.flair.cache.gossip.NodeStatus;
import com.simplj.flair.cache.transport.RawFrame;
import com.simplj.flair.cache.transport.TcpServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.net.InetAddress;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for the TCP keepalive PING/PONG mechanism.
 *
 * Each test manages its own lifecycle so failures don't bleed between tests.
 */
@Timeout(15)
class KeepaliveTest {

    // ── PING is sent to connected peer ────────────────────────────────────────

    @Test
    void keepalive_sends_ping_to_connected_peer() throws Exception {
        CountDownLatch pingReceived = new CountDownLatch(1);
        List<RawFrame> captured = new CopyOnWriteArrayList<>();

        TcpServer captureServer = TcpServer.builder()
                .port(0)
                .handler((conn, frame) -> {
                    captured.add(frame);
                    if (frame.type() == FrameEncoder.TYPE_PING) pingReceived.countDown();
                })
                .build();
        captureServer.start();

        UUID nodeId = UUID.randomUUID();
        ReplicationEngine.Builder eb = ReplicationEngine.builder()
                .localNodeId(nodeId)
                .keepaliveIntervalMs(200L)
                .keepalivePongTimeoutMs(60_000L) // long — we're not testing stale here
                .blockLookup(name -> null);

        TcpServer transport = TcpServer.builder().port(0).handler(eb.frameHandler()).build();
        GossipNode gossip = GossipNode.builder()
                .nodeId(nodeId).bindAddress("127.0.0.1").bindPort(0).seedPeers(List.of())
                .build();
        ReplicationEngine engine = eb.transport(transport).cluster(gossip).build();
        transport.start();
        engine.start();

        try {
            // Simulate a peer connection to the capture server
            NodeInfo peer = new NodeInfo(UUID.randomUUID(),
                    InetAddress.getByName("127.0.0.1"), captureServer.localPort(),
                    NodeStatus.ALIVE, 0L, System.currentTimeMillis());
            engine.connectAsync(peer);

            assertTrue(pingReceived.await(5, TimeUnit.SECONDS),
                    "PING frame must arrive within keepalive interval");

            RawFrame ping = captured.stream()
                    .filter(f -> f.type() == FrameEncoder.TYPE_PING)
                    .findFirst()
                    .orElseThrow();
            UUID senderId = FrameDecoder.decodePingPong(ping.payload());
            assertNotNull(senderId);
            assertEquals(nodeId, senderId, "PING payload must carry the sender's node ID");
        } finally {
            engine.shutdown();
            transport.shutdown();
            gossip.shutdown();
            captureServer.shutdown();
        }
    }

    // ── PONG from peer keeps connection alive ─────────────────────────────────

    @Test
    void peer_that_responds_with_pong_is_not_disconnected() throws Exception {
        // A server that echoes every PING with a PONG.
        CountDownLatch pingReceived = new CountDownLatch(1);

        UUID remoteId = UUID.randomUUID();
        TcpServer echoServer = TcpServer.builder()
                .port(0)
                .handler((conn, frame) -> {
                    if (frame.type() == FrameEncoder.TYPE_PING) {
                        conn.send(FrameEncoder.encodePong(remoteId));
                        pingReceived.countDown();
                    }
                })
                .build();
        echoServer.start();

        UUID nodeId = UUID.randomUUID();
        long keepaliveMs = 150L;
        long pongTimeoutMs = 300L;

        ReplicationEngine.Builder eb = ReplicationEngine.builder()
                .localNodeId(nodeId)
                .keepaliveIntervalMs(keepaliveMs)
                .keepalivePongTimeoutMs(pongTimeoutMs)
                .blockLookup(name -> null);

        TcpServer transport = TcpServer.builder().port(0).handler(eb.frameHandler()).build();
        GossipNode gossip = GossipNode.builder()
                .nodeId(nodeId).bindAddress("127.0.0.1").bindPort(0).seedPeers(List.of())
                .build();
        ReplicationEngine engine = eb.transport(transport).cluster(gossip).build();
        transport.start();
        engine.start();

        try {
            NodeInfo peer = new NodeInfo(remoteId,
                    InetAddress.getByName("127.0.0.1"), echoServer.localPort(),
                    NodeStatus.ALIVE, 0L, System.currentTimeMillis());
            engine.connectAsync(peer);

            // Wait for at least one PING to be sent and PONG to come back
            assertTrue(pingReceived.await(5, TimeUnit.SECONDS), "PING must reach the echo server");

            // Wait for 3 keepalive cycles to pass
            Thread.sleep(3 * keepaliveMs + 200);

            // The responding peer must still be considered alive
            assertEquals(1, engine.peerRegistry().aliveCount(),
                    "Peer that replies with PONG must remain connected");
        } finally {
            engine.shutdown();
            transport.shutdown();
            gossip.shutdown();
            echoServer.shutdown();
        }
    }

    // ── Silent peer is disconnected after pong timeout ────────────────────────

    @Test
    void stale_peer_is_disconnected_after_pong_timeout() throws Exception {
        CountDownLatch pingReceived = new CountDownLatch(1);

        TcpServer silentServer = TcpServer.builder()
                .port(0)
                .handler((conn, frame) -> {
                    // Receive PINGs but never respond with PONGs
                    if (frame.type() == FrameEncoder.TYPE_PING) pingReceived.countDown();
                })
                .build();
        silentServer.start();

        UUID nodeId = UUID.randomUUID();
        long keepaliveMs = 150L;
        long pongTimeoutMs = 300L; // 2 keepalive cycles before disconnect

        ReplicationEngine.Builder eb = ReplicationEngine.builder()
                .localNodeId(nodeId)
                .keepaliveIntervalMs(keepaliveMs)
                .keepalivePongTimeoutMs(pongTimeoutMs)
                .blockLookup(name -> null);

        TcpServer transport = TcpServer.builder().port(0).handler(eb.frameHandler()).build();
        GossipNode gossip = GossipNode.builder()
                .nodeId(nodeId).bindAddress("127.0.0.1").bindPort(0).seedPeers(List.of())
                .build();
        ReplicationEngine engine = eb.transport(transport).cluster(gossip).build();
        transport.start();
        engine.start();

        try {
            UUID peerId = UUID.randomUUID();
            NodeInfo peer = new NodeInfo(peerId,
                    InetAddress.getByName("127.0.0.1"), silentServer.localPort(),
                    NodeStatus.ALIVE, 0L, System.currentTimeMillis());
            engine.connectAsync(peer);

            // Wait for the first PING to confirm the connection is up
            assertTrue(pingReceived.await(5, TimeUnit.SECONDS), "PING must reach the silent server");

            // Wait long enough for pong timeout to trigger: seed + pongTimeout + 2 keepalive cycles
            Thread.sleep(pongTimeoutMs + 2 * keepaliveMs + 300);

            assertEquals(0, engine.peerRegistry().aliveCount(),
                    "Stale connection must be closed after pong timeout with no reply");
        } finally {
            engine.shutdown();
            transport.shutdown();
            gossip.shutdown();
            silentServer.shutdown();
        }
    }
}
