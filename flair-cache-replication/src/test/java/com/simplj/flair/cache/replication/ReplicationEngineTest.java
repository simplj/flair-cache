package com.simplj.flair.cache.replication;

import com.simplj.flair.cache.gossip.GossipNode;
import com.simplj.flair.cache.gossip.NodeInfo;
import com.simplj.flair.cache.gossip.NodeStatus;
import com.simplj.flair.cache.hlc.HLCTimestamp;
import com.simplj.flair.cache.serial.codecs.ByteArrayCodec;
import com.simplj.flair.cache.serial.codecs.StringCodec;
import com.simplj.flair.cache.store.CacheBlock;
import com.simplj.flair.cache.store.CacheEntry;
import com.simplj.flair.cache.transport.Connection;
import com.simplj.flair.cache.transport.FrameHandler;
import com.simplj.flair.cache.transport.RawFrame;
import com.simplj.flair.cache.transport.TcpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.net.InetAddress;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests ReplicationEngine lifecycle, all three consistency modes (EVENTUAL, QUORUM, STRONG),
 * the QUORUM vs STRONG behavioural distinction with multiple peers, and ACK timeout.
 *
 * Multi-node tests that need more than two engines use the self-contained {@link Node} helper
 * and manage their own cleanup via try-finally rather than @AfterEach fields, keeping
 * each test independent and unambiguous about which resources it owns.
 */
@Timeout(30)
class ReplicationEngineTest {

    // Fields used by single-engine tests and the existing 2-node QUORUM test
    private TcpServer serverA, serverB;
    private GossipNode gossipA, gossipB;
    private ReplicationEngine engineA, engineB;
    private CacheBlock<String, byte[]> blockA, blockB;

    @AfterEach
    void tearDown() {
        shutdown(engineA); shutdown(engineB);
        shutdown(serverA); shutdown(serverB);
        shutdown(gossipA); shutdown(gossipB);
        close(blockA);     close(blockB);
    }

    // ── Lifecycle guards ──────────────────────────────────────────────────────

    @Test
    void start_twice_throws_illegal_state() throws IOException {
        engineA = buildEngine(null);
        engineA.start();
        assertThrows(IllegalStateException.class, engineA::start);
    }

    @Test
    void replicate_before_start_throws_illegal_state() throws IOException {
        engineA = buildEngine(null);
        assertThrows(IllegalStateException.class,
                () -> engineA.replicate(putEvent("b", new byte[]{1}, ConsistencyMode.EVENTUAL)));
    }

    // ── Consistency modes — no alive peers (degenerate / fire-and-forget) ─────

    @Test
    void eventual_with_no_peers_returns_immediately() throws Exception {
        engineA = buildEngine(null);
        engineA.start();
        assertDoesNotThrow(() -> engineA.replicate(putEvent("b", new byte[]{1}, ConsistencyMode.EVENTUAL)));
    }

    @Test
    void quorum_with_no_peers_completes_as_fire_and_forget() throws Exception {
        // 0 alive peers → required = (0+1)/2 = 0 → treated as fire-and-forget
        engineA = buildEngine(null);
        engineA.start();
        assertDoesNotThrow(() -> engineA.replicate(putEvent("b", new byte[]{1}, ConsistencyMode.QUORUM)));
    }

    @Test
    void strong_with_no_peers_completes_as_fire_and_forget() throws Exception {
        // 0 alive peers → required = 0 → treated as fire-and-forget
        engineA = buildEngine(null);
        engineA.start();
        assertDoesNotThrow(() -> engineA.replicate(putEvent("b", new byte[]{1}, ConsistencyMode.STRONG)));
    }

    // ── QUORUM round-trip: 2 nodes ────────────────────────────────────────────

    @Test
    void quorum_completes_when_single_peer_acks() throws Exception {
        blockA = block("data");
        blockB = block("data");

        ReplicationEngine.Builder ebA = engineBuilder(UUID.randomUUID(), "data", blockA, 5_000);
        FrameHandler fhA = ebA.frameHandler();
        serverA = TcpServer.builder().port(0).handler(fhA).build();
        gossipA = gossip(UUID.randomUUID());
        engineA = ebA.transport(serverA).cluster(gossipA).build();
        serverA.start();
        engineA.start();

        UUID idB = UUID.randomUUID();
        ReplicationEngine.Builder ebB = engineBuilder(idB, "data", blockB, 5_000);
        serverB = TcpServer.builder().port(0).handler(ebB.frameHandler()).build();
        gossipB = gossip(idB);
        engineB = ebB.transport(serverB).cluster(gossipB).build();
        serverB.start();
        engineB.start();

        wire(engineA, idB, serverB.localPort());
        Thread.sleep(600);

        assertQuorumCompletes(engineA, "data", "hello".getBytes());
        Thread.sleep(100);
        assertNotNull(blockB.getRaw("hello".getBytes()), "peer must have the replicated entry");
    }

    // ── STRONG round-trip: 2 nodes ────────────────────────────────────────────

    @Test
    void strong_completes_when_single_peer_acks() throws Exception {
        // 2 nodes → aliveCount=1 → STRONG requires 1 ACK (same as QUORUM for 2 nodes).
        // Explicitly exercises the STRONG code path, not the fire-and-forget shortcut.
        blockA = block("data");
        blockB = block("data");

        UUID idA = UUID.randomUUID();
        UUID idB = UUID.randomUUID();

        ReplicationEngine.Builder ebA = engineBuilder(idA, "data", blockA, 5_000);
        serverA = TcpServer.builder().port(0).handler(ebA.frameHandler()).build();
        gossipA = gossip(idA);
        engineA = ebA.transport(serverA).cluster(gossipA).build();
        serverA.start();
        engineA.start();

        ReplicationEngine.Builder ebB = engineBuilder(idB, "data", blockB, 5_000);
        serverB = TcpServer.builder().port(0).handler(ebB.frameHandler()).build();
        gossipB = gossip(idB);
        engineB = ebB.transport(serverB).cluster(gossipB).build();
        serverB.start();
        engineB.start();

        wire(engineA, idB, serverB.localPort());
        Thread.sleep(600);

        assertStrongCompletes(engineA, "data", "world".getBytes());
        Thread.sleep(100);
        assertNotNull(blockB.getRaw("world".getBytes()));
    }

    // ── QUORUM vs STRONG distinction: 3 nodes ────────────────────────────────
    //
    // With 3 nodes (2 alive peers):
    //   QUORUM: required = (2+1)/2 = 1 peer ACK  → completes even if one peer is silent
    //   STRONG: required = 2 peer ACKs            → times out if one peer is silent

    @Test
    void quorum_completes_with_two_peers_even_when_one_does_not_ack() throws Exception {
        // A connects to B (real engine — will ACK) and to a fake server (will not ACK).
        // aliveCount=2, required=(2+1)/2=1. B's single ACK is enough.
        Node nodeA = Node.build("items", 5_000);
        Node nodeB = Node.build("items", 5_000);
        TcpServer fakeServer = TcpServer.builder().port(0)
                .handler((conn, frame) -> {}) // never ACKs
                .build();
        fakeServer.start();

        try {
            nodeA.start();
            nodeB.start();

            // Wire A → B (real ACK) and A → fake (silent)
            wire(nodeA.engine, nodeB.id, nodeB.server.localPort());
            wire(nodeA.engine, UUID.randomUUID(), fakeServer.localPort());
            Thread.sleep(700); // let both connections establish

            // QUORUM: 1 ACK needed out of 2 peers → completes on B's ACK alone
            assertQuorumCompletes(nodeA.engine, "items", "q-key".getBytes());
        } finally {
            nodeA.shutdown(); nodeB.shutdown();
            fakeServer.shutdown();
        }
    }

    @Test
    void strong_times_out_when_one_of_two_peers_does_not_ack() throws Exception {
        // Same topology as above but uses STRONG (requires ALL peers to ACK).
        // A connects to B (ACKs) and fake server (silent).
        // aliveCount=2, STRONG required=2. Only 1 ACK arrives → timeout.
        Node nodeA = Node.build("items", 800); // short timeout so test runs fast
        Node nodeB = Node.build("items", 5_000);
        TcpServer fakeServer = TcpServer.builder().port(0)
                .handler((conn, frame) -> {})
                .build();
        fakeServer.start();

        try {
            nodeA.start();
            nodeB.start();

            wire(nodeA.engine, nodeB.id, nodeB.server.localPort());
            wire(nodeA.engine, UUID.randomUUID(), fakeServer.localPort());
            Thread.sleep(700);

            // STRONG: 2 ACKs needed, only 1 arrives → ReplicationTimeoutException
            assertThrows(ReplicationTimeoutException.class,
                    () -> nodeA.engine.replicate(putEvent("items", "s-key".getBytes(), ConsistencyMode.STRONG)));
        } finally {
            nodeA.shutdown(); nodeB.shutdown();
            fakeServer.shutdown();
        }
    }

    @Test
    void strong_completes_when_all_peers_ack() throws Exception {
        // 3 real engines: A sends STRONG to B and C; both ACK → completes.
        Node nodeA = Node.build("items", 8_000);
        Node nodeB = Node.build("items", 5_000);
        Node nodeC = Node.build("items", 5_000);

        try {
            nodeA.start();
            nodeB.start();
            nodeC.start();

            wire(nodeA.engine, nodeB.id, nodeB.server.localPort());
            wire(nodeA.engine, nodeC.id, nodeC.server.localPort());
            Thread.sleep(700);

            // STRONG: aliveCount=2, required=2. Both B and C ACK → must complete.
            assertStrongCompletes(nodeA.engine, "items", "strong-key".getBytes());
        } finally {
            nodeA.shutdown(); nodeB.shutdown(); nodeC.shutdown();
        }
    }

    // ── DELETE end-to-end ─────────────────────────────────────────────────────

    @Test
    void delete_replicated_and_applied_on_peer() throws Exception {
        blockA = block("data");
        blockB = block("data");

        UUID idA = UUID.randomUUID();
        UUID idB = UUID.randomUUID();

        ReplicationEngine.Builder ebA = engineBuilder(idA, "data", blockA, 5_000);
        serverA = TcpServer.builder().port(0).handler(ebA.frameHandler()).build();
        gossipA = gossip(idA);
        engineA = ebA.transport(serverA).cluster(gossipA).build();
        serverA.start();
        engineA.start();

        ReplicationEngine.Builder ebB = engineBuilder(idB, "data", blockB, 5_000);
        serverB = TcpServer.builder().port(0).handler(ebB.frameHandler()).build();
        gossipB = gossip(idB);
        engineB = ebB.transport(serverB).cluster(gossipB).build();
        serverB.start();
        engineB.start();

        // Seed the key on B's block directly so there is something to delete
        byte[] key = "gone".getBytes();
        blockB.putRaw(key, new CacheEntry("present".getBytes(),
                new HLCTimestamp(1L, 0L), 0L, 0L, 0L, idB));
        assertNotNull(blockB.getRaw(key), "pre-condition: key must exist on B before DELETE");

        wire(engineA, idB, serverB.localPort());
        Thread.sleep(600);

        // aliveCount=1 on A → QUORUM required=1; B receives, deletes, and ACKs
        engineA.replicate(ReplicationEvent.delete("data", key,
                new HLCTimestamp(System.currentTimeMillis(), 0L), idA, ConsistencyMode.QUORUM));

        Thread.sleep(100);
        assertNull(blockB.getRaw(key), "key must be absent on peer after DELETE replication");
    }

    // ── QUORUM timeout ────────────────────────────────────────────────────────

    @Test
    void quorum_throws_timeout_exception_when_peer_does_not_ack() throws Exception {
        // A is connected to a fake server that accepts frames but never ACKs.
        // aliveCount=1, QUORUM required=1. The ACK never arrives → timeout.
        Node nodeA = Node.build("items", 600); // short timeout
        TcpServer fakeServer = TcpServer.builder().port(0)
                .handler((conn, frame) -> {})
                .build();
        fakeServer.start();

        try {
            nodeA.start();
            wire(nodeA.engine, UUID.randomUUID(), fakeServer.localPort());
            Thread.sleep(500);

            assertThrows(ReplicationTimeoutException.class,
                    () -> nodeA.engine.replicate(putEvent("items", "key".getBytes(), ConsistencyMode.QUORUM)));
        } finally {
            nodeA.shutdown();
            fakeServer.shutdown();
        }
    }

    // ── Incoming callback ─────────────────────────────────────────────────────

    @Test
    void on_incoming_callback_fires_when_registered() throws IOException {
        blockA = block("items");
        AtomicReference<ReplicationEvent> received = new AtomicReference<>();

        ReplicationEngine.Builder eb = ReplicationEngine.builder()
                .localNodeId(UUID.randomUUID())
                .blockLookup(name -> "items".equals(name) ? blockA : null);
        FrameHandler fh = eb.frameHandler();
        serverA = TcpServer.builder().port(0).handler(fh).build();
        gossipA = gossip(UUID.randomUUID());
        engineA = eb.transport(serverA).cluster(gossipA).build();
        engineA.onIncoming(received::set);
        engineA.start();

        IncomingHandler handler = (IncomingHandler) fh;
        CacheEntry entry = new CacheEntry("v".getBytes(), new HLCTimestamp(1L, 0L), 0, 0, 0, UUID.randomUUID());
        handler.onFrame(noOpConn(), FrameEncoder.encodePut(1L, false,
                new ReplicationEvent.PutEvent("items", "k".getBytes(), entry, ConsistencyMode.EVENTUAL)));

        assertNotNull(received.get());
        assertEquals("items", received.get().blockName());
        assertEquals(ConsistencyMode.EVENTUAL, received.get().mode());
    }

    // ── AckTracker sweep ──────────────────────────────────────────────────────

    @Test
    void ack_sweep_expires_overdue_pending_write() throws Exception {
        engineA = buildEngine(null, 50L);
        engineA.start();
        PendingWrite pw = new PendingWrite(42L, 1, System.currentTimeMillis() - 1); // already expired
        engineA.ackTracker().track(pw);
        Thread.sleep(300);
        assertTrue(pw.future.isCompletedExceptionally());
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private ReplicationEngine buildEngine(
            java.util.function.Function<String, CacheBlock<?, ?>> lookup) throws IOException {
        return buildEngine(lookup, 500L);
    }

    private ReplicationEngine buildEngine(
            java.util.function.Function<String, CacheBlock<?, ?>> lookup,
            long ackTimeoutMs) throws IOException {
        UUID id = UUID.randomUUID();
        ReplicationEngine.Builder eb = ReplicationEngine.builder()
                .localNodeId(id).ackTimeoutMs(ackTimeoutMs);
        if (lookup != null) eb.blockLookup(lookup);
        serverA = TcpServer.builder().port(0).handler(eb.frameHandler()).build();
        gossipA = gossip(id);
        return eb.transport(serverA).cluster(gossipA).build();
    }

    private static ReplicationEngine.Builder engineBuilder(UUID id, String blockName,
            CacheBlock<?, ?> block, long ackTimeoutMs) {
        return ReplicationEngine.builder()
                .localNodeId(id)
                .blockLookup(name -> blockName.equals(name) ? block : null)
                .ackTimeoutMs(ackTimeoutMs);
    }

    /** Connects engine's outgoing peer registry to a remote server port. */
    private static void wire(ReplicationEngine engine, UUID peerId, int peerPort) throws Exception {
        NodeInfo peer = new NodeInfo(peerId, InetAddress.getByName("127.0.0.1"),
                peerPort, NodeStatus.ALIVE, 0L, System.currentTimeMillis());
        engine.connectAsync(peer);
    }

    private static void assertQuorumCompletes(ReplicationEngine engine,
            String blockName, byte[] key) throws Exception {
        assertReplicate(engine, blockName, key, ConsistencyMode.QUORUM);
    }

    private static void assertStrongCompletes(ReplicationEngine engine,
            String blockName, byte[] key) throws Exception {
        assertReplicate(engine, blockName, key, ConsistencyMode.STRONG);
    }

    private static void assertReplicate(ReplicationEngine engine, String blockName,
            byte[] key, ConsistencyMode mode) throws Exception {
        AtomicReference<Throwable> err = new AtomicReference<>();
        CountDownLatch done = new CountDownLatch(1);
        Thread t = new Thread(() -> {
            try {
                engine.replicate(putEvent(blockName, key, mode));
                done.countDown();
            } catch (Exception e) {
                err.set(e);
                done.countDown();
            }
        });
        t.setDaemon(true);
        t.start();
        assertTrue(done.await(10, TimeUnit.SECONDS), mode + " write did not complete in time");
        assertNull(err.get(), () -> mode + " write failed: " + err.get());
    }

    private static CacheBlock<String, byte[]> block(String name) {
        return CacheBlock.<String, byte[]>builder()
                .name(name).keyCodec(StringCodec.INSTANCE).valueCodec(ByteArrayCodec.INSTANCE)
                .build();
    }

    private static GossipNode gossip(UUID id) throws IOException {
        return GossipNode.builder()
                .nodeId(id).bindAddress("127.0.0.1").bindPort(0).seedPeers(List.of())
                .build();
    }

    private static ReplicationEvent putEvent(String blockName, byte[] value, ConsistencyMode mode) {
        return ReplicationEvent.put(blockName, value,
                new CacheEntry(value, new HLCTimestamp(1L, 0L), 0L, 0L, 0L, UUID.randomUUID()), mode);
    }

    private static Connection noOpConn() {
        return new Connection() {
            @Override public UUID id()                   { return UUID.randomUUID(); }
            @Override public InetAddress remoteAddress() { return null; }
            @Override public void send(RawFrame f)       {}
            @Override public void close()                {}
            @Override public boolean isAlive()           { return true; }
        };
    }

    private static void shutdown(Object o) {
        if (o == null) return;
        try { o.getClass().getMethod("shutdown").invoke(o); } catch (Exception ignored) {}
    }

    private static void close(AutoCloseable c) {
        if (c == null) return;
        try { c.close(); } catch (Exception ignored) {}
    }

    // ── Self-contained 3-node helper ──────────────────────────────────────────

    /**
     * A fully owned engine + server + gossip + block tuple.
     * Tests that need 3 nodes create Nodes as local variables and clean up in try-finally,
     * keeping them independent from the @AfterEach field cleanup.
     */
    private static final class Node {
        final UUID id;
        final TcpServer server;
        final GossipNode gossip;
        final ReplicationEngine engine;
        final CacheBlock<String, byte[]> block;

        private Node(UUID id, TcpServer server, GossipNode gossip,
                     ReplicationEngine engine, CacheBlock<String, byte[]> block) {
            this.id     = id;
            this.server = server;
            this.gossip = gossip;
            this.engine = engine;
            this.block  = block;
        }

        static Node build(String blockName, long ackTimeoutMs) throws IOException {
            UUID id = UUID.randomUUID();
            CacheBlock<String, byte[]> blk = CacheBlock.<String, byte[]>builder()
                    .name(blockName).keyCodec(StringCodec.INSTANCE).valueCodec(ByteArrayCodec.INSTANCE)
                    .build();
            ReplicationEngine.Builder eb = ReplicationEngine.builder()
                    .localNodeId(id)
                    .blockLookup(name -> blockName.equals(name) ? blk : null)
                    .ackTimeoutMs(ackTimeoutMs);
            TcpServer srv = TcpServer.builder().port(0).handler(eb.frameHandler()).build();
            GossipNode gsp = GossipNode.builder()
                    .nodeId(id).bindAddress("127.0.0.1").bindPort(0).seedPeers(List.of())
                    .build();
            ReplicationEngine eng = eb.transport(srv).cluster(gsp).build();
            return new Node(id, srv, gsp, eng, blk);
        }

        void start() throws IOException {
            server.start();
            engine.start();
        }

        void shutdown() {
            try { engine.shutdown(); } catch (Exception ignored) {}
            try { server.shutdown(); } catch (Exception ignored) {}
            try { gossip.shutdown(); } catch (Exception ignored) {}
            try { block.close();     } catch (Exception ignored) {}
        }
    }
}
