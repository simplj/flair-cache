package com.simplj.flair.cache.replication;

import com.simplj.flair.cache.gossip.GossipNode;
import com.simplj.flair.cache.hlc.HLCTimestamp;
import com.simplj.flair.cache.hlc.HybridLogicalClock;
import com.simplj.flair.cache.serial.codecs.ByteArrayCodec;
import com.simplj.flair.cache.serial.codecs.StringCodec;
import com.simplj.flair.cache.store.CacheBlock;
import com.simplj.flair.cache.store.CacheEntry;
import com.simplj.flair.cache.transport.Connection;
import com.simplj.flair.cache.transport.FrameHandler;
import com.simplj.flair.cache.transport.RawFrame;
import com.simplj.flair.cache.transport.TcpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.net.InetAddress;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests the incoming frame dispatch path: PUT/DELETE/ACK handling, conflict resolution,
 * HLC update, ACK echo, and guard for null engine (pre-start).
 *
 * The IncomingHandler is accessed by casting frameHandler() to IncomingHandler — valid
 * because both types are in the same package.
 */
@Timeout(15)
class IncomingHandlerTest {

    private TcpServer server;
    private GossipNode gossip;
    private ReplicationEngine engine;
    private CacheBlock<String, byte[]> block;
    private IncomingHandler handler;

    @BeforeEach
    void setUp() throws IOException {
        block = CacheBlock.<String, byte[]>builder()
                .name("items")
                .keyCodec(StringCodec.INSTANCE)
                .valueCodec(ByteArrayCodec.INSTANCE)
                .build();

        UUID nodeId = UUID.randomUUID();
        ReplicationEngine.Builder eb = ReplicationEngine.builder()
                .localNodeId(nodeId)
                .blockLookup(name -> "items".equals(name) ? block : null);

        FrameHandler fh = eb.frameHandler();
        handler = (IncomingHandler) fh;

        server = TcpServer.builder().port(0).handler(fh).build();
        gossip = GossipNode.builder()
                .nodeId(nodeId)
                .bindAddress("127.0.0.1")
                .bindPort(0)
                .seedPeers(List.of())
                .build();

        engine = eb.transport(server).cluster(gossip).build();
        engine.start();
    }

    @AfterEach
    void tearDown() {
        try { engine.shutdown();  } catch (Exception ignored) {}
        try { server.shutdown();  } catch (Exception ignored) {}
        try { gossip.shutdown();  } catch (Exception ignored) {}
        try { block.close();      } catch (Exception ignored) {}
    }

    // ── PUT — store application ───────────────────────────────────────────────

    @Test
    void put_writes_entry_when_block_is_empty() {
        byte[] key = "k1".getBytes();
        byte[] value = "hello".getBytes();
        CacheEntry entry = entry(value, 100L, 0L);

        handler.onFrame(noOp(), encodePut(1L, false, "items", key, entry));

        CacheEntry stored = block.getRaw(key);
        assertNotNull(stored);
        assertArrayEquals(value, stored.value());
        assertEquals(new HLCTimestamp(100L, 0L), stored.hlc());
    }

    @Test
    void put_incoming_higher_hlc_overwrites_existing() {
        byte[] key = "k2".getBytes();
        block.putRaw(key, entry("old".getBytes(), 50L, 0L));

        handler.onFrame(noOp(), encodePut(2L, false, "items", key, entry("new".getBytes(), 200L, 0L)));

        assertArrayEquals("new".getBytes(), block.getRaw(key).value());
    }

    @Test
    void put_existing_higher_hlc_keeps_existing() {
        byte[] key = "k3".getBytes();
        block.putRaw(key, entry("original".getBytes(), 500L, 0L));

        handler.onFrame(noOp(), encodePut(3L, false, "items", key, entry("stale".getBytes(), 100L, 0L)));

        assertArrayEquals("original".getBytes(), block.getRaw(key).value());
    }

    @Test
    void put_existing_wins_on_tiebreak_and_entry_is_unchanged() {
        // Same logical HLC, different nodeId — the entry with the higher nodeId UUID wins.
        // This exercises the branch where incoming loses via tiebreak (not by logical clock),
        // yet updateClock() must still be called so the local HLC tracks remote progress.
        HLCTimestamp hlc = new HLCTimestamp(100L, 0L);
        UUID highNode = new UUID(1L, 0L); // larger MSB — wins tiebreak
        UUID lowNode  = new UUID(0L, 1L); // smaller MSB — loses tiebreak

        byte[] key = "k4".getBytes();
        CacheEntry existingEntry = new CacheEntry("winner".getBytes(), hlc, 0, 0, 0, highNode);
        block.putRaw(key, existingEntry);

        CacheEntry incomingEntry = new CacheEntry("loser".getBytes(), hlc, 0, 0, 0, lowNode);
        handler.onFrame(noOp(), FrameEncoder.encodePut(4L, false,
                new ReplicationEvent.PutEvent("items", key, incomingEntry, ConsistencyMode.EVENTUAL)));

        // highNode wins tiebreak → existing is preserved
        assertArrayEquals("winner".getBytes(), block.getRaw(key).value());
    }

    // ── PUT — ACK behaviour ───────────────────────────────────────────────────

    @Test
    void put_sends_ack_when_needsAck_true() {
        byte[] key = "k5".getBytes();
        Capture cap = new Capture();

        handler.onFrame(cap, encodePut(55L, true, "items", key, entry(new byte[0], 1L, 0L)));

        assertEquals(1, cap.sent.size());
        assertEquals(FrameEncoder.TYPE_ACK, cap.sent.get(0).type());
        assertEquals(55L, FrameDecoder.decodeAck(cap.sent.get(0).payload()));
    }

    @Test
    void put_does_not_send_ack_when_needsAck_false() {
        byte[] key = "k6".getBytes();
        Capture cap = new Capture();

        handler.onFrame(cap, encodePut(66L, false, "items", key, entry(new byte[0], 1L, 0L)));

        assertTrue(cap.sent.isEmpty());
    }

    // ── PUT — HLC advancement on conflict loss ────────────────────────────────

    @Test
    void hlc_advances_even_when_incoming_put_loses_conflict() throws IOException {
        // This test injects a shared HybridLogicalClock so we can inspect it directly.
        // A self-contained block/handler/engine is built here; the @BeforeEach fixtures
        // use the default (un-injectable) clock.
        HybridLogicalClock sharedClock = new HybridLogicalClock();
        CacheBlock<String, byte[]> localBlock = CacheBlock.<String, byte[]>builder()
                .name("items").keyCodec(StringCodec.INSTANCE).valueCodec(ByteArrayCodec.INSTANCE)
                .hlc(sharedClock).build();

        UUID nodeId = UUID.randomUUID();
        ReplicationEngine.Builder eb = ReplicationEngine.builder()
                .localNodeId(nodeId)
                .blockLookup(name -> "items".equals(name) ? localBlock : null);
        FrameHandler fh = eb.frameHandler();
        IncomingHandler localHandler = (IncomingHandler) fh;
        TcpServer srv = TcpServer.builder().port(0).handler(fh).build();
        GossipNode gsp = GossipNode.builder()
                .nodeId(nodeId).bindAddress("127.0.0.1").bindPort(0).seedPeers(List.of()).build();
        ReplicationEngine eng = eb.transport(srv).cluster(gsp).build();
        eng.start();

        try {
            long farFuture = System.currentTimeMillis() + 1_000_000_000L; // ~11 days ahead
            HLCTimestamp farHlc = new HLCTimestamp(farFuture, 0L);
            UUID highNode = new UUID(1L, 0L);
            UUID lowNode  = new UUID(0L, 1L);
            byte[] key = "contested".getBytes();

            // putRaw bypasses the internal clock — sharedClock stays at wall time
            localBlock.putRaw(key, new CacheEntry("winner".getBytes(), farHlc, 0, 0, 0, highNode));

            // Incoming: same farFuture HLC, lowNode → loses tiebreak.
            // IncomingHandler calls block.updateClock(farHlc) BEFORE resolving the conflict.
            CacheEntry incoming = new CacheEntry("loser".getBytes(), farHlc, 0, 0, 0, lowNode);
            localHandler.onFrame(noOp(), encodePut(99L, false, "items", key, incoming));

            // Existing must be preserved
            assertArrayEquals("winner".getBytes(), localBlock.getRaw(key).value());

            // After updateClock(farFuture, 0), sharedClock.now() must return logical >= farFuture.
            // This is the invariant: causal time advances even when the write is rejected.
            HLCTimestamp advanced = sharedClock.now();
            assertTrue(advanced.logical() >= farFuture,
                    "HLC must advance to at least incoming.hlc.logical even when write loses; "
                    + "got logical=" + advanced.logical() + " expected >= " + farFuture);
        } finally {
            try { eng.shutdown();    } catch (Exception ignored) {}
            try { srv.shutdown();    } catch (Exception ignored) {}
            try { gsp.shutdown();    } catch (Exception ignored) {}
            try { localBlock.close(); } catch (Exception ignored) {}
        }
    }

    // ── PUT — callback ────────────────────────────────────────────────────────

    @Test
    void put_fires_incoming_callback_with_eventual_mode() {
        AtomicReference<ReplicationEvent> received = new AtomicReference<>();
        engine.onIncoming(received::set);

        byte[] key = "k7".getBytes();
        handler.onFrame(noOp(), encodePut(7L, false, "items", key, entry("v".getBytes(), 1L, 0L)));

        assertNotNull(received.get());
        assertInstanceOf(ReplicationEvent.PutEvent.class, received.get());
        assertEquals("items", received.get().blockName());
        assertEquals(ConsistencyMode.EVENTUAL, received.get().mode()); // sentinel for wire-applied events
    }

    @Test
    void put_fires_callback_even_when_existing_wins() {
        AtomicReference<ReplicationEvent> received = new AtomicReference<>();
        engine.onIncoming(received::set);

        byte[] key = "k8".getBytes();
        block.putRaw(key, entry("winner".getBytes(), 999L, 0L));

        handler.onFrame(noOp(), encodePut(8L, false, "items", key, entry("loser".getBytes(), 1L, 0L)));

        assertNotNull(received.get(), "callback must fire regardless of conflict outcome");
    }

    // ── DELETE ────────────────────────────────────────────────────────────────

    @Test
    void delete_removes_entry_from_block() {
        byte[] key = "k9".getBytes();
        block.putRaw(key, entry("v".getBytes(), 1L, 0L));
        assertNotNull(block.getRaw(key));

        handler.onFrame(noOp(), encodeDelete(9L, false, "items", key));

        assertNull(block.getRaw(key));
    }

    @Test
    void delete_on_absent_key_does_not_crash() {
        byte[] key = "k10".getBytes();
        assertNull(block.getRaw(key)); // nothing there

        assertDoesNotThrow(() ->
                handler.onFrame(noOp(), encodeDelete(10L, false, "items", key)));
    }

    @Test
    void delete_sends_ack_when_needsAck_true() {
        byte[] key = "k11".getBytes();
        Capture cap = new Capture();

        handler.onFrame(cap, encodeDelete(88L, true, "items", key));

        assertEquals(1, cap.sent.size());
        assertEquals(FrameEncoder.TYPE_ACK, cap.sent.get(0).type());
        assertEquals(88L, FrameDecoder.decodeAck(cap.sent.get(0).payload()));
    }

    @Test
    void delete_with_older_hlc_does_not_remove_newer_put() {
        // PUT lands at HLC logical=10. An out-of-order DELETE carrying logical=5 (older) must
        // NOT remove the entry — applying it would clobber a newer write and diverge the cluster.
        byte[] key = "del-lww-1".getBytes();
        block.putRaw(key, entry("fresh".getBytes(), 10L, 0L));

        handler.onFrame(noOp(), encodeDelete(70L, false, "items", key, new HLCTimestamp(5L, 0L)));

        assertNotNull(block.getRaw(key), "stale DELETE (older HLC) must be rejected");
        assertArrayEquals("fresh".getBytes(), block.getRaw(key).value());
    }

    @Test
    void delete_with_newer_hlc_removes_entry() {
        // PUT lands at HLC logical=5. A DELETE carrying logical=10 (newer) wins and removes it.
        byte[] key = "del-lww-2".getBytes();
        block.putRaw(key, entry("stale".getBytes(), 5L, 0L));

        handler.onFrame(noOp(), encodeDelete(71L, false, "items", key, new HLCTimestamp(10L, 0L)));

        assertNull(block.getRaw(key), "newer DELETE must win and remove the entry");
    }

    @Test
    void delete_does_not_send_ack_when_needsAck_false() {
        byte[] key = "k12".getBytes();
        Capture cap = new Capture();

        handler.onFrame(cap, encodeDelete(89L, false, "items", key));

        assertTrue(cap.sent.isEmpty());
    }

    @Test
    void delete_fires_incoming_callback() {
        AtomicReference<ReplicationEvent> received = new AtomicReference<>();
        engine.onIncoming(received::set);

        byte[] key = "k13".getBytes();
        handler.onFrame(noOp(), encodeDelete(13L, false, "items", key));

        assertNotNull(received.get());
        assertInstanceOf(ReplicationEvent.DeleteEvent.class, received.get());
        assertEquals(ConsistencyMode.EVENTUAL, received.get().mode());
    }

    // ── ACK ───────────────────────────────────────────────────────────────────

    @Test
    void ack_frame_dispatches_to_ack_tracker_and_completes_pending_write() {
        long frameId = 99L;
        PendingWrite pw = new PendingWrite(frameId, 1, System.currentTimeMillis() + 5_000);
        engine.ackTracker().track(pw);

        handler.onFrame(noOp(), FrameEncoder.encodeAck(frameId));

        assertTrue(pw.future.isDone());
        assertFalse(pw.future.isCompletedExceptionally());
    }

    @Test
    void ack_with_unknown_frameId_is_noop() {
        assertDoesNotThrow(() -> handler.onFrame(noOp(), FrameEncoder.encodeAck(Long.MAX_VALUE)));
    }

    // ── Edge cases ────────────────────────────────────────────────────────────

    @Test
    void frames_before_start_are_silently_dropped() {
        IncomingHandler freshHandler = new IncomingHandler(); // no engine set
        // Must not throw regardless of frame type
        assertDoesNotThrow(() -> freshHandler.onFrame(noOp(), FrameEncoder.encodeAck(1L)));
    }

    @Test
    void put_with_unknown_block_name_does_not_crash() {
        byte[] key = "k".getBytes();
        assertDoesNotThrow(() ->
                handler.onFrame(noOp(),
                        encodePut(1L, false, "no-such-block", key, entry(new byte[0], 1L, 0L))));
    }

    @Test
    void unknown_frame_type_is_ignored() {
        RawFrame unknown = new RawFrame((byte) 0xFF, new byte[]{1, 2, 3});
        assertDoesNotThrow(() -> handler.onFrame(noOp(), unknown));
    }

    @Test
    void corrupt_put_payload_does_not_crash() {
        RawFrame bad = new RawFrame(FrameEncoder.TYPE_PUT, new byte[]{1, 2, 3}); // too short
        assertDoesNotThrow(() -> handler.onFrame(noOp(), bad));
    }

    @Test
    void corrupt_delete_payload_does_not_crash() {
        RawFrame bad = new RawFrame(FrameEncoder.TYPE_DELETE, new byte[]{1}); // too short
        assertDoesNotThrow(() -> handler.onFrame(noOp(), bad));
    }

    // ── PING / PONG ───────────────────────────────────────────────────────────

    @Test
    void ping_receives_pong_carrying_local_node_id() {
        UUID senderId = UUID.randomUUID();
        Capture cap = new Capture();

        handler.onFrame(cap, FrameEncoder.encodePing(senderId));

        assertEquals(1, cap.sent.size());
        RawFrame response = cap.sent.get(0);
        assertEquals(FrameEncoder.TYPE_PONG, response.type());
        assertEquals(engine.localNodeId(), FrameDecoder.decodePingPong(response.payload()));
    }

    @Test
    void pong_does_not_trigger_any_reply() {
        UUID senderId = UUID.randomUUID();
        Capture cap = new Capture();

        handler.onFrame(cap, FrameEncoder.encodePong(senderId));

        assertTrue(cap.sent.isEmpty(), "PONG must not trigger any reply frame");
    }

    @Test
    void corrupt_ping_payload_still_sends_pong() {
        // Even with an unreadable sender ID the local node still responds —
        // we know our own ID and the connection is clearly alive.
        RawFrame bad = new RawFrame(FrameEncoder.TYPE_PING, new byte[]{1, 2}); // too short
        Capture cap = new Capture();

        assertDoesNotThrow(() -> handler.onFrame(cap, bad));
        assertEquals(1, cap.sent.size());
        assertEquals(FrameEncoder.TYPE_PONG, cap.sent.get(0).type());
    }

    // ── Bug-2 regression: TOCTOU race in handlePut (ConcurrentHashMap.compute vs getRaw+putRaw) ──
    //
    // Pre-fix, handlePut() did: getRaw(key) → resolve → putRaw(winner). Two worker threads racing
    // on the same key can both see existing=null, both compute winner=their-own-entry, and the
    // second writer blindly overwrites the first without going through LWW. The fix replaces that
    // with putRawIfBetter() which wraps the entire read-modify-write in ConcurrentHashMap.compute(),
    // serialising per-key conflict resolution.
    //
    // Test strategy: 500 rounds, each starting from an empty slot (block.clear()). In each round
    // both entries compete simultaneously so both threads can see existing=null. Without compute(),
    // LOW (HLC=1) overwrites HIGH (HLC=100) in roughly half of all rounds — virtually certain to
    // fail at least once across 500 rounds. With compute(), HIGH always wins. Reverting
    // IncomingHandler.handlePut() to the old getRaw+putRaw path makes this test fail.

    @Test
    void concurrent_put_low_hlc_cannot_overwrite_high_hlc() throws InterruptedException {
        byte[] key = "race-key".getBytes();
        UUID highNode = new UUID(1L, 0L);
        UUID lowNode  = new UUID(0L, 1L);
        CacheEntry high = new CacheEntry("HIGH".getBytes(), new HLCTimestamp(100L, 0L), 0, 0, 0, highNode);
        CacheEntry low  = new CacheEntry("LOW".getBytes(),  new HLCTimestamp(1L, 0L),   0, 0, 0, lowNode);

        int rounds = 500;
        for (int round = 0; round < rounds; round++) {
            // Fresh slot: both threads simultaneously see existing=null — maximises the race window.
            block.clear();

            CountDownLatch start = new CountDownLatch(1);
            CountDownLatch done  = new CountDownLatch(2);
            int frameBase = round * 2;

            Thread tHigh = new Thread(() -> {
                try { start.await(); } catch (InterruptedException ignored) { Thread.currentThread().interrupt(); }
                handler.onFrame(noOp(), encodePut(frameBase,     false, "items", key, high));
                done.countDown();
            }, "test-high-" + round);

            Thread tLow = new Thread(() -> {
                try { start.await(); } catch (InterruptedException ignored) { Thread.currentThread().interrupt(); }
                handler.onFrame(noOp(), encodePut(frameBase + 1, false, "items", key, low));
                done.countDown();
            }, "test-low-" + round);

            tHigh.start();
            tLow.start();
            start.countDown();
            assertTrue(done.await(10, TimeUnit.SECONDS), "round " + round + " timed out");

            CacheEntry result = block.getRaw(key);
            assertNotNull(result, "round " + round + ": key must exist after concurrent puts");
            assertArrayEquals("HIGH".getBytes(), result.value(),
                    "round " + round + ": LWW loser (LOW, HLC=1) overwrote winner (HIGH, HLC=100) "
                    + "— TOCTOU race: both threads saw existing=null and the last writer won");
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static CacheEntry entry(byte[] value, long logical, long counter) {
        return new CacheEntry(value, new HLCTimestamp(logical, counter), 0L, 0L, 0L, UUID.randomUUID());
    }

    private static RawFrame encodePut(long frameId, boolean needsAck,
                                      String blockName, byte[] key, CacheEntry entry) {
        return FrameEncoder.encodePut(frameId, needsAck,
                new ReplicationEvent.PutEvent(blockName, key, entry, ConsistencyMode.EVENTUAL));
    }

    private static RawFrame encodeDelete(long frameId, boolean needsAck,
                                         String blockName, byte[] key) {
        return encodeDelete(frameId, needsAck, blockName, key, new HLCTimestamp(1L, 0L));
    }

    private static RawFrame encodeDelete(long frameId, boolean needsAck,
                                         String blockName, byte[] key, HLCTimestamp hlc) {
        return FrameEncoder.encodeDelete(frameId, needsAck,
                new ReplicationEvent.DeleteEvent(blockName, key,
                        hlc, UUID.randomUUID(), ConsistencyMode.EVENTUAL));
    }

    private static Connection noOp() {
        return new Connection() {
            @Override public UUID id()                        { return UUID.randomUUID(); }
            @Override public InetAddress remoteAddress()      { return null; }
            @Override public void send(RawFrame f)            {}
            @Override public void close()                     {}
            @Override public boolean isAlive()                { return true; }
        };
    }

    private static final class Capture implements Connection {
        final List<RawFrame> sent = new CopyOnWriteArrayList<>();
        @Override public UUID id()                      { return UUID.randomUUID(); }
        @Override public InetAddress remoteAddress()    { return null; }
        @Override public void send(RawFrame f)          { sent.add(f); }
        @Override public void close()                   {}
        @Override public boolean isAlive()              { return true; }
    }
}
