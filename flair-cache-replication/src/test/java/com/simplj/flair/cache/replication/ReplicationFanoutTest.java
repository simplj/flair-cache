package com.simplj.flair.cache.replication;

import com.simplj.flair.cache.gossip.GossipNode;
import com.simplj.flair.cache.hlc.HLCTimestamp;
import com.simplj.flair.cache.store.CacheEntry;
import com.simplj.flair.cache.transport.TcpServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests ReplicationFanout mechanics through the ReplicationEngine EVENTUAL path.
 *
 * EVENTUAL mode enqueues frames into the 65,536-entry LinkedBlockingQueue.
 * No peers are connected so frames are dropped silently in the fanout step —
 * we are only verifying queue bounds and shutdown behaviour, not delivery.
 */
@Timeout(15)
class ReplicationFanoutTest {

    // Must match ReplicationFanout's internal LinkedBlockingQueue capacity
    private static final int QUEUE_CAPACITY = 65_536;

    /**
     * Filling the queue to capacity must never throw; EVENTUAL drops excess frames
     * with a warning log rather than raising an exception to the caller.
     *
     * In practice the fanout thread drains the queue every 2ms, so this test races
     * with the drain. We therefore assert the absence of exceptions, not a specific
     * accepted-vs-rejected ratio — the queue bound is an implementation invariant,
     * not a caller-visible error condition for EVENTUAL mode.
     */
    @Test
    void eventual_replicate_beyond_capacity_does_not_throw() throws Exception {
        try (EngineScope scope = new EngineScope()) {
            scope.engine.start();
            for (int i = 0; i <= QUEUE_CAPACITY + 10; i++) {
                assertDoesNotThrow(
                        () -> scope.engine.replicate(putEvent("items", "k".getBytes())),
                        "EVENTUAL replicate must never throw, even when queue is full");
            }
        }
    }

    @Test
    void shutdown_with_queued_events_does_not_throw() throws Exception {
        try (EngineScope scope = new EngineScope()) {
            scope.engine.start();
            for (int i = 0; i < 200; i++) {
                scope.engine.replicate(putEvent("items", ("k" + i).getBytes()));
            }
            assertDoesNotThrow(scope.engine::shutdown);
        }
    }

    @Test
    void shutdown_with_empty_queue_does_not_throw() throws IOException {
        try (EngineScope scope = new EngineScope()) {
            scope.engine.start();
            assertDoesNotThrow(scope.engine::shutdown);
        }
    }

    /**
     * Verifies the hard queue capacity boundary via direct package-private fanout access.
     *
     * The fanout thread is intentionally NOT started so no drain competes with the fill,
     * making the boundary assertion deterministic: the 65,537th offer is always rejected.
     */
    @Test
    void fanout_queue_rejects_offer_at_capacity_plus_one() throws Exception {
        try (EngineScope scope = new EngineScope()) {
            PeerRegistry registry = new PeerRegistry(UUID.randomUUID(), scope.server.eventLoop(), null);
            ReplicationFanout fanout = new ReplicationFanout(
                    registry, scope.gossip.members(), UUID.randomUUID(), 2L, 64);
            // Do NOT start the fanout drain thread — queue must stay full
            try {
                for (int i = 0; i < QUEUE_CAPACITY; i++) {
                    assertTrue(fanout.offer(new ReplicationFanout.QueuedEvent(
                                    putEvent("items", ("k" + i).getBytes()), i, false)),
                            "offer #" + i + " must succeed within capacity");
                }
                assertFalse(fanout.offer(new ReplicationFanout.QueuedEvent(
                                putEvent("items", "overflow".getBytes()), QUEUE_CAPACITY + 1, false)),
                        "offer beyond capacity must return false");
            } finally {
                registry.shutdown();
                fanout.shutdown();
            }
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static ReplicationEvent putEvent(String blockName, byte[] key) {
        return ReplicationEvent.put(blockName, key,
                new CacheEntry(key, new HLCTimestamp(1L, 0L), 0L, 0L, 0L, UUID.randomUUID()),
                ConsistencyMode.EVENTUAL);
    }

    private static final class EngineScope implements AutoCloseable {
        final TcpServer server;
        final GossipNode gossip;
        final ReplicationEngine engine;

        EngineScope() throws IOException {
            UUID id = UUID.randomUUID();
            ReplicationEngine.Builder eb = ReplicationEngine.builder()
                    .localNodeId(id)
                    .blockLookup(name -> null);
            server = TcpServer.builder().port(0).handler(eb.frameHandler()).build();
            gossip = GossipNode.builder()
                    .nodeId(id).bindAddress("127.0.0.1").bindPort(0).seedPeers(List.of())
                    .build();
            engine = eb.transport(server).cluster(gossip).build();
        }

        @Override
        public void close() {
            try { engine.shutdown(); } catch (Exception ignored) {}
            try { server.shutdown(); } catch (Exception ignored) {}
            try { gossip.shutdown(); } catch (Exception ignored) {}
        }
    }
}
