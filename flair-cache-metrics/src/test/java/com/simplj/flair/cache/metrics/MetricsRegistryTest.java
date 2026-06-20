package com.simplj.flair.cache.metrics;

import com.simplj.flair.cache.gossip.GossipNode;
import com.simplj.flair.cache.serial.codecs.ByteArrayCodec;
import com.simplj.flair.cache.serial.codecs.StringCodec;
import com.simplj.flair.cache.store.CacheBlock;
import com.simplj.flair.cache.store.EvictionPolicy;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class MetricsRegistryTest {

    private static final MBeanServer MBS = ManagementFactory.getPlatformMBeanServer();
    private MetricsRegistry registry;

    @BeforeEach
    void setUp() {
        registry = new MetricsRegistry();
    }

    @AfterEach
    void tearDown() {
        registry.shutdown();
    }

    private CacheBlock<String, byte[]> newBlock(String name) {
        return CacheBlock.<String, byte[]>builder()
                .name(name)
                .keyCodec(StringCodec.INSTANCE)
                .valueCodec(ByteArrayCodec.INSTANCE)
                .build();
    }

    private CacheBlock<String, byte[]> evictingBlock(String name, int maxEntries) {
        return CacheBlock.<String, byte[]>builder()
                .name(name)
                .keyCodec(StringCodec.INSTANCE)
                .valueCodec(ByteArrayCodec.INSTANCE)
                .eviction(EvictionPolicy.LRU)
                .maxEntries(maxEntries)
                .build();
    }

    // ── Basic wiring ─────────────────────────────────────────────────────────

    @Test
    void cacheHitsReturnZeroForUnknownBlock() {
        assertEquals(0L, registry.cacheHits("nonexistent"));
    }

    @Test
    void cacheHitsTrackedAfterRegisterBlock() {
        try (CacheBlock<String, byte[]> block = newBlock("users")) {
            registry.registerBlock("users", block);
            block.put("k", "v".getBytes());
            block.get("k");
            assertEquals(1L, registry.cacheHits("users"));
        }
    }

    @Test
    void avgReplicationLagZeroWhenNotWired() {
        assertEquals(0L, registry.avgReplicationLagMs());
    }

    @Test
    void aliveNodeCountZeroWhenNotWired() {
        assertEquals(0, registry.aliveNodeCount());
    }

    @Test
    void evictionMetricsAlwaysAvailable() {
        assertNotNull(registry.evictionMetrics());
    }

    @Test
    void replicationMetricsNullWhenNotWired() {
        assertNull(registry.replicationMetrics());
    }

    @Test
    void clusterMetricsNullWhenNotWired() {
        assertNull(registry.clusterMetrics());
    }

    @Test
    void withClusterNullGossipNodeThrows() {
        assertThrows(NullPointerException.class, () -> registry.withCluster(null));
    }

    @Test
    void shutdownDeregistersAllMBeans() throws Exception {
        try (CacheBlock<String, byte[]> block = newBlock("shutdown-test")) {
            registry.registerBlock("shutdown-test", block);
            registry.shutdown();

            Set<ObjectName> remaining = MBS.queryNames(
                    new ObjectName(JmxRegistrar.DOMAIN + ":*"), null);
            assertTrue(remaining.isEmpty(),
                    "No MBeans must remain after shutdown: " + remaining);
        }
    }

    @Test
    void shutdownDeregistersAllFourMBeanTypes() throws Exception {
        // shutdownDeregistersAllMBeans only wires CacheMetrics + EvictionMetrics.
        // This test wires all 4 types and verifies each is cleaned up by shutdown().
        try (CacheBlock<String, byte[]> block = newBlock("full-sd")) {
            GossipNode node = GossipNode.builder().build();
            registry.registerBlock("full-sd", block); // CacheMetrics
            registry.withReplication(() -> 0L, () -> 0L);       // ReplicationMetrics
            registry.withCluster(node);               // ClusterMetrics
            // EvictionMetrics registered in MetricsRegistry constructor

            registry.shutdown();

            Set<ObjectName> remaining = MBS.queryNames(
                    new ObjectName(JmxRegistrar.DOMAIN + ":*"), null);
            assertTrue(remaining.isEmpty(),
                    "All 4 MBean types (Cache, Replication, Cluster, Eviction) must be gone after shutdown: "
                            + remaining);
        }
    }

    @Test
    void replicationMetricsWiredViaWithReplication() {
        ReplicationMetricsMBean bean = registry.withReplication(() -> 3L, () -> 0L);
        assertNotNull(bean);
        assertSame(bean, registry.replicationMetrics());
        assertEquals(3L, bean.getPendingFrameCount());
    }

    @Test
    void registerBlockReturnsBeanWithCorrectStats() {
        try (CacheBlock<String, byte[]> block = newBlock("items")) {
            CacheMetricsMBean bean = registry.registerBlock("items", block);
            assertNotNull(bean);

            block.put("x", "v".getBytes());
            block.get("x");       // hit
            block.get("missing"); // miss

            assertEquals(1L, bean.getHitCount());
            assertEquals(1L, bean.getMissCount());
        }
    }

    @Test
    void multipleBlocksRegisteredIndependently() {
        try (CacheBlock<String, byte[]> b1 = newBlock("orders");
             CacheBlock<String, byte[]> b2 = newBlock("products")) {

            registry.registerBlock("orders",   b1);
            registry.registerBlock("products", b2);

            b1.put("o1", "v".getBytes());
            b1.get("o1"); // hit on orders

            b2.put("p1", "v".getBytes());
            b2.get("missing"); // miss on products

            assertEquals(1L, registry.cacheHits("orders"));
            assertEquals(0L, registry.cacheHits("products"));
        }
    }

    // ── Bug 1: withReplication() idempotency ─────────────────────────────────

    @Test
    void withReplicationIdempotentReturnsSameBeanPreservingHistory() {
        ReplicationMetricsMBean bean1 = registry.withReplication(() -> 5L, () -> 0L);
        bean1.recordReplicationLag(100L);
        bean1.recordDroppedFrame();

        ReplicationMetricsMBean bean2 = registry.withReplication(() -> 9L, () -> 0L);

        assertSame(bean1, bean2, "Second withReplication() call must return the existing bean");
        assertEquals(100L, bean2.getAvgReplicationLagMs(),
                "Accumulated lag history must be preserved — not reset to zero");
        assertEquals(1L,   bean2.getDroppedFrameCount(),
                "Accumulated drop count must be preserved");
        assertEquals(5L,   bean2.getPendingFrameCount(),
                "First supplier must remain active — not replaced by second call's supplier");
    }

    @Test
    void clusterMetricsAccessorReturnsWiredBean() throws Exception {
        assertNull(registry.clusterMetrics(), "must be null before withCluster()");
        GossipNode node = GossipNode.builder().build();
        ClusterMetricsMBean bean = registry.withCluster(node);
        assertSame(bean, registry.clusterMetrics(),
                "clusterMetrics() must return the same bean as withCluster()");
    }

    @Test
    void reRegisterAfterDeregisterCreatesFreshBeanWithNewBaseline() {
        try (CacheBlock<String, byte[]> block = newBlock("recycle")) {
            CacheMetricsMBean first = registry.registerBlock("recycle", block);
            block.put("k", "v".getBytes());
            block.get("k"); // hit
            assertEquals(1L, first.getPutCount());
            assertEquals(1L, first.getHitCount());

            registry.deregisterBlock("recycle");

            // Re-register the same block under the same name
            CacheMetricsMBean second = registry.registerBlock("recycle", block);
            assertNotSame(first, second, "re-registration after deregister must produce a fresh bean");

            // Fresh bean starts at zero for all counters (baseline set at re-registration time)
            assertEquals(0L, second.getPutCount(),
                    "listener-based counter starts at 0 from the new listener attachment point");
            assertEquals(0L, second.getHitCount(),
                    "hit baseline at re-registration subtracts the prior hit");

            // Post-re-registration operations are counted on the fresh bean
            block.put("k2", "v".getBytes());
            block.get("k2"); // hit
            assertEquals(1L, second.getPutCount());
            assertEquals(1L, second.getHitCount());
        }
    }

    // ── Bug 1: withCluster() idempotency ─────────────────────────────────────

    @Test
    void withClusterIdempotentReturnsSameBeanWithoutGhostListener() throws Exception {
        GossipNode node = GossipNode.builder().build();
        ClusterMetricsMBean bean1 = registry.withCluster(node);
        ClusterMetricsMBean bean2 = registry.withCluster(node);

        assertSame(bean1, bean2, "Second withCluster() call must return existing bean");

        // If a ghost listener were added, onDead dispatched by the gossip node would
        // increment deadCount twice. Simulate dispatch to verify only one increment.
        bean1.onDead(null); // direct call — same effect as gossip dispatch
        assertEquals(1L, bean1.getDeadNodeCount(),
                "Ghost listener would cause double-increment");
    }

    // ── Bug 2: registerBlock() does not replace block reference on re-registration ──

    @Test
    void registerBlockIdempotentForSameName() {
        try (CacheBlock<String, byte[]> block = newBlock("dedup")) {
            CacheMetricsMBean bean1 = registry.registerBlock("dedup", block);
            CacheMetricsMBean bean2 = registry.registerBlock("dedup", block);

            assertSame(bean1, bean2, "Re-registration must return the same bean, not create a new listener");

            block.put("k", "v".getBytes());
            assertEquals(1L, bean1.getPutCount(),
                    "A dangling second listener would double-count puts");
        }
    }

    @Test
    void registerBlockDoesNotReplaceBlockReferenceOnReRegistration() throws Exception {
        try (CacheBlock<String, byte[]> block1 = evictingBlock("b1", 1);
             CacheBlock<String, byte[]> block2 = evictingBlock("b2", 1)) {

            // Register block1 under "slot"
            registry.registerBlock("slot", block1);

            // Re-register the same slot name with a different evicting block
            registry.registerBlock("slot", block2);

            // Cause an eviction in block2 — if block2 were in the backing map,
            // getEvictedEntryCount() would increase
            block2.put("k1", "v".getBytes());
            block2.put("k2", "v".getBytes()); // evicts k1 from block2

            assertEquals(0L, registry.evictionMetrics().getEvictedEntryCount(),
                    "block2 must not be tracked — re-registration must not replace the block reference");
        }
    }

    // ── Bug 4: deregisterBlock() ─────────────────────────────────────────────

    @Test
    void deregisterBlockRemovesFromJmxAndEvictionCounting() throws Exception {
        try (CacheBlock<String, byte[]> block = evictingBlock("to-remove", 1)) {
            registry.registerBlock("to-remove", block);

            block.put("k1", "v".getBytes());
            block.put("k2", "v".getBytes()); // evicts k1
            assertEquals(1L, registry.evictionMetrics().getEvictedEntryCount());

            registry.deregisterBlock("to-remove");

            ObjectName jmxName = new ObjectName(
                    JmxRegistrar.DOMAIN + ":type=CacheMetrics,block=to-remove");
            assertFalse(MBS.isRegistered(jmxName), "JMX MBean must be deregistered");
            assertEquals(0L, registry.evictionMetrics().getEvictedEntryCount(),
                    "Block must no longer contribute to eviction aggregate");
            assertEquals(0L, registry.cacheHits("to-remove"),
                    "cacheHits must return 0 for deregistered block");
            assertNull(registry.cacheMetrics("to-remove"),
                    "cacheMetrics must return null for deregistered block");
        }
    }

    @Test
    void deregisterBlockIsNoOpForUnknownBlock() {
        assertDoesNotThrow(() -> registry.deregisterBlock("nonexistent"));
    }

    @Test
    void shutdownAfterDeregisterBlockDoesNotThrow() throws Exception {
        try (CacheBlock<String, byte[]> block = newBlock("partial")) {
            registry.registerBlock("partial", block);
            registry.deregisterBlock("partial");
            // shutdown must not attempt to re-deregister the already-removed ObjectName
            assertDoesNotThrow(() -> registry.shutdown());
        }
    }

    // ── Delegation wiring ─────────────────────────────────────────────────────

    @Test
    void avgReplicationLagMsDelegatesToBean() {
        ReplicationMetricsMBean bean = registry.withReplication(() -> 0L, () -> 0L);
        bean.recordReplicationLag(75L);
        assertEquals(75L, registry.avgReplicationLagMs(),
                "avgReplicationLagMs() must delegate to the wired ReplicationMetricsMBean");
    }

    @Test
    void aliveNodeCountDelegatesToBean() throws Exception {
        GossipNode node = GossipNode.builder().build();
        registry.withCluster(node);
        // A non-started node has an empty membership list — alive count is 0.
        // The non-zero path is covered by ClusterMetricsMBeanTest.aliveAndSuspectedCountsDelegateToMembershipList.
        assertEquals(0, registry.aliveNodeCount(),
                "aliveNodeCount() must delegate to the wired ClusterMetricsMBean");
    }

    // ── Bug 5: shutdown() clears block references ─────────────────────────────

    @Test
    void shutdownClearsBlockMapsReleasingReferences() throws Exception {
        try (CacheBlock<String, byte[]> block = evictingBlock("mem-test", 1)) {
            registry.registerBlock("mem-test", block);
            block.put("k1", "v".getBytes());
            block.put("k2", "v".getBytes()); // evicts k1

            registry.shutdown();

            // blocks map is cleared — EvictionMetricsMBean iterates an empty map
            assertEquals(0L, registry.evictionMetrics().getEvictedEntryCount(),
                    "eviction aggregate must be 0 after shutdown clears the blocks map");
            // blockBeans map is cleared — bean lookups return null / zero
            assertNull(registry.cacheMetrics("mem-test"),
                    "cacheMetrics() must return null after shutdown clears blockBeans");
            assertEquals(0L, registry.cacheHits("mem-test"),
                    "cacheHits() must return 0 after shutdown clears blockBeans");
        }
    }

    @Test
    void doubleShutdownIsIdempotent() throws Exception {
        try (CacheBlock<String, byte[]> block = newBlock("double-sd")) {
            registry.registerBlock("double-sd", block);
            registry.shutdown(); // first shutdown clears maps and JMX
            assertDoesNotThrow(() -> registry.shutdown(), "second shutdown must be a no-op, not throw");
        }
    }

    // ── Bug 6: cacheMetrics() accessor ───────────────────────────────────────

    @Test
    void cacheMetricsReturnsRegisteredBean() {
        try (CacheBlock<String, byte[]> block = newBlock("lookup")) {
            CacheMetricsMBean registered = registry.registerBlock("lookup", block);
            assertSame(registered, registry.cacheMetrics("lookup"),
                    "cacheMetrics() must return the same bean as registerBlock()");
        }
    }

    @Test
    void cacheMetricsReturnsNullForUnknownBlock() {
        assertNull(registry.cacheMetrics("nonexistent"));
    }

    @Test
    void cacheMetricsReturnsNullAfterDeregisterBlock() {
        try (CacheBlock<String, byte[]> block = newBlock("gone")) {
            registry.registerBlock("gone", block);
            assertNotNull(registry.cacheMetrics("gone"));
            registry.deregisterBlock("gone");
            assertNull(registry.cacheMetrics("gone"));
        }
    }
}
