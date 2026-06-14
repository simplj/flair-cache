package com.simplj.flair.cache.metrics;

import com.simplj.flair.cache.gossip.GossipNode;
import com.simplj.flair.cache.serial.codecs.ByteArrayCodec;
import com.simplj.flair.cache.serial.codecs.StringCodec;
import com.simplj.flair.cache.store.CacheBlock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.*;

class JmxRegistrarTest {

    private static final MBeanServer MBS = ManagementFactory.getPlatformMBeanServer();
    private JmxRegistrar registrar;

    @BeforeEach
    void setUp() {
        registrar = new JmxRegistrar();
    }

    @AfterEach
    void tearDown() {
        registrar.deregisterAll();
    }

    private CacheBlock<String, byte[]> newBlock(String name) {
        return CacheBlock.<String, byte[]>builder()
                .name(name)
                .keyCodec(StringCodec.INSTANCE)
                .valueCodec(ByteArrayCodec.INSTANCE)
                .build();
    }

    @Test
    void cacheMetricsMBeanIsReadableViaJmx() throws Exception {
        try (CacheBlock<String, byte[]> block = newBlock("products")) {
            CacheMetricsMBean bean = new CacheMetricsMBean(block);
            registrar.registerCacheMetrics("products", bean);

            block.put("k", "v".getBytes());
            block.get("k");       // hit
            block.get("missing"); // miss

            ObjectName name = new ObjectName(JmxRegistrar.DOMAIN + ":type=CacheMetrics,block=products");
            assertTrue(MBS.isRegistered(name));

            long hits   = (Long) MBS.getAttribute(name, "HitCount");
            long misses = (Long) MBS.getAttribute(name, "MissCount");
            assertEquals(1L, hits);
            assertEquals(1L, misses);
        }
    }

    @Test
    void hitRatePercentReadableViaJmx() throws Exception {
        try (CacheBlock<String, byte[]> block = newBlock("rate-test")) {
            CacheMetricsMBean bean = new CacheMetricsMBean(block);
            registrar.registerCacheMetrics("rate-test", bean);

            block.put("k", "v".getBytes());
            block.get("k");       // hit
            block.get("missing"); // miss

            ObjectName name = new ObjectName(JmxRegistrar.DOMAIN + ":type=CacheMetrics,block=rate-test");
            double rate = (Double) MBS.getAttribute(name, "HitRatePercent");
            assertEquals(50.0, rate, 1e-9);
        }
    }

    @Test
    void separateMBeanRegisteredForEachBlock() throws Exception {
        try (CacheBlock<String, byte[]> b1 = newBlock("alpha");
             CacheBlock<String, byte[]> b2 = newBlock("beta")) {

            registrar.registerCacheMetrics("alpha", new CacheMetricsMBean(b1));
            registrar.registerCacheMetrics("beta",  new CacheMetricsMBean(b2));

            ObjectName n1 = new ObjectName(JmxRegistrar.DOMAIN + ":type=CacheMetrics,block=alpha");
            ObjectName n2 = new ObjectName(JmxRegistrar.DOMAIN + ":type=CacheMetrics,block=beta");
            assertTrue(MBS.isRegistered(n1), "alpha MBean must be registered");
            assertTrue(MBS.isRegistered(n2), "beta MBean must be registered");
            assertNotEquals(n1, n2);
        }
    }

    @Test
    void replicationMetricsMBeanReadableViaJmx() throws Exception {
        ReplicationMetricsMBean bean = new ReplicationMetricsMBean(() -> 7L);
        bean.recordDroppedFrame();
        bean.recordReplicationLag(25L);

        registrar.registerReplicationMetrics(bean);

        ObjectName name = new ObjectName(JmxRegistrar.DOMAIN + ":type=ReplicationMetrics");
        assertTrue(MBS.isRegistered(name));

        assertEquals(1L,  (Long) MBS.getAttribute(name, "DroppedFrameCount"));
        assertEquals(25L, (Long) MBS.getAttribute(name, "AvgReplicationLagMs"));
        assertEquals(7L,  (Long) MBS.getAttribute(name, "PendingFrameCount"));
    }

    @Test
    void evictionMetricsMBeanReadableViaJmx() throws Exception {
        ConcurrentHashMap<String, CacheBlock<?, ?>> blocks = new ConcurrentHashMap<>();
        EvictionMetricsMBean bean = new EvictionMetricsMBean(blocks);

        registrar.registerEvictionMetrics(bean);

        ObjectName name = new ObjectName(JmxRegistrar.DOMAIN + ":type=EvictionMetrics");
        assertTrue(MBS.isRegistered(name));
        assertEquals(0L, (Long) MBS.getAttribute(name, "EvictedEntryCount"));
        assertEquals(0L, (Long) MBS.getAttribute(name, "ExpiredEntryCount"));
    }

    @Test
    void deregisterAllLeavesNoLeakedMBeans() throws Exception {
        try (CacheBlock<String, byte[]> block = newBlock("leak-test")) {
            registrar.registerCacheMetrics("leak-test", new CacheMetricsMBean(block));
            registrar.registerReplicationMetrics(new ReplicationMetricsMBean(() -> 0L));
            registrar.registerEvictionMetrics(
                    new EvictionMetricsMBean(new ConcurrentHashMap<>()));

            registrar.deregisterAll();

            Set<ObjectName> remaining = MBS.queryNames(
                    new ObjectName(JmxRegistrar.DOMAIN + ":*"), null);
            assertTrue(remaining.isEmpty(),
                    "No MBeans must remain under domain after deregisterAll: " + remaining);
        }
    }

    @Test
    void clusterMetricsMBeanReadableViaJmx() throws Exception {
        // GossipNode.build() allocates internal state but does NOT bind a socket;
        // start() binds the socket. A non-started node is safe for this JMX attribute test.
        GossipNode node = GossipNode.builder().build();
        ClusterMetricsMBean bean = new ClusterMetricsMBean(node);
        bean.setBootstrapSyncDurationMs(250L);
        bean.onDead(null); // ClusterMetricsMBean.onDead only calls deadCount.increment() — null NodeInfo is safe

        registrar.registerClusterMetrics(bean);

        ObjectName name = new ObjectName(JmxRegistrar.DOMAIN + ":type=ClusterMetrics");
        assertTrue(MBS.isRegistered(name));
        assertEquals(0,    (Integer) MBS.getAttribute(name, "AliveNodeCount"));
        assertEquals(0,    (Integer) MBS.getAttribute(name, "SuspectedNodeCount"));
        assertEquals(1L,   (Long)    MBS.getAttribute(name, "DeadNodeCount"));
        assertEquals(0L,   (Long)    MBS.getAttribute(name, "GossipTickCount"));
        assertEquals(250L, (Long)    MBS.getAttribute(name, "BootstrapSyncDurationMs"));
    }

    @Test
    void registerSameTypeTwiceDoesNotDuplicateTracking() throws Exception {
        // Calling registerReplicationMetrics() twice replaces the MBean in JMX but must
        // not add the same ObjectName twice to the internal tracking set. If it did,
        // deregisterAll() would attempt to unregister the same name twice; with the Set-based
        // tracking the second attempt is silently skipped because the Set dedups the name.
        ReplicationMetricsMBean bean1 = new ReplicationMetricsMBean(() -> 1L);
        ReplicationMetricsMBean bean2 = new ReplicationMetricsMBean(() -> 2L);

        registrar.registerReplicationMetrics(bean1);
        registrar.registerReplicationMetrics(bean2); // replaces bean1 in JMX

        ObjectName name = new ObjectName(JmxRegistrar.DOMAIN + ":type=ReplicationMetrics");
        assertEquals(2L, (Long) MBS.getAttribute(name, "PendingFrameCount"),
                "bean2 must be the active MBean after second registration");

        // deregisterAll() must clean up exactly once — no exception from double-deregistration
        assertDoesNotThrow(() -> registrar.deregisterAll());
        assertFalse(MBS.isRegistered(name), "MBean must be deregistered after deregisterAll()");
    }

    @Test
    void deregisterCacheMetricsRemovesMBeanAndTracking() throws Exception {
        try (CacheBlock<String, byte[]> block = newBlock("remove-me")) {
            registrar.registerCacheMetrics("remove-me", new CacheMetricsMBean(block));

            ObjectName name = new ObjectName(JmxRegistrar.DOMAIN + ":type=CacheMetrics,block=remove-me");
            assertTrue(MBS.isRegistered(name));

            registrar.deregisterCacheMetrics("remove-me");

            assertFalse(MBS.isRegistered(name), "MBean must be removed from JMX");

            // deregisterAll() must not attempt to re-deregister the already-removed name
            assertDoesNotThrow(() -> registrar.deregisterAll());
        }
    }

    @Test
    void blockNameWithSpecialCharactersIsJmxQuotedCorrectly() throws Exception {
        // Block names outside [a-zA-Z0-9_.-] must be passed through ObjectName.quote()
        // in quoteName() — this exercises that branch and verifies the resulting ObjectName
        // is valid and round-trips correctly through register → deregister.
        String specialName = "orders:v2 (shard*)";
        try (CacheBlock<String, byte[]> block = newBlock("ignored")) {
            CacheMetricsMBean bean = new CacheMetricsMBean(block);

            assertDoesNotThrow(() -> registrar.registerCacheMetrics(specialName, bean),
                    "Special-char block name must be JMX-quoted rather than throwing");

            ObjectName quotedName = new ObjectName(
                    JmxRegistrar.DOMAIN + ":type=CacheMetrics,block=" + ObjectName.quote(specialName));
            assertTrue(MBS.isRegistered(quotedName),
                    "MBean must be findable via the quoted ObjectName");

            registrar.deregisterCacheMetrics(specialName);
            assertFalse(MBS.isRegistered(quotedName),
                    "Deregistration must use the same quoting as registration");
        }
    }

    @Test
    void mBeanAttributeSizeReflectsCurrentEntryCount() throws Exception {
        try (CacheBlock<String, byte[]> block = newBlock("size-test")) {
            CacheMetricsMBean bean = new CacheMetricsMBean(block);
            registrar.registerCacheMetrics("size-test", bean);

            ObjectName name = new ObjectName(JmxRegistrar.DOMAIN + ":type=CacheMetrics,block=size-test");
            assertEquals(0L, (Long) MBS.getAttribute(name, "Size"));

            block.put("k1", "v".getBytes());
            block.put("k2", "v".getBytes());
            assertEquals(2L, (Long) MBS.getAttribute(name, "Size"));
        }
    }
}
