package com.simplj.flair.cache.metrics;

import com.simplj.flair.cache.gossip.GossipNode;
import com.simplj.flair.cache.replication.ReplicationEngine;
import com.simplj.flair.cache.store.CacheBlock;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;
import java.util.logging.Logger;

/**
 * Central metrics registry. Registers all FlairCache JMX MBeans under
 * {@code com.simplj.flair.cache} and provides programmatic read access.
 *
 * <p>Usage (facade wires all components on startup):</p>
 * <pre>{@code
 * MetricsRegistry metrics = new MetricsRegistry();
 * metrics.withReplication(replicationEngine);
 * metrics.withCluster(gossipNode);       // call BEFORE gossipNode.start()
 * metrics.registerBlock("products", productsBlock);
 *
 * // Programmatic access
 * long hits = metrics.cacheHits("products");
 * long lag  = metrics.avgReplicationLagMs();
 * int  alive = metrics.aliveNodeCount();
 *
 * // On shutdown
 * metrics.shutdown();
 * }</pre>
 */
public final class MetricsRegistry {

    private static final Logger log = Logger.getLogger(MetricsRegistry.class.getName());

    private final ConcurrentHashMap<String, CacheBlock<?, ?>>   blocks     = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, CacheMetricsMBean>  blockBeans = new ConcurrentHashMap<>();
    private final JmxRegistrar         registrar;
    private final EvictionMetricsMBean evictionBean;

    // Dedicated lock for withReplication() and withCluster() — both are check-then-act
    // sequences that must be atomic to prevent two concurrent callers from both passing
    // the null check and attaching two listeners or creating two beans.
    // Reads (replicationMetrics(), clusterMetrics(), etc.) do NOT take this lock — they
    // read the volatile fields directly for zero-overhead access on the read path.
    private final Object configLock = new Object();

    private volatile ReplicationMetricsMBean replicationBean;
    private volatile ClusterMetricsMBean     clusterBean;

    public MetricsRegistry() {
        this.registrar    = new JmxRegistrar();
        this.evictionBean = new EvictionMetricsMBean(blocks);
        registrar.registerEvictionMetrics(evictionBean);
    }

    /**
     * Registers a {@link CacheBlock} and its per-block JMX MBean.
     * Idempotent: the first registration for a given {@code blockName} wins.
     * Subsequent calls for the same name return the existing bean without creating a second
     * listener or updating the backing block reference. {@link CacheBlock} has no
     * listener-removal API, so creating a second bean would leave a permanently dangling
     * PutListener/DeleteListener; updating the block reference would diverge the
     * per-block MBean stats from the eviction aggregate.
     */
    public CacheMetricsMBean registerBlock(String blockName, CacheBlock<?, ?> block) {
        final boolean[] created = {false};
        CacheMetricsMBean bean = blockBeans.computeIfAbsent(blockName, n -> {
            created[0] = true;
            return new CacheMetricsMBean(block);
        });
        if (created[0]) {
            // Only update blocks on first registration — keeps blocks ↔ blockBeans consistent.
            // A re-registration with a different block instance must not silently switch the
            // EvictionMetricsMBean to a different block while the per-block CacheMetricsMBean
            // still reads stats from the original block.
            blocks.put(blockName, block);
            registrar.registerCacheMetrics(blockName, bean);
        }
        return bean;
    }

    /**
     * Deregisters the named {@link CacheBlock} from JMX and from the eviction aggregate.
     * Safe to call on a block that was never registered (no-op).
     *
     * <p>Note: {@link CacheBlock} has no listener-removal API, so any {@code PutListener} /
     * {@code DeleteListener} attached by the retired {@code CacheMetricsMBean} will remain on
     * the block until it is closed. Those listeners are harmless (they increment counters in
     * the orphaned bean that nobody reads) but they cannot be removed.
     */
    public void deregisterBlock(String blockName) {
        blocks.remove(blockName);
        blockBeans.remove(blockName);
        registrar.deregisterCacheMetrics(blockName);
    }

    /**
     * Wires replication metrics. Call once after {@link ReplicationEngine#start()}.
     * Returns the bean so callers can invoke {@code recordDroppedFrame()} etc.
     *
     * <p>Idempotent: a second call returns the existing bean without creating a new one.
     * This preserves accumulated lag/drop history and prevents the first caller's bean
     * reference from becoming stale.
     */
    public ReplicationMetricsMBean withReplication(ReplicationEngine engine) {
        return withReplication(engine::pendingFrameCount);
    }

    // Package-private: for testing and internal use where a full ReplicationEngine is not available
    ReplicationMetricsMBean withReplication(LongSupplier pendingSupplier) {
        synchronized (configLock) {
            if (replicationBean != null) {
                log.warning("MetricsRegistry.withReplication() called more than once — " +
                        "returning existing ReplicationMetricsMBean to preserve accumulated lag/drop history.");
                return replicationBean;
            }
            replicationBean = new ReplicationMetricsMBean(pendingSupplier);
            registrar.registerReplicationMetrics(replicationBean);
            return replicationBean;
        }
    }

    /**
     * Wires cluster metrics and registers this registry as a
     * {@link com.simplj.flair.cache.gossip.MembershipListener}.
     *
     * <p><strong>Call this between {@link GossipNode#builder() GossipNode.build()} and
     * {@link GossipNode#start()}.</strong> Dead-node events that occur between {@code start()}
     * and this call are missed, causing {@code DeadNodeCount} to permanently under-count.
     * Calling this before {@link GossipNode#builder() build()} is not possible; calling it
     * after {@link GossipNode#start()} is the only error case, and a {@code WARNING} is logged.
     *
     * <p>Idempotent: a second call returns the existing bean without adding a second
     * {@code MembershipListener} to the node (which would cause dead events to be counted twice).
     *
     * <p>Returns the bean so callers can invoke {@code setBootstrapSyncDurationMs()} etc.
     */
    public ClusterMetricsMBean withCluster(GossipNode gossipNode) {
        Objects.requireNonNull(gossipNode, "gossipNode must not be null");
        synchronized (configLock) {
            if (clusterBean != null) {
                log.warning("MetricsRegistry.withCluster() called more than once — " +
                        "returning existing ClusterMetricsMBean to prevent ghost MembershipListener accumulation.");
                return clusterBean;
            }
            if (gossipNode.isStarted()) {
                log.warning("MetricsRegistry.withCluster() called after GossipNode.start() — " +
                        "dead node events that occurred before registration are not reflected in DeadNodeCount. " +
                        "Call withCluster() before GossipNode.start() to avoid this.");
            }
            clusterBean = new ClusterMetricsMBean(gossipNode);
            gossipNode.addMembershipListener(clusterBean);
            registrar.registerClusterMetrics(clusterBean);
            return clusterBean;
        }
    }

    // ── Programmatic access ──────────────────────────────────────────────────

    public ReplicationMetricsMBean replicationMetrics() { return replicationBean; }
    public ClusterMetricsMBean     clusterMetrics()     { return clusterBean; }
    public EvictionMetricsMBean    evictionMetrics()    { return evictionBean; }

    /** Returns the per-block {@link CacheMetricsMBean}, or {@code null} if not registered. */
    public CacheMetricsMBean cacheMetrics(String blockName) {
        return blockBeans.get(blockName);
    }

    public long cacheHits(String blockName) {
        CacheMetricsMBean bean = blockBeans.get(blockName);
        return bean != null ? bean.getHitCount() : 0L;
    }

    public long avgReplicationLagMs() {
        ReplicationMetricsMBean bean = replicationBean;
        return bean != null ? bean.getAvgReplicationLagMs() : 0L;
    }

    public int aliveNodeCount() {
        ClusterMetricsMBean bean = clusterBean;
        return bean != null ? bean.getAliveNodeCount() : 0;
    }

    /**
     * Deregisters all JMX MBeans and clears internal block references.
     * After shutdown, programmatic accessors (cacheHits, evictionMetrics, etc.) return zero/null
     * and no longer hold strong references to registered {@link CacheBlock} instances.
     * Call on FlairCache shutdown.
     */
    public void shutdown() {
        blocks.clear();
        blockBeans.clear();
        registrar.deregisterAll();
    }
}
