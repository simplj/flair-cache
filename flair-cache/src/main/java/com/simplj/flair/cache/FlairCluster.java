package com.simplj.flair.cache;

import com.simplj.flair.cache.replication.ConsistencyMode;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.locks.LockSupport;
import java.util.logging.Logger;

/**
 * Embedded multi-node cluster for integration testing. All nodes run in the same JVM
 * on loopback — no Docker, no external infrastructure required.
 *
 * <pre>{@code
 * FlairCluster cluster = FlairCluster.builder()
 *     .nodes(5).basePort(17890).consistency(ConsistencyMode.QUORUM)
 *     .build().start();
 *
 * CacheBlock<String, byte[]> b0 = cluster.node(0).<String, byte[]>block("x")
 *     .keyCodec(stringCodec).valueCodec(bytesCodec).build();
 * b0.put("k", data);
 *
 * cluster.awaitReplication(Duration.ofSeconds(2));
 * assertNotNull(cluster.node(1).block("x")...);
 *
 * cluster.shutdown();
 * }</pre>
 *
 * <p>Node 0 starts with no seed peers (it is the seed). Nodes 1..N-1 each use node 0
 * ({@code 127.0.0.1:basePort}) as their seed peer; gossip propagates full membership
 * to every node within a few hundred milliseconds.</p>
 */
public final class FlairCluster {

    private static final Logger log = Logger.getLogger(FlairCluster.class.getName());

    private final List<FlairCache> nodes;

    private FlairCluster(List<FlairCache> nodes) {
        this.nodes = Collections.unmodifiableList(nodes);
    }

    /**
     * Starts all nodes in order (node 0 first), then blocks until all nodes have discovered
     * each other via SWIM gossip and established TCP replication connections. Tests can write
     * immediately after this method returns.
     *
     * @return {@code this} for chaining: {@code FlairCluster.builder()...build().start()}
     * @throws IOException if any node cannot bind its port
     */
    public FlairCluster start() throws IOException {
        List<FlairCache> started = new ArrayList<>(nodes.size());
        try {
            for (FlairCache node : nodes) {
                node.start();
                started.add(node);
            }
        } catch (Exception e) {
            for (int i = started.size() - 1; i >= 0; i--) {
                try { started.get(i).shutdown(); }
                catch (Exception ex) {
                    log.warning("Error during cluster startup rollback: " + ex.getMessage());
                }
            }
            if (e instanceof IOException) throw (IOException) e;
            throw new RuntimeException("FlairCluster start failed", e);
        }
        awaitClusterFormed();
        return this;
    }

    /**
     * Blocks until every node's SWIM membership list contains all {@code nodeCount} members
     * (including itself) as ALIVE, then waits a short grace period for TCP replication connections
     * to be fully established before returning. Times out after 15 seconds.
     */
    private void awaitClusterFormed() {
        if (nodes.size() <= 1) return;
        long deadlineMs = System.currentTimeMillis() + 15_000L;
        int expected = nodes.size(); // each node's membership includes itself
        while (System.currentTimeMillis() < deadlineMs) {
            boolean allFormed = true;
            for (FlairCache node : nodes) {
                if (node.cluster().alive().size() < expected) {
                    allFormed = false;
                    break;
                }
            }
            if (allFormed) {
                // Give TCP replication connections a short window to become established
                // after gossip has propagated membership.
                LockSupport.parkNanos(200_000_000L); // 200ms
                return;
            }
            LockSupport.parkNanos(20_000_000L); // 20ms
        }
        // Log actual state for diagnosis but do not throw — partial cluster may still work.
        StringBuilder sb = new StringBuilder("Cluster did not fully form within 15s. Alive per node:");
        for (int i = 0; i < nodes.size(); i++) {
            sb.append(" node").append(i).append("=").append(nodes.get(i).cluster().alive().size());
        }
        log.warning(sb.toString());
    }

    /**
     * Returns the {@link FlairCache} instance at the given index.
     *
     * @throws IndexOutOfBoundsException if {@code index} is out of range
     */
    public FlairCache node(int index) {
        if (index < 0 || index >= nodes.size()) {
            throw new IndexOutOfBoundsException(
                    "Node index " + index + " out of range [0, " + nodes.size() + ")");
        }
        return nodes.get(index);
    }

    /** Returns the number of nodes in this cluster. */
    public int size() {
        return nodes.size();
    }

    /**
     * Polls all nodes until replication has fully settled, or until {@code timeout} elapses.
     *
     * <p>Four conditions are checked on every poll:
     * <ol>
     *   <li>{@code pendingFrameCount == 0} — no frames waiting in any fanout queue.</li>
     *   <li>{@code pendingWriteCount == 0} — no frames sitting in a per-peer write queue;
     *       all distributed frames have been handed to the OS TCP send buffer. Without this,
     *       {@code awaitReplication()} could return while frames were still in the 2ms writer
     *       batch window and had not yet left the JVM.</li>
     *   <li>{@code pendingAckCount == 0} — no QUORUM/STRONG frames awaiting peer ACKs.</li>
     *   <li>{@code sum(totalFramesApplied) >= sum(totalFramesDistributed)} — all frames that
     *       were flushed to the OS have also been received and applied by the remote
     *       {@code IncomingHandler} on each receiving node. This closes the gap between "frame
     *       in OS buffer" and "frame applied to the remote store" without using a fixed delay.</li>
     * </ol>
     *
     * @throws IllegalStateException if replication has not fully settled before the timeout
     */
    public void awaitReplication(Duration timeout) {
        Objects.requireNonNull(timeout, "timeout must not be null");
        long deadlineMs = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadlineMs) {
            // Phase 1: check that no frames are in-flight in any queue on any node.
            boolean allQuiet = true;
            for (FlairCache node : nodes) {
                var rm = node.metrics().replicationMetrics();
                if (rm != null && (rm.getPendingFrameCount() > 0
                        || rm.getPendingAckCount() > 0
                        || rm.getPendingWriteCount() > 0)) {
                    allQuiet = false;
                    break;
                }
            }
            // Phase 2: if all queues are empty, verify that all distributed frames have been
            // applied by the remote workers. This closes the "frame in OS buffer but not yet
            // applied" gap without a fixed sleep.
            if (allQuiet) {
                long totalDist = 0, totalAppl = 0;
                for (FlairCache node : nodes) {
                    var rm = node.metrics().replicationMetrics();
                    if (rm != null) {
                        totalDist += rm.getTotalFramesDistributed();
                        totalAppl += rm.getTotalFramesApplied();
                    }
                }
                if (totalAppl >= totalDist) {
                    return;
                }
            }
            LockSupport.parkNanos(5_000_000L); // 5ms
        }
        throw new IllegalStateException(
                "Replication did not drain within " + timeout.toMillis() + "ms");
    }

    /**
     * Shuts down all nodes in reverse order (highest index first) so that the seed node
     * (node 0) departs last, giving peers time to broadcast LEAVE.
     * Idempotent — safe to call even if some nodes were never started.
     */
    public void shutdown() {
        for (int i = nodes.size() - 1; i >= 0; i--) {
            try {
                nodes.get(i).shutdown();
            } catch (Exception e) {
                log.warning("Error shutting down cluster node " + i + ": " + e.getMessage());
            }
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {

        private int             nodeCount   = 3;
        private int             basePort    = 17890;
        private ConsistencyMode consistency = ConsistencyMode.QUORUM;

        public Builder nodes(int n) {
            if (n < 1) throw new IllegalArgumentException("nodes must be >= 1");
            this.nodeCount = n;
            return this;
        }

        /**
         * Base TCP/UDP port. Nodes occupy ports {@code basePort} through
         * {@code basePort + nodes - 1}. Default: {@code 17890}.
         *
         * <p>The effective upper bound depends on the node count:
         * {@code basePort ≤ 65535 − nodes + 1}. This is validated at {@link #build()} time
         * when the final {@code nodeCount} is known.</p>
         */
        public Builder basePort(int port) {
            if (port < 1 || port > 65535) {
                throw new IllegalArgumentException("basePort must be in [1, 65535]");
            }
            this.basePort = port;
            return this;
        }

        /** Default consistency mode for all nodes. Default: {@link ConsistencyMode#QUORUM}. */
        public Builder consistency(ConsistencyMode consistency) {
            this.consistency = Objects.requireNonNull(consistency, "consistency must not be null");
            return this;
        }

        /**
         * Creates all {@link FlairCache} instances (not yet started).
         * Call {@link FlairCluster#start()} to bind ports and join the cluster.
         */
        public FlairCluster build() {
            if (basePort + nodeCount - 1 > 65535) {
                throw new IllegalArgumentException(
                        "basePort " + basePort + " + nodes " + nodeCount
                        + " exceeds max port 65535");
            }
            List<FlairCache> nodes = new ArrayList<>(nodeCount);

            // Node 0: standalone seed — no seed peers.
            nodes.add(FlairCache.builder()
                    .bindAddress("127.0.0.1")
                    .bindPort(basePort)
                    .consistency(consistency)
                    .build());

            // Nodes 1..N-1: each uses node 0 as their seed peer.
            String seedPeer = "127.0.0.1:" + basePort;
            for (int i = 1; i < nodeCount; i++) {
                nodes.add(FlairCache.builder()
                        .bindAddress("127.0.0.1")
                        .bindPort(basePort + i)
                        .seedPeers(List.of(seedPeer))
                        .consistency(consistency)
                        .build());
            }
            return new FlairCluster(nodes);
        }
    }
}
