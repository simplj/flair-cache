package com.simplj.flair.cache.it;

import com.simplj.flair.cache.FlairCache;
import com.simplj.flair.cache.FlairCluster;
import com.simplj.flair.cache.replication.ConsistencyMode;
import com.simplj.flair.cache.store.CacheBlock;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.simplj.flair.cache.it.ITSupport.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Scenarios 5, 6, 14 — Cluster topology changes.
 *
 * <ul>
 *   <li>Scenario 5: Node join — 2-node cluster running, node 3 joins and bootstrap-syncs all state.</li>
 *   <li>Scenario 6: Node failure — 3-node cluster, one node killed, remaining two still replicate.</li>
 *   <li>Scenario 14: Graceful shutdown — departed node is marked DEAD, remaining nodes operate.</li>
 * </ul>
 *
 * <p>All tests use EVENTUAL consistency so put() never blocks on ACKs from unavailable nodes.</p>
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS)
class ClusterTopologyIT {

    // ── Scenario 5: Node join ─────────────────────────────────────────────────

    @Test
    void nodeJoin_bootstrapSyncTransfersFullState() throws Exception {
        int[] ports = freePorts(3);
        int port0 = ports[0], port1 = ports[1], port2 = ports[2];

        // Start a 2-node cluster and populate it with data.
        FlairCache node0 = startNode(port0, List.of(), ConsistencyMode.EVENTUAL);
        FlairCache node1 = startNode(port1, List.of("127.0.0.1:" + port0), ConsistencyMode.EVENTUAL);

        CacheBlock<String, String> b0 = registerBlock(node0, "data");
        CacheBlock<String, String> b1 = registerBlock(node1, "data");

        for (int i = 0; i < 50; i++) {
            b0.put("key" + i, "val" + i);
        }
        // Wait for node0 → node1 replication to settle.
        awaitCondition(Duration.ofSeconds(5), 50, () -> b1.get("key49") != null);

        // Start the third node with node0 as seed peer.
        FlairCache node2 = startNode(port2, List.of("127.0.0.1:" + port0), ConsistencyMode.EVENTUAL);
        CacheBlock<String, String> b2 = registerBlock(node2, "data");

        try {
            // bootstrapSync() performs a point-in-time state transfer from node0.
            node2.bootstrapSync("data");

            // All 50 entries must now be present on node2.
            for (int i = 0; i < 50; i++) {
                assertEquals("val" + i, b2.get("key" + i),
                        "bootstrap sync must transfer key" + i + " to node2");
            }

            // Live replication still works after bootstrap: write on node0, read on node2.
            b0.put("live-key", "live-val");
            awaitCondition(Duration.ofSeconds(3), 20, () -> b2.get("live-key") != null);
            assertEquals("live-val", b2.get("live-key"),
                    "live replication must work after bootstrap sync completes");
        } finally {
            node0.shutdown();
            node1.shutdown();
            node2.shutdown();
        }
    }

    // ── Scenario 6: Node failure ──────────────────────────────────────────────

    @Test
    void nodeFailure_remainingNodesStillReplicateToEachOther() throws IOException {
        int[] ports = freePorts(3);

        FlairCache node0 = startNode(ports[0], List.of(), ConsistencyMode.EVENTUAL);
        FlairCache node1 = startNode(ports[1], List.of("127.0.0.1:" + ports[0]), ConsistencyMode.EVENTUAL);
        FlairCache node2 = startNode(ports[2], List.of("127.0.0.1:" + ports[0]), ConsistencyMode.EVENTUAL);

        CacheBlock<String, String> b0 = registerBlock(node0, "failover");
        CacheBlock<String, String> b1 = registerBlock(node1, "failover");
        CacheBlock<String, String> b2 = registerBlock(node2, "failover");

        // Warm up — verify 3-node replication works.
        b0.put("initial", "data");
        awaitCondition(Duration.ofSeconds(3), 20, () -> b1.get("initial") != null && b2.get("initial") != null);

        try {
            // Shut down node1 (simulates failure — remaining nodes must continue operating).
            node1.shutdown();

            // Writes on node0 must still replicate to node2.
            b0.put("after-failure", "ok");
            boolean replicated = awaitCondition(Duration.ofSeconds(5), 50,
                    () -> b2.get("after-failure") != null);
            assertTrue(replicated, "write on node0 must replicate to node2 after node1 is down");
            assertEquals("ok", b2.get("after-failure"));
        } finally {
            node0.shutdown();
            node2.shutdown();
        }
    }

    // ── Scenario 14: Graceful shutdown ────────────────────────────────────────

    @Test
    void gracefulShutdown_remainingNodesContinueOperating() throws IOException {
        int[] ports = freePorts(3);

        FlairCache node0 = startNode(ports[0], List.of(), ConsistencyMode.EVENTUAL);
        FlairCache node1 = startNode(ports[1], List.of("127.0.0.1:" + ports[0]), ConsistencyMode.EVENTUAL);
        FlairCache node2 = startNode(ports[2], List.of("127.0.0.1:" + ports[0]), ConsistencyMode.EVENTUAL);

        CacheBlock<String, String> b0 = registerBlock(node0, "graceful");
        CacheBlock<String, String> b1 = registerBlock(node1, "graceful");
        CacheBlock<String, String> b2 = registerBlock(node2, "graceful");

        // Warm up the cluster.
        b0.put("pre", "data");
        awaitCondition(Duration.ofSeconds(3), 20, () -> b1.get("pre") != null && b2.get("pre") != null);

        try {
            // Graceful shutdown of node2: broadcasts LEAVE, drains pending frames.
            node2.shutdown();
            assertFalse(node2.isRunning(), "shutdown node must not be running");

            // Nodes 0 and 1 continue to replicate to each other.
            b0.put("post-shutdown", "still-works");
            boolean replicated = awaitCondition(Duration.ofSeconds(5), 50,
                    () -> b1.get("post-shutdown") != null);
            assertTrue(replicated, "node0 → node1 replication must survive node2 shutdown");
            assertEquals("still-works", b1.get("post-shutdown"));

            // node2 must be absent from node0's alive list within the SWIM suspicion window.
            // A graceful LEAVE is broadcast synchronously on shutdown(), so peers should
            // mark node2 as departed quickly.  We poll for up to 15 s to accommodate the
            // full SWIM suspect→dead path on slow CI machines.
            boolean markedDead = awaitCondition(Duration.ofSeconds(15), 500,
                    () -> node0.cluster().alive().stream()
                            .noneMatch(n -> n.port() == ports[2]));
            assertTrue(markedDead, "node2 must be removed from node0's alive membership after graceful shutdown");
        } finally {
            node0.shutdown();
            node1.shutdown();
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static FlairCache startNode(int port, List<String> seeds, ConsistencyMode mode) throws IOException {
        return FlairCache.builder()
                .bindAddress("127.0.0.1")
                .bindPort(port)
                .seedPeers(seeds)
                .consistency(mode)
                .ackTimeoutMs(2000)
                .build()
                .start();
    }

    private static CacheBlock<String, String> registerBlock(FlairCache node, String name) {
        return node.<String, String>registerBlock(name)
                .keyCodec(STRING_CODEC)
                .valueCodec(STRING_CODEC)
                .build();
    }
}
