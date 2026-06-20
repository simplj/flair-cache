package com.simplj.flair.cache.it;

import com.simplj.flair.cache.FlairCache;
import com.simplj.flair.cache.FlairCluster;
import com.simplj.flair.cache.replication.ConsistencyMode;
import com.simplj.flair.cache.store.CacheBlock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.simplj.flair.cache.it.ITSupport.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Scenarios 2, 3, 4 — Replication consistency modes on a 3-node embedded cluster.
 *
 * <ul>
 *   <li>Scenario 2: EVENTUAL — write on A, reads visible on B and C within 500ms.</li>
 *   <li>Scenario 3: QUORUM  — put() blocks until 2 nodes ACK; immediately consistent on all nodes.</li>
 *   <li>Scenario 4: STRONG  — put() blocks until all 3 nodes ACK; immediately consistent on all nodes.</li>
 * </ul>
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS)
class ReplicationIT {

    private FlairCluster cluster;

    @AfterEach
    void tearDown() {
        if (cluster != null) {
            cluster.shutdown();
            cluster = null;
        }
    }

    // ── Scenario 2: EVENTUAL ──────────────────────────────────────────────────

    @Test
    void eventual_writeOnNodeA_replicatesToBAndCWithin500ms() throws IOException {
        int basePort = freePort();
        cluster = buildCluster(basePort, ConsistencyMode.EVENTUAL, 3);

        CacheBlock<String, String> blockA = registerBlock(cluster.node(0), "items");
        CacheBlock<String, String> blockB = registerBlock(cluster.node(1), "items");
        CacheBlock<String, String> blockC = registerBlock(cluster.node(2), "items");

        blockA.put("key", "value");

        // awaitReplication() now checks pendingFrameCount == 0 AND pendingWriteCount == 0,
        // so it only returns once the frame has been fully flushed to the TCP socket for all
        // peers — not merely when it left the fanout queue. For loopback, delivery to the
        // remote selector is essentially instantaneous after that point, so assertions can
        // follow directly without a separate polling loop.
        cluster.awaitReplication(Duration.ofSeconds(2));

        assertEquals("value", blockB.get("key"), "node B must see the replicated value");
        assertEquals("value", blockC.get("key"), "node C must see the replicated value");
    }

    @Test
    void eventual_multipleWrites_allReplicateToAllNodes() throws IOException {
        int basePort = freePort();
        cluster = buildCluster(basePort, ConsistencyMode.EVENTUAL, 3);

        CacheBlock<String, String> blockA = registerBlock(cluster.node(0), "multi");
        CacheBlock<String, String> blockB = registerBlock(cluster.node(1), "multi");
        CacheBlock<String, String> blockC = registerBlock(cluster.node(2), "multi");

        for (int i = 0; i < 20; i++) {
            blockA.put("k" + i, "v" + i);
        }

        cluster.awaitReplication(Duration.ofSeconds(2));

        for (int i = 0; i < 20; i++) {
            assertEquals("v" + i, blockB.get("k" + i), "node B missing key k" + i);
            assertEquals("v" + i, blockC.get("k" + i), "node C missing key k" + i);
        }
    }

    // ── Scenario 3: QUORUM ────────────────────────────────────────────────────

    @Test
    void quorum_putBlocksUntilQuorumAck_thenImmediatelyVisibleOnAllNodes() throws IOException {
        int basePort = freePort();
        cluster = buildCluster(basePort, ConsistencyMode.QUORUM, 3);

        CacheBlock<String, String> blockA = registerBlock(cluster.node(0), "quorum");
        CacheBlock<String, String> blockB = registerBlock(cluster.node(1), "quorum");
        CacheBlock<String, String> blockC = registerBlock(cluster.node(2), "quorum");

        // put() on a QUORUM block blocks until N/2+1 = 2 nodes have ACKed.
        blockA.put("key", "quorum-value");

        // After put() returns, at least 2 nodes have the data.
        // The third may still be in-flight, but awaitReplication() ensures full convergence.
        cluster.awaitReplication(Duration.ofSeconds(2));

        assertEquals("quorum-value", blockA.get("key"), "writer node must have the value");
        assertEquals("quorum-value", blockB.get("key"), "node B must have the value after QUORUM ACK");
        assertEquals("quorum-value", blockC.get("key"), "node C must have the value after full replication");
    }

    @Test
    void quorum_deleteReplicatesToAllNodes() throws IOException {
        int basePort = freePort();
        cluster = buildCluster(basePort, ConsistencyMode.QUORUM, 3);

        CacheBlock<String, String> blockA = registerBlock(cluster.node(0), "qdel");
        CacheBlock<String, String> blockB = registerBlock(cluster.node(1), "qdel");
        CacheBlock<String, String> blockC = registerBlock(cluster.node(2), "qdel");

        blockA.put("k", "v");
        cluster.awaitReplication(Duration.ofSeconds(2));

        blockA.delete("k");
        cluster.awaitReplication(Duration.ofSeconds(2));

        assertNull(blockA.get("k"), "writer must not see deleted key");
        assertNull(blockB.get("k"), "node B must not see deleted key");
        assertNull(blockC.get("k"), "node C must not see deleted key");
    }

    // ── Scenario 4: STRONG ────────────────────────────────────────────────────

    @Test
    void strong_putBlocksUntilAllNodesAck_thenImmediatelyVisibleEverywhere() throws IOException {
        int basePort = freePort();
        cluster = buildCluster(basePort, ConsistencyMode.STRONG, 3);

        CacheBlock<String, String> blockA = registerBlock(cluster.node(0), "strong");
        CacheBlock<String, String> blockB = registerBlock(cluster.node(1), "strong");
        CacheBlock<String, String> blockC = registerBlock(cluster.node(2), "strong");

        // put() blocks until ALL 3 nodes ACK (STRONG mode).
        // After this line returns, all nodes have the data — no awaitReplication() needed.
        blockA.put("key", "strong-value");

        assertEquals("strong-value", blockA.get("key"), "writer must have the value");
        assertEquals("strong-value", blockB.get("key"), "node B must have the value after STRONG ACK");
        assertEquals("strong-value", blockC.get("key"), "node C must have the value after STRONG ACK");
    }

    @Test
    void strong_multipleKeys_allPresentOnAllNodesImmediately() throws IOException {
        int basePort = freePort();
        cluster = buildCluster(basePort, ConsistencyMode.STRONG, 3);

        CacheBlock<String, String> blockA = registerBlock(cluster.node(0), "smulti");
        CacheBlock<String, String> blockB = registerBlock(cluster.node(1), "smulti");
        CacheBlock<String, String> blockC = registerBlock(cluster.node(2), "smulti");

        for (int i = 0; i < 10; i++) {
            blockA.put("k" + i, "v" + i);
        }

        // All puts blocked until all nodes ACKed; no additional wait needed.
        for (int i = 0; i < 10; i++) {
            assertEquals("v" + i, blockB.get("k" + i), "node B missing k" + i + " after STRONG put");
            assertEquals("v" + i, blockC.get("k" + i), "node C missing k" + i + " after STRONG put");
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static FlairCluster buildCluster(int basePort, ConsistencyMode mode, int nodes) throws IOException {
        return FlairCluster.builder()
                .basePort(basePort)
                .nodes(nodes)
                .consistency(mode)
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
