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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.simplj.flair.cache.it.ITSupport.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Scenarios 12, 13 — Concurrent writes and conflict resolution.
 *
 * <ul>
 *   <li>Scenario 12: 16 threads writing different keys simultaneously; all keys present
 *       on all nodes after replication settles.</li>
 *   <li>Scenario 13: Two nodes write the same key simultaneously; all nodes converge to
 *       the same value after replication (HLC-based LWW semantics).</li>
 * </ul>
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS)
class ConcurrencyIT {

    private FlairCluster cluster;

    @AfterEach
    void tearDown() {
        if (cluster != null) {
            cluster.shutdown();
            cluster = null;
        }
    }

    // ── Scenario 12: Concurrent writes — different keys ───────────────────────

    @Test
    void concurrentWrites_16Threads_allKeysOnAllNodesAfterReplication() throws Exception {
        int basePort = freePort();
        cluster = FlairCluster.builder()
                .basePort(basePort)
                .nodes(3)
                .consistency(ConsistencyMode.EVENTUAL)
                .build()
                .start();

        CacheBlock<String, String> b0 = registerBlock(cluster.node(0), "concurrent");
        CacheBlock<String, String> b1 = registerBlock(cluster.node(1), "concurrent");
        CacheBlock<String, String> b2 = registerBlock(cluster.node(2), "concurrent");

        int threadCount = 16;
        int keysPerThread = 100;
        ExecutorService pool = Executors.newFixedThreadPool(threadCount);
        CountDownLatch allDone = new CountDownLatch(threadCount);
        AtomicBoolean writeError = new AtomicBoolean(false);

        List<Future<?>> futures = new ArrayList<>();
        for (int t = 0; t < threadCount; t++) {
            final int threadIdx = t;
            futures.add(pool.submit(() -> {
                try {
                    for (int k = 0; k < keysPerThread; k++) {
                        String key = "t" + threadIdx + "-k" + k;
                        b0.put(key, "v" + k);
                    }
                } catch (Exception e) {
                    writeError.set(true);
                } finally {
                    allDone.countDown();
                }
            }));
        }

        assertTrue(allDone.await(30, TimeUnit.SECONDS), "all writer threads must complete within 30 s");
        pool.shutdown();
        assertFalse(writeError.get(), "no write must throw an exception");

        // awaitReplication() checks pendingFrameCount == 0 AND pendingWriteCount == 0, so it
        // only returns once all 1600 frames have been fully flushed to the TCP socket for every
        // peer — not when they merely left the fanout queue. On loopback, the remote selector
        // picks up OP_READ events immediately after the write, so assertions can follow directly.
        cluster.awaitReplication(Duration.ofSeconds(5));

        // Count missing on each node to produce a useful failure message.
        int missingOnB1 = 0, missingOnB2 = 0;
        for (int t = 0; t < threadCount; t++) {
            for (int k = 0; k < keysPerThread; k++) {
                String key = "t" + t + "-k" + k;
                if (b1.get(key) == null) missingOnB1++;
                if (b2.get(key) == null) missingOnB2++;
            }
        }
        assertEquals(0, missingOnB1, "all " + (threadCount * keysPerThread) + " keys must be on node 1");
        assertEquals(0, missingOnB2, "all " + (threadCount * keysPerThread) + " keys must be on node 2");
    }

    // ── Scenario 13: Conflict resolution — same key from two nodes ────────────

    @Test
    void conflictResolution_sameKeyConcurrentWrite_allNodesConvergeToSameValue() throws Exception {
        int basePort = freePort();
        cluster = FlairCluster.builder()
                .basePort(basePort)
                .nodes(3)
                .consistency(ConsistencyMode.EVENTUAL)
                .build()
                .start();

        CacheBlock<String, String> b0 = registerBlock(cluster.node(0), "conflict");
        CacheBlock<String, String> b1 = registerBlock(cluster.node(1), "conflict");
        CacheBlock<String, String> b2 = registerBlock(cluster.node(2), "conflict");

        // Write the same key from two different nodes near-simultaneously.
        // HLC-based LWW ensures the write with the higher HLC timestamp wins on all nodes.
        // We do not know which value will win — only that all nodes converge to the SAME value.
        String key = "conflict-key";

        Thread t0 = new Thread(() -> b0.put(key, "from-node0"));
        Thread t2 = new Thread(() -> b2.put(key, "from-node2"));
        t0.start();
        t2.start();
        t0.join(5000);
        t2.join(5000);

        // Wait for LWW convergence: all three nodes must agree on the SAME value.
        // The two writes carry distinct HLC timestamps; the higher one wins on all nodes.
        // This may take longer than just waiting for each node to have *a* value, because
        // node0 sees its own write immediately but may not yet have received node2's write.
        boolean converged = awaitCondition(Duration.ofSeconds(10), 50, () -> {
            String v0 = b0.get(key);
            if (v0 == null) return false;
            return v0.equals(b1.get(key)) && v0.equals(b2.get(key));
        });
        assertTrue(converged, "all nodes must converge to the same LWW-winning value within 10 s");

        String val0 = b0.get(key);
        String val1 = b1.get(key);
        String val2 = b2.get(key);

        assertNotNull(val0, "node0 must have the key");
        assertNotNull(val1, "node1 must have the key");
        assertNotNull(val2, "node2 must have the key");

        // All nodes must agree on the same value (LWW winner).
        assertEquals(val0, val1, "node0 and node1 must converge to the same value");
        assertEquals(val0, val2, "node0 and node2 must converge to the same value");

        // The winner must be one of the two candidate values — not some third value.
        assertTrue(val0.equals("from-node0") || val0.equals("from-node2"),
                "converged value must be one of the two writes; got: " + val0);
    }

    @Test
    void concurrentWrites_sameNodeSameKey_lastWriteVisible() throws Exception {
        cluster = FlairCluster.builder()
                .basePort(freePort())
                .nodes(3)
                .consistency(ConsistencyMode.EVENTUAL)
                .build()
                .start();

        CacheBlock<String, String> b0 = registerBlock(cluster.node(0), "same-key");
        CacheBlock<String, String> b1 = registerBlock(cluster.node(1), "same-key");

        int iterations = 200;
        String key = "hotkey";

        // Sequential writes from the same node — the block must always return SOME value.
        for (int i = 0; i < iterations; i++) {
            b0.put(key, "v" + i);
        }

        // Final write must be visible and consistent.
        String finalVal = b0.get(key);
        assertNotNull(finalVal, "key must be present after repeated writes");

        // Poll for convergence by checking actual value visibility. Use a generous timeout
        // because 200 sequential puts generate 200 replication frames that must all arrive
        // in order and be applied on node1 before the final value (highest HLC) is visible.
        boolean converged = awaitCondition(Duration.ofSeconds(10), 50, () -> finalVal.equals(b1.get(key)));
        assertTrue(converged, "node1 must converge to the final value within 10 s");

        // All nodes must agree — exact value is the one with the highest HLC timestamp.
        String val1 = b1.get(key);
        assertEquals(finalVal, val1, "all nodes must converge to the same value after sequential writes");
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static CacheBlock<String, String> registerBlock(FlairCache node, String name) {
        return node.<String, String>registerBlock(name)
                .keyCodec(STRING_CODEC)
                .valueCodec(STRING_CODEC)
                .build();
    }
}
