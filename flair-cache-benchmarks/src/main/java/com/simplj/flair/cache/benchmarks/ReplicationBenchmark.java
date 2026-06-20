package com.simplj.flair.cache.benchmarks;

import com.simplj.flair.cache.FlairCache;
import com.simplj.flair.cache.FlairCluster;
import com.simplj.flair.cache.replication.ConsistencyMode;
import com.simplj.flair.cache.store.CacheBlock;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * Benchmarks put() with QUORUM consistency on a 3-node embedded cluster.
 *
 * <p><strong>V1 limitation:</strong> the FlairCache facade wires replication through
 * {@code attachBlock}, which catches {@code ReplicationTimeoutException} internally.
 * The QUORUM write therefore returns immediately after the local write + async enqueue,
 * not after N/2+1 ACKs. The {@link #putQuorumAwaitDrain} variant spin-waits until the
 * sender's pending-frame and pending-ACK counters both reach zero, capturing the full
 * end-to-end latency including loopback TCP round-trips and remote-node processing.</p>
 */
@State(Scope.Benchmark)
@BenchmarkMode({Mode.SampleTime, Mode.Throughput})
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 3, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class ReplicationBenchmark {

    private static final int CLUSTER_NODES = 3;
    // Generous timeout for ACKs during benchmark — avoids spurious QUORUM timeouts
    private static final long ACK_TIMEOUT_MS = 2_000L;
    private static final long AWAIT_DRAIN_DEADLINE_NS = 500_000_000L; // 500ms

    private FlairCluster cluster;
    private CacheBlock<String, String> block0; // block on the writer node

    @State(Scope.Thread)
    public static class PutCounter {
        int i = 0;
    }

    @Setup(Level.Trial)
    public void setup() throws IOException, InterruptedException {
        int basePort = findFreeBasePort(CLUSTER_NODES);

        cluster = FlairCluster.builder()
                .nodes(CLUSTER_NODES)
                .basePort(basePort)
                .consistency(ConsistencyMode.QUORUM)
                .build()
                .start();

        // Register the same block name on every node so the remote IncomingHandler can
        // resolve it when forwarding replicated frames.
        for (int i = 0; i < CLUSTER_NODES; i++) {
            FlairCache node = cluster.node(i);
            CacheBlock<String, String> b = node.<String, String>registerBlock("repl-bench")
                    .keyCodec(BenchmarkCodecs.STRING)
                    .valueCodec(BenchmarkCodecs.STRING)
                    .consistency(ConsistencyMode.QUORUM)
                    .build();
            if (i == 0) {
                block0 = b;
            }
        }

        // Allow gossip to converge and TCP replication connections to be established.
        awaitClusterReady(2_000);
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        if (cluster != null) {
            cluster.shutdown();
        }
    }

    /**
     * Measures put() with QUORUM mode on the write node. Due to the V1 facade limitation
     * (see class-level Javadoc), the call returns after local write + async replication enqueue —
     * not after N/2+1 ACKs. Use {@link #putQuorumAwaitDrain} for full round-trip measurement.
     */
    @Benchmark
    public void putQuorum(PutCounter counter) {
        block0.put("rk-" + (counter.i++ & 0xFFFF), "v");
    }

    /**
     * Measures the full QUORUM round-trip: put() + spin-wait until sender's pending-frame
     * and pending-ACK counters both reach zero. Captures loopback TCP RTT and remote
     * processing overhead.
     */
    @Benchmark
    public void putQuorumAwaitDrain(PutCounter counter) {
        block0.put("rk-" + (counter.i++ & 0xFFFF), "v");

        var rm = cluster.node(0).metrics().replicationMetrics();
        if (rm == null) return;

        long deadline = System.nanoTime() + AWAIT_DRAIN_DEADLINE_NS;
        while ((rm.getPendingFrameCount() > 0 || rm.getPendingAckCount() > 0)
                && System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private void awaitClusterReady(long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            boolean allReady = true;
            for (int i = 0; i < CLUSTER_NODES; i++) {
                try {
                    int alive = cluster.node(i).cluster().alive().size();
                    if (alive < CLUSTER_NODES) {
                        allReady = false;
                        break;
                    }
                } catch (Exception e) {
                    allReady = false;
                    break;
                }
            }
            if (allReady) return;
            LockSupport.parkNanos(50_000_000L); // 50ms
        }
        // Proceed even if not fully converged — warmup iterations handle the remaining delay.
    }

    /** Finds a base port such that [basePort, basePort + count) are all free. */
    private static int findFreeBasePort(int count) {
        for (int attempt = 0; attempt < 100; attempt++) {
            int candidate = 18000 + (int)(Math.random() * 10000);
            if (isRangeAvailable(candidate, count)) return candidate;
        }
        throw new RuntimeException("Cannot find " + count + " consecutive free ports");
    }

    private static boolean isRangeAvailable(int base, int count) {
        for (int i = 0; i < count; i++) {
            try (ServerSocket ss = new ServerSocket(base + i)) {
                ss.setReuseAddress(true);
            } catch (IOException e) {
                return false;
            }
        }
        return true;
    }
}
