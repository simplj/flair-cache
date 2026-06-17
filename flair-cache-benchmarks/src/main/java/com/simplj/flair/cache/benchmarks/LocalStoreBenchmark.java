package com.simplj.flair.cache.benchmarks;

import com.simplj.flair.cache.FlairCache;
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
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.TimeUnit;

/**
 * Benchmarks single-node local store operations: get hit, get miss, put with EVENTUAL
 * consistency, and delete. All operations use the FlairCache facade on a standalone node.
 *
 * <p>The node is pre-populated with {@value #PRELOAD_COUNT} entries before measurement begins.
 * The delete benchmark cycles through all pre-loaded keys; once a key is deleted it becomes
 * a no-op for subsequent iterations, which still exercises the {@code ConcurrentHashMap.remove()}
 * hot path.</p>
 *
 * <p>Benchmark targets (from CLAUDE.md):</p>
 * <ul>
 *   <li>{@code get()} local &lt; 200ns p99</li>
 *   <li>{@code put()} + async enqueue &lt; 500ns p99</li>
 * </ul>
 */
@State(Scope.Benchmark)
@BenchmarkMode({Mode.SampleTime, Mode.Throughput})
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class LocalStoreBenchmark {

    static final int PRELOAD_COUNT = 10_000;

    private FlairCache           cache;
    private CacheBlock<String, String> block;

    @State(Scope.Thread)
    public static class PutCounter {
        int i = 0;
    }

    @State(Scope.Thread)
    public static class DeleteCounter {
        int i = 0;
    }

    @Setup(Level.Trial)
    public void setup() throws IOException {
        cache = FlairCache.builder()
                .bindAddress("127.0.0.1")
                .bindPort(findFreePort())
                .consistency(ConsistencyMode.EVENTUAL)
                .build()
                .start();

        block = cache.<String, String>registerBlock("local-bench")
                .keyCodec(BenchmarkCodecs.STRING)
                .valueCodec(BenchmarkCodecs.STRING)
                .build();

        for (int i = 0; i < PRELOAD_COUNT; i++) {
            block.put("key-" + i, "val-" + i);
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        if (cache != null) {
            cache.shutdown();
        }
    }

    /** Measures a cache hit — the key always exists after pre-population. */
    @Benchmark
    public String getHit(Blackhole bh) {
        return block.get("key-5000");
    }

    /** Measures a cache miss — the key is never inserted. */
    @Benchmark
    public String getMiss(Blackhole bh) {
        return block.get("key-missing");
    }

    /**
     * Measures put() with EVENTUAL consistency.
     * Cycles through a per-thread counter to avoid always writing the same key,
     * but keeps the key set small enough to stay in the hot ConcurrentHashMap path.
     */
    @Benchmark
    public void putEventual(PutCounter counter) {
        int idx = counter.i & (PRELOAD_COUNT - 1);
        block.put("key-" + idx, "bench-val");
        counter.i++;
    }

    /**
     * Measures delete() cycling through pre-populated keys.
     * After all keys are deleted the operations become no-ops; both paths exercise
     * the same {@code ConcurrentHashMap.remove()} O(1) operation.
     */
    @Benchmark
    public void delete(DeleteCounter counter) {
        int idx = counter.i & (PRELOAD_COUNT - 1);
        block.delete("key-" + idx);
        counter.i++;
    }

    static int findFreePort() {
        try (ServerSocket ss = new ServerSocket(0)) {
            ss.setReuseAddress(true);
            return ss.getLocalPort();
        } catch (IOException e) {
            throw new RuntimeException("Cannot find a free port", e);
        }
    }
}
