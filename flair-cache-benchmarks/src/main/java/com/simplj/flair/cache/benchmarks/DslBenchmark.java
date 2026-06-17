package com.simplj.flair.cache.benchmarks;

import com.simplj.flair.cache.FlairCache;
import com.simplj.flair.cache.dsl.Aggregators;
import com.simplj.flair.cache.dsl.QueryEngine;
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
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Benchmarks the FlairCache DSL (QueryEngine) over realistic data volumes.
 *
 * <p>All queries execute against a snapshot taken once at setup time, so the measurement
 * isolates DSL execution overhead from snapshot-creation cost. The data does not change
 * during benchmarking.</p>
 *
 * <p>Data model: String values formatted {@code "cat{i%10}"} (10 evenly distributed
 * categories). For the join, both sides use {@code "order-{i}"} strings joined on
 * exact equality (O(n+m) hash join, 10k matched pairs).</p>
 *
 * <p>Benchmark targets (from CLAUDE.md):</p>
 * <ul>
 *   <li>{@code where().fetch()} on 10k entries &lt; 5ms p99</li>
 *   <li>{@code join()} on 10k×10k entries &lt; 50ms p99</li>
 * </ul>
 */
@State(Scope.Benchmark)
@BenchmarkMode({Mode.SampleTime, Mode.Throughput})
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 3, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class DslBenchmark {

    private static final int TEN_K     = 10_000;
    private static final int HUNDRED_K = 100_000;

    private FlairCache   cache;
    private QueryEngine  engine;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        cache = FlairCache.builder()
                .bindAddress("127.0.0.1")
                .bindPort(LocalStoreBenchmark.findFreePort())
                .consistency(ConsistencyMode.EVENTUAL)
                .build()
                .start();

        CacheBlock<String, String> orders10k = register("orders10k");
        for (int i = 0; i < TEN_K; i++) {
            orders10k.put("k" + i, "order-" + i);
        }

        CacheBlock<String, String> items10k = register("items10k");
        for (int i = 0; i < TEN_K; i++) {
            items10k.put("k" + i, "order-" + i);
        }

        CacheBlock<String, String> orders100k = register("orders100k");
        for (int i = 0; i < HUNDRED_K; i++) {
            orders100k.put("k" + i, "cat" + (i % 10));
        }

        CacheBlock<String, String> gb100k = register("gb100k");
        for (int i = 0; i < HUNDRED_K; i++) {
            gb100k.put("k" + i, "cat" + (i % 10));
        }

        // Snapshot taken once; blocks are not mutated during measurement.
        engine = cache.query();
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        if (cache != null) {
            cache.shutdown();
        }
    }

    /**
     * {@code where().fetch()} scan over 10k entries — ~1k results (category "cat5").
     * Target: &lt; 5ms p99.
     */
    @Benchmark
    public List<String> whereOnTenK() {
        return engine.from("orders10k", String.class, v -> (String) v)
                     .where(s -> s.startsWith("order-5"))
                     .fetch();
    }

    /**
     * {@code where().fetch()} scan over 100k entries — ~10k results (category "cat5").
     */
    @Benchmark
    public List<String> whereOnHundredK() {
        return engine.from("orders100k", String.class, v -> (String) v)
                     .where(s -> s.equals("cat5"))
                     .fetch();
    }

    /**
     * Hash join of two 10k collections on exact value equality.
     * All 10k "order-{i}" values on the left side match exactly one on the right side,
     * producing 10k result pairs. Strategy: hash join O(n+m).
     * Target: &lt; 50ms p99.
     */
    @Benchmark
    public List<String> joinTenKByTenK() {
        return engine.from("orders10k", String.class, v -> (String) v)
                     .join("items10k", String.class, v -> (String) v)
                     .on(l -> l, r -> r)
                     .select((l, r) -> l)
                     .fetch();
    }

    /**
     * {@code groupBy().count()} over 100k entries.
     * 10 groups with 10k entries each. Returns {@code Map<String, Long>}.
     */
    @Benchmark
    public Map<String, Long> groupByCountHundredK() {
        return engine.from("gb100k", String.class, v -> (String) v)
                     .groupBy(s -> s)
                     .aggregate(Aggregators.count());
    }

    private CacheBlock<String, String> register(String name) {
        return cache.<String, String>registerBlock(name)
                .keyCodec(BenchmarkCodecs.STRING)
                .valueCodec(BenchmarkCodecs.STRING)
                .build();
    }
}
