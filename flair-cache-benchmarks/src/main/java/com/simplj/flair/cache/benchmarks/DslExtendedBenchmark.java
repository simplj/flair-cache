package com.simplj.flair.cache.benchmarks;

import com.simplj.flair.cache.FlairCache;
import com.simplj.flair.cache.dsl.Order;
import com.simplj.flair.cache.dsl.QueryEngine;
import com.simplj.flair.cache.dsl.SummaryStatistics;
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
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Benchmarks DSL operations not covered by {@link DslBenchmark}: early-termination via
 * {@code limit()}, top-N sort via {@code orderBy().limit()}, parallel execution,
 * {@code findFirst()} short-circuit, and numeric {@code summarize()}.
 *
 * <p>All queries run against a snapshot taken once at setup. Data sets:</p>
 * <ul>
 *   <li>{@code data10k}: 10k entries, values {@code "order-{i}"} — for sort, findFirst, summarize</li>
 *   <li>{@code data100k}: 100k entries, values {@code "cat{i%10}"} — for limit + parallel scan</li>
 * </ul>
 */
@State(Scope.Benchmark)
@BenchmarkMode({Mode.SampleTime, Mode.Throughput})
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 3, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class DslExtendedBenchmark {

    private static final int TEN_K     = 10_000;
    private static final int HUNDRED_K = 100_000;

    private FlairCache  cache;
    private QueryEngine engine;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        cache = FlairCache.builder()
                .bindAddress("127.0.0.1")
                .bindPort(LocalStoreBenchmark.findFreePort())
                .consistency(ConsistencyMode.EVENTUAL)
                .build()
                .start();

        CacheBlock<String, String> data10k = register("data10k");
        for (int i = 0; i < TEN_K; i++) {
            data10k.put("k" + i, "order-" + i);
        }

        CacheBlock<String, String> data100k = register("data100k");
        for (int i = 0; i < HUNDRED_K; i++) {
            data100k.put("k" + i, "cat" + (i % 10));
        }

        engine = cache.query();
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        if (cache != null) {
            cache.shutdown();
        }
    }

    /**
     * {@code where().limit(10).fetch()} on 100k entries (10% match rate).
     * Stream short-circuits after the first 10 matches — scans ~100 entries on average
     * vs the full 100k scan in {@link DslBenchmark#whereOnHundredK()}.
     */
    @Benchmark
    public List<String> whereWithLimit() {
        return engine.from("data100k", String.class, v -> (String) v)
                     .where(s -> s.equals("cat5"))
                     .limit(10)
                     .fetch();
    }

    /**
     * {@code orderBy().limit(100).fetch()} on 10k entries.
     * Materialises all 10k entries, sorts, then returns the top 100.
     * Measures: full scan + O(n log n) sort + limit.
     */
    @Benchmark
    public List<String> orderByLimit() {
        return engine.from("data10k", String.class, v -> (String) v)
                     .orderBy(s -> s, Order.ASC)
                     .limit(100)
                     .fetch();
    }

    /**
     * {@code parallel().where().fetch()} on 100k entries via the dedicated DSL ForkJoinPool.
     * Compare against {@link DslBenchmark#whereOnHundredK()} to quantify parallel overhead vs gain.
     * The parallel path submits to the pool and blocks waiting for the result.
     */
    @Benchmark
    public List<String> parallelWhere() {
        return engine.from("data100k", String.class, v -> (String) v)
                     .parallel()
                     .where(s -> s.equals("cat5"))
                     .fetch();
    }

    /**
     * {@code where().findFirst()} on 10k entries (~11% match rate for {@code "order-5*"}).
     * Terminates after the first match — typically scans fewer than 10 entries.
     * Compare against {@code where().fetch()} to show short-circuit benefit.
     */
    @Benchmark
    public Optional<String> findFirst() {
        return engine.from("data10k", String.class, v -> (String) v)
                     .where(s -> s.startsWith("order-5"))
                     .findFirst();
    }

    /**
     * {@code summarize()} across 10k entries.
     * Computes min/max/avg/sum of {@code String::length} for all values.
     * Measures: full scan + mapToDouble + DoubleSummaryStatistics accumulation.
     */
    @Benchmark
    public SummaryStatistics summarize() {
        return engine.from("data10k", String.class, v -> (String) v)
                     .summarize(String::length);
    }

    private CacheBlock<String, String> register(String name) {
        return cache.<String, String>registerBlock(name)
                .keyCodec(BenchmarkCodecs.STRING)
                .valueCodec(BenchmarkCodecs.STRING)
                .build();
    }
}
