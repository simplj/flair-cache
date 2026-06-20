package com.simplj.flair.cache.benchmarks;

import com.simplj.flair.cache.FlairCache;
import com.simplj.flair.cache.replication.ConsistencyMode;
import com.simplj.flair.cache.store.CacheBlock;
import com.simplj.flair.cache.watch.ChangeEvent;
import com.simplj.flair.cache.watch.WatchRegistry;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

/**
 * Benchmarks {@link WatchRegistry#dispatch(ChangeEvent)} with 5 active async subscribers.
 *
 * <p>Each of the 5 subscribers runs an async drain thread that increments a
 * {@link LongAdder}. The benchmark measures the dispatch hot path: 5 non-blocking
 * {@code offer()} calls to {@link java.util.concurrent.ArrayBlockingQueue}s (capacity
 * 1024 each) plus filter checks (null filter — all pass).</p>
 *
 * <p>Design note: {@code WatchRegistry.dispatch()} is documented to complete in &lt; 500ns
 * regardless of subscriber count. This benchmark verifies that contract under a
 * realistic 5-subscriber configuration.</p>
 */
@State(Scope.Benchmark)
@BenchmarkMode({Mode.SampleTime, Mode.Throughput})
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class WatchDispatchBenchmark {

    private static final int LISTENER_COUNT = 5;

    private FlairCache                    cache;
    private WatchRegistry<String, String> registry;
    private ChangeEvent<String, String>   event;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        cache = FlairCache.builder()
                .bindAddress("127.0.0.1")
                .bindPort(LocalStoreBenchmark.findFreePort())
                .consistency(ConsistencyMode.EVENTUAL)
                .build()
                .start();

        CacheBlock<String, String> block = cache.<String, String>registerBlock("watch-bench")
                .keyCodec(BenchmarkCodecs.STRING)
                .valueCodec(BenchmarkCodecs.STRING)
                .build();

        registry = cache.watchRegistry("watch-bench");

        LongAdder counter = new LongAdder();
        for (int i = 0; i < LISTENER_COUNT; i++) {
            registry.watch()
                    .onPut((k, v) -> counter.increment())
                    .async(true)
                    .register();
        }

        // Pre-create the event object to exclude allocation from the hot path.
        event = new ChangeEvent.PutEvent<>("bench-key", "bench-val", null, ChangeEvent.Source.LOCAL);
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        if (cache != null) {
            cache.shutdown();
        }
    }

    /**
     * Dispatches a PUT event to 5 async subscribers.
     * Measures: 5 key-filter checks (null → pass) + 5 non-blocking queue {@code offer()} calls.
     * Each drain thread consumes events concurrently; the queues (capacity 1024) absorb
     * bursts without blocking the dispatch caller.
     */
    @Benchmark
    public void dispatchToFiveListeners() {
        registry.dispatch(event);
    }
}
