package com.simplj.flair.cache.benchmarks;

import com.simplj.flair.cache.watch.ChangeEvent;
import com.simplj.flair.cache.watch.WatchRegistry;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

/**
 * Benchmarks {@link WatchRegistry#dispatch(ChangeEvent)} as async subscriber count scales.
 *
 * <p>{@code WatchRegistry} guarantees dispatch returns in O(1) regardless of subscriber count:
 * the calling thread enqueues one {@code DispatchWork} to a single {@code ConcurrentLinkedQueue}
 * inbox and unparks the dedicated {@code flaircache-watch-dispatch} fan-out thread.
 * All per-subscriber work ({@code N} key-filter checks + {@code N} {@code ABQ.offer()} calls)
 * runs asynchronously on that fan-out thread.
 * This benchmark verifies the O(1) claim at 1, 5, 10, and 50 subscribers.</p>
 *
 * <p>Unlike {@link WatchDispatchBenchmark} (fixed at 5 subscribers), this benchmark uses
 * {@code @Param} so JMH reports separate percentile distributions for each count.
 * Compare the p50 and p99 columns across param values — they should be flat, not O(n).</p>
 *
 * <p>Uses {@link WatchRegistry} directly — no {@link com.simplj.flair.cache.FlairCache}
 * required. Each async subscriber gets its own drain thread via {@link java.util.concurrent.ArrayBlockingQueue}.</p>
 */
@State(Scope.Benchmark)
@BenchmarkMode({Mode.SampleTime, Mode.Throughput})
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class WatchSubscriberScaleBenchmark {

    @Param({"1", "5", "10", "50"})
    public int subscriberCount;

    private WatchRegistry<String, String> registry;
    private ChangeEvent<String, String>   event;

    @Setup(Level.Trial)
    public void setup() {
        registry = new WatchRegistry<>();

        LongAdder counter = new LongAdder();
        for (int i = 0; i < subscriberCount; i++) {
            registry.watch()
                    .onPut((k, v) -> counter.increment())
                    .async(true)
                    .register();
        }

        event = new ChangeEvent.PutEvent<>("bench-key", "bench-val", null, ChangeEvent.Source.LOCAL);
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        if (registry != null) {
            registry.shutdown();
        }
    }

    /**
     * Dispatches one PUT event to {@code subscriberCount} async subscribers.
     * Calling-thread hot path (O(1), subscriber-count-independent):
     *   1 CLQ.offer to the registry inbox + 1 LockSupport.unpark of the fan-out thread.
     * The fan-out thread handles the O(N) work asynchronously.
     */
    @Benchmark
    public void dispatch() {
        registry.dispatch(event);
    }
}
