package com.simplj.flair.cache.benchmarks;

import com.simplj.flair.cache.hlc.HLCTimestamp;
import com.simplj.flair.cache.hlc.HybridLogicalClock;
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
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.TimeUnit;

/**
 * Benchmarks {@link HybridLogicalClock#now()} under 8-thread contention.
 *
 * <p>{@code now()} is {@code synchronized} — this benchmark measures monitor-lock
 * acquisition and clock-tick overhead when 8 threads all call {@code now()} on the same
 * shared instance. This mirrors the worst-case hot path in a write-heavy workload where
 * every put() advances the clock.</p>
 *
 * <p>Uses {@link Scope#Benchmark} so all 8 threads share the same {@link HybridLogicalClock}
 * instance. A per-thread instance would remove contention and not reflect production usage.</p>
 *
 * <p>{@link #hlcUpdate} measures {@link HybridLogicalClock#update(HLCTimestamp)} under the
 * same 8-thread contention. {@code update()} is called on every incoming replication frame —
 * comparing its p99 against {@code hlcNow()} reveals whether the remote-advance path is faster
 * or slower than the local-tick path under lock contention.</p>
 */
@State(Scope.Benchmark)
@BenchmarkMode({Mode.SampleTime, Mode.Throughput})
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@Threads(8)
public class HlcBenchmark {

    private HybridLogicalClock hlc;

    /**
     * Per-thread target timestamp for {@link #hlcUpdate}.
     * Initialised to wall-clock time so the update is a realistic remote-advance scenario.
     * {@link Scope#Thread} prevents contention on the target object itself.
     */
    @State(Scope.Thread)
    public static class UpdateState {
        HLCTimestamp target;

        @Setup(Level.Trial)
        public void setup() {
            target = new HLCTimestamp(System.currentTimeMillis(), 0L);
        }
    }

    @Setup(Level.Trial)
    public void setup() {
        hlc = new HybridLogicalClock();
    }

    /**
     * Advances the HLC for a local event under 8-thread contention.
     * Measures: monitor-lock acquire + max(wall, logical) + counter increment + object alloc.
     */
    @Benchmark
    public HLCTimestamp hlcNow() {
        return hlc.now();
    }

    /**
     * Updates the HLC with a remote timestamp under 8-thread contention.
     * Called on every incoming replication PUT or DELETE frame.
     * Measures: monitor-lock acquire + max(wall, local, remote) + counter update.
     * Returns void; use {@link Blackhole} to suppress dead-code elimination via the bh param.
     */
    @Benchmark
    public void hlcUpdate(UpdateState state, Blackhole bh) {
        hlc.update(state.target);
        bh.consume(state.target);
    }
}
