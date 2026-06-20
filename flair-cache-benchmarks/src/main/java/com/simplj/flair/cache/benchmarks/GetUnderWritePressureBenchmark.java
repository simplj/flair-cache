package com.simplj.flair.cache.benchmarks;

import com.simplj.flair.cache.store.CacheBlock;
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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Measures how get() p99 latency degrades as concurrent background writer count increases.
 *
 * <h2>Motivation</h2>
 * <p>{@code LocalStoreBenchmark.getHit} measures single-threaded get() in isolation: a clean
 * {@link java.util.concurrent.ConcurrentHashMap} lookup with no concurrent mutation. In practice,
 * a busy node runs concurrent put() threads alongside get() — and the write pressure causes
 * measurable get() latency inflation through two independent mechanisms:</p>
 *
 * <ol>
 *   <li><b>CPU cache-coherence pressure:</b> {@code ConcurrentHashMap.put()} modifies internal
 *       table nodes, broadcasting cache-line invalidation signals (MESI I-state) to all CPU cores.
 *       {@code get()} uses volatile reads — no lock — but must wait for the invalidated cache line
 *       to be re-fetched from L3 or DRAM. This adds latency proportional to how many writers are
 *       hitting the same map concurrently, regardless of key overlap.</li>
 *   <li><b>GC allocation churn:</b> each {@code put()} allocates a new {@code CacheEntry},
 *       {@code HLCTimestamp}, and {@code ByteArrayKey}. Under N concurrent put threads this
 *       floods the JVM's young generation, triggering more frequent minor GC pauses that
 *       stop-the-world all threads including get() callers.</li>
 * </ol>
 *
 * <p>Note: {@link com.simplj.flair.cache.hlc.HybridLogicalClock} uses a
 * {@link java.util.concurrent.locks.StampedLock} write lock in {@code now()} — concurrent
 * writers contend on this lock with each other, but it does NOT affect the get() caller:
 * {@code get()} never calls {@code hlc.now()} or {@code hlc.update()}. The get() path is
 * {@code ConcurrentHashMap.get()} only.</p>
 *
 * <h2>Setup</h2>
 * <p>The read key space ("r-*") and write key space ("w-*") are pre-populated independently.
 * Writers cycle through the write space so no new table growth occurs during measurement —
 * only in-place value updates. The benchmark method reads "r-5000", which no writer touches.
 * This isolates indirect cache-coherence and GC pressure from direct key contention.</p>
 *
 * <h2>Reading the results</h2>
 * <p>Compare p99 across {@code writerThreads} values. The inflection point — where p99 starts
 * rising faster than linearly — indicates where write pressure transitions from negligible to
 * a material contributor. At {@code writerThreads=0} this benchmark is equivalent to
 * {@code LocalStoreBenchmark.getHit} (single-threaded baseline).</p>
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class GetUnderWritePressureBenchmark {

    /** Number of concurrent background writer threads. 0 = single-threaded baseline. */
    @Param({"0", "1", "2", "4", "8", "16"})
    public int writerThreads;

    // Write-target key slots per writer: 10k pre-populated write keys / max 16 writers = 625 each.
    // Writers stay within their own slice so no two writers ever contend on the same key.
    private static final int WRITE_KEYS_PER_WRITER = 625;
    private static final int READ_KEY_COUNT  = 10_000;
    private static final int WRITE_KEY_COUNT = 10_000;  // 16 × 625

    private CacheBlock<String, String> block;
    private volatile boolean           running;
    private final List<Thread>         writers = new ArrayList<>();

    @Setup(Level.Trial)
    public void setup() {
        block = CacheBlock.<String, String>builder()
                .name("write-pressure")
                .keyCodec(BenchmarkCodecs.STRING)
                .valueCodec(BenchmarkCodecs.STRING)
                .build();

        // Read-only key space — the benchmark method reads "r-5000".
        for (int i = 0; i < READ_KEY_COUNT; i++) {
            block.put("r-" + i, "val-" + i);
        }
        // Write-target key space — pre-populated so writers never trigger ConcurrentHashMap
        // table growth during measurement; they only overwrite existing entries.
        for (int i = 0; i < WRITE_KEY_COUNT; i++) {
            block.put("w-" + i, "init");
        }

        running = true;
        for (int t = 0; t < writerThreads; t++) {
            final int tid = t;
            Thread writer = new Thread(() -> {
                int seq = 0;
                while (running) {
                    // Each writer owns WRITE_KEYS_PER_WRITER exclusive key slots.
                    block.put("w-" + (tid * WRITE_KEYS_PER_WRITER + seq % WRITE_KEYS_PER_WRITER),
                            "v" + seq);
                    seq++;
                }
            }, "bench-writer-" + tid);
            writer.setDaemon(true);
            writer.start();
            writers.add(writer);
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        running = false;
        for (Thread w : writers) {
            try { w.join(2000); } catch (InterruptedException ignored) {}
        }
        writers.clear();
        if (block != null) block.close();
    }

    /**
     * Reads "r-5000" — a key that no background writer ever touches.
     * Latency increases above the {@code writerThreads=0} baseline reflect indirect
     * write pressure: CPU cache-coherence invalidations from concurrent CHM puts,
     * and GC pauses from concurrent per-put allocation (CacheEntry + HLCTimestamp).
     */
    @Benchmark
    public String getUnderWriters() {
        return block.get("r-5000");
    }
}
