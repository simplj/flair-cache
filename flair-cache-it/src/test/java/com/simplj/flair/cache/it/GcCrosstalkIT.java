package com.simplj.flair.cache.it;

import com.simplj.flair.cache.FlairCache;
import com.simplj.flair.cache.FlairCluster;
import com.simplj.flair.cache.replication.ConsistencyMode;
import com.simplj.flair.cache.store.CacheBlock;
import com.simplj.flair.cache.watch.WatchHandle;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.logging.Logger;

import static com.simplj.flair.cache.it.ITSupport.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Scenario 15 — GC cross-talk under sustained mixed load.
 *
 * <h2>Motivation</h2>
 * Isolated JMH benchmarks for {@code HybridLogicalClock} and {@code WatchRegistry} showed
 * GC interference when run adjacently in the same session: one subsystem's allocation pattern
 * affected the other's tail latency. This test reproduces that interaction under realistic
 * concurrent facade load to confirm the interference is bounded.
 *
 * <h2>Load profile (5 s warmup + 30 s measurement)</h2>
 * A 5-second unmetered warmup phase runs all threads first so the JIT compiles hot paths
 * before samples are collected. The GC/heap baseline is taken after warmup.
 * All of the following then run concurrently for 30 s against a 3-node FlairCluster:
 * <ul>
 *   <li>4 threads: continuous {@code put()/get()} generating HLC timestamps on every write</li>
 *   <li>2 threads: continuous DSL {@code where().fetch()} queries over a 10k-entry block</li>
 *   <li>20 active watch subscriptions on the same put/get block, receiving {@code onPut} events</li>
 *   <li>QUORUM-mode replication traffic from all put() calls</li>
 * </ul>
 *
 * <h2>Assertions</h2>
 * <ol>
 *   <li><b>get() p99 latency</b>: warmed IT observed ~1,800 ns (~9× JMH isolated 200 ns).
 *       {@code get()} is a pure {@code ConcurrentHashMap.get()} — no locks, no HLC call.
 *       Inflation under 4 concurrent writers has two sources: (a) CPU cache-coherence pressure
 *       — concurrent CHM puts broadcast cache-line invalidations; the reader must re-fetch from
 *       L3/DRAM; (b) GC pressure from per-put allocation ({@code CacheEntry + HLCTimestamp +
 *       ByteArrayKey}) causing occasional minor-GC stop-the-world pauses.
 *       {@code GetUnderWritePressureBenchmark} isolates this: p99 = 283 ns at 0 writers →
 *       633 ns at 4 writers (+2.2×); individual iterations spike to ~8 µs under GC.
 *       The remaining gap to 1,800 ns in this test comes from the fuller environment:
 *       scheduling jitter from 20 watch drain threads + DSL threads + QUORUM traffic.
 *       Threshold: {@value GET_P99_THRESHOLD_NS} ns (5.5× warmed observed).</li>
 *   <li><b>put() p99 latency</b>: warmed observed ~53,000 ns. The dominant overhead (~48 µs)
 *       is indirect: OS scheduling jitter from 20 async drain threads waking concurrently after
 *       each dispatch, plus GC pressure from per-put {@code DispatchWork}/{@code ChangeEvent}
 *       object churn. The calling-thread {@code dispatch()} cost is O(1) regardless of subscriber
 *       count (confirmed by {@code WatchSubscriberScaleBenchmark}: p99 = 1,444 ns at 1 subscriber
 *       → 1,980 ns at 50 subscribers, +536 ns). Threshold:
 *       {@value PUT_P99_THRESHOLD_NS} ns (4× warmed observed) — sized to absorb up to 50
 *       subscribers: worst-case linear projection 53,000 × 50/20 = 132,500 ns &lt; threshold.</li>
 *   <li><b>Total GC pause time</b>: G1GC (MaxGCPauseMillis=50) over the 30 s measurement window.
 *       Observed ~4 s on developer hardware. Threshold: {@value GC_PAUSE_THRESHOLD_MS} ms
 *       (2.5× observed; catches memory-leak regressions where GC runs continuously).</li>
 *   <li><b>Heap growth</b>: measured from the post-warmup baseline. Observed 1.3–2.5×
 *       depending on GC timing. Threshold: {@value MAX_HEAP_GROWTH_RATIO}× catches runaway
 *       heap growth while surviving GC-cycle variance in the denominator.</li>
 * </ol>
 *
 * <p>All four measurements are printed explicitly to {@code stdout} regardless of pass/fail
 * so this test functions as a measurement tool across CI runs, not just a regression gate.</p>
 */
@Timeout(value = 120, unit = TimeUnit.SECONDS)
class GcCrosstalkIT {

    // ── Thresholds ───────────────────────────────────────────────────────────
    //
    // Threshold derivation (from controlled hypothesis-testing experiments):
    //
    // (1) get() p99 mechanism: get() is a pure ConcurrentHashMap.get() — no StampedLock,
    //     no HLC call. The ~9× inflation vs the JMH isolated baseline (200 ns → ~1,800 ns)
    //     has two root causes:
    //       (a) CPU cache-coherence pressure: concurrent CHM put() calls broadcast cache-line
    //           invalidation signals; the reading thread must re-fetch from L3/DRAM.
    //           GetUnderWritePressureBenchmark isolates this: p99 = 283 ns at 0 writers →
    //           633 ns at 4 writers (+2.2×); GC spikes push individual iterations to ~8 µs.
    //       (b) GC stop-the-world from per-put CacheEntry/HLCTimestamp/ByteArrayKey allocation
    //           churn across 4 threads + watch event allocation from 20 subscribers.
    //     The remaining gap to 1,800 ns in this test comes from the fuller environment: OS
    //     scheduling jitter from 20 watch drain threads + 2 DSL threads + QUORUM write thread.
    //     Threshold is set at 5.5× the warmed observed value to catch regressions that add
    //     locking or blocking to the get() path (e.g., accidentally taking a write lock).
    //
    // (2) put() p99 mechanism: warmed observed ~53,000 ns; the ~48,000 ns overhead above the
    //     5,000 ns no-watch baseline is indirect — OS scheduling jitter from 20 async drain
    //     threads waking concurrently after each dispatch, plus GC pressure from per-put
    //     DispatchWork/ChangeEvent allocation. The calling-thread dispatch() cost is O(1)
    //     regardless of subscriber count: WatchSubscriberScaleBenchmark measured p99 = 1,444 ns
    //     at 1 subscriber and 1,980 ns at 50 subscribers (+536 ns for 49 extra subs). Threshold
    //     is set at 4× the warmed observed value to catch regressions that make watch dispatch
    //     synchronous or add per-put heap allocation. 50-subscriber boundary: worst-case linear
    //     projection 53,000 × 50/20 = 132,500 ns — still within threshold with 67,500 ns margin.
    //
    // (3) GC pause mechanism: driven by allocation rate from 4 continuous put threads +
    //     DSL query results. G1GC (with MaxGCPauseMillis=50 hint) produces ~4 s of total
    //     pause over the 30 s window on the reference machine. Threshold is 2.5× to catch
    //     memory-leak regressions (heap never shrinks → GC runs continuously).
    //
    // (4) Heap ratio mechanism: baseline is taken after WARMUP_MS of load, so the JVM is
    //     in a stabilised state. The cache accumulates ~4k live entries from put threads;
    //     GC keeps heap growth bounded. Observed range 1.3–2.5× depending on GC timing.
    //     Threshold is 5× to catch runaway heap growth while allowing GC-timing variance.

    /**
     * get() p99 threshold with JIT warmup active.
     * JMH isolated baseline (0 concurrent writers): 200 ns. Warmed IT observed: ~1,800 ns.
     *
     * <p>{@code get()} is a pure {@code ConcurrentHashMap.get()} — no locks, no HLC call.
     * The 9× inflation vs JMH is compound: CPU cache-coherence pressure from 4 concurrent
     * CHM writers (+2.2× from write pressure alone, per {@code GetUnderWritePressureBenchmark}),
     * per-put GC allocation churn (individual p99 spikes to ~8 µs under minor GC), and OS
     * scheduling jitter from 20 async watch drain threads competing for CPU with put threads.</p>
     *
     * <p>Threshold: 10,000 ns (5.5× warmed observed) — catches regressions that add locking
     * or blocking to the get() path, not the expected ~1,800 ns from the current environment.</p>
     */
    private static final long GET_P99_THRESHOLD_NS  = 10_000L;

    /**
     * put() p99 threshold (store + HLC + 20 async WatchRegistry subscriptions, EVENTUAL mode).
     * JMH isolated baseline: 500 ns.  Warmed IT observed: ~53,000 ns (~106× JMH).
     *
     * <p>The ~48,000 ns overhead from 20 async subscribers is indirect: OS scheduling jitter
     * from 20 drain threads waking concurrently after each dispatch, plus GC pressure from
     * per-put {@code DispatchWork}/{@code ChangeEvent} allocation. The calling-thread
     * {@code dispatch()} cost is O(1) regardless of subscriber count — confirmed by
     * {@code WatchSubscriberScaleBenchmark}: p99 = 1,444 ns at 1 subscriber → 1,980 ns at
     * 50 subscribers (+536 ns for 49 extra subscribers).</p>
     *
     * <p>Known boundary (50 subscribers): worst-case linear scaling projects
     * 53,000 × (50/20) = 132,500 ns — within this threshold by 67,500 ns. The 4× multiplier
     * holds comfortably through at least 50 subscribers without adjustment.</p>
     *
     * <p>Catches regressions that: make watch dispatch synchronous (O(N) calling-thread cost),
     * add per-put heap allocation on the store hot path, or block the NIO selector thread.</p>
     */
    private static final long PUT_P99_THRESHOLD_NS  = 200_000L;

    /**
     * Total GC pause budget over the 30 s measurement window.
     * Observed with JIT warmup and MaxGCPauseMillis=50: ~4 s. Threshold: 10 s (2.5× observed)
     * — catches memory-leak regressions where GC runs nearly continuously (30+ s).
     */
    private static final long GC_PAUSE_THRESHOLD_MS = 10_000L;

    /**
     * Maximum heap-growth ratio (end / start) over the measurement window.
     * Baseline taken after warmup (heap already stabilised at ~100–280 MB).
     * Observed ratio: 1.3–2.5× (varies with GC timing). Threshold: 5.0× catches runaway
     * heap growth while allowing normal GC-cycle variance.
     */
    private static final double MAX_HEAP_GROWTH_RATIO = 5.0;

    /** Duration of the sustained-load window. */
    private static final long LOAD_WINDOW_MS = 30_000L;

    /** Unmetered JIT warmup before the measurement window. No samples recorded, no GC baseline taken. */
    private static final long WARMUP_MS = 5_000L;

    /** Sample count for p99 calculations — latency array is pre-allocated at this size. */
    private static final int SAMPLE_COUNT = 200_000;

    private static final Logger log = Logger.getLogger(GcCrosstalkIT.class.getName());

    private FlairCluster cluster;

    @AfterEach
    void tearDown() {
        if (cluster != null) {
            cluster.shutdown();
            cluster = null;
        }
    }

    @Test
    void sustainedMixedLoad_gcMetricsAndLatencyWithinThresholds() throws Exception {
        // ── Cluster setup ─────────────────────────────────────────────────────
        int basePort = freePort();
        cluster = FlairCluster.builder()
                .basePort(basePort)
                .nodes(3)
                .consistency(ConsistencyMode.QUORUM)
                .build()
                .start();

        // Block for put/get throughput (EVENTUAL so put() doesn't block on ACK latency,
        // letting us measure pure store + HLC overhead at p99).
        CacheBlock<String, String> putGetBlock = cluster.node(0).<String, String>registerBlock("pg")
                .keyCodec(STRING_CODEC).valueCodec(STRING_CODEC)
                .consistency(ConsistencyMode.EVENTUAL)
                .build();
        cluster.node(1).<String, String>registerBlock("pg")
                .keyCodec(STRING_CODEC).valueCodec(STRING_CODEC).build();
        cluster.node(2).<String, String>registerBlock("pg")
                .keyCodec(STRING_CODEC).valueCodec(STRING_CODEC).build();

        // Block for QUORUM replication traffic (separate block so ACK latency doesn't pollute put p99)
        CacheBlock<String, String> quorumBlock = cluster.node(0).<String, String>registerBlock("qr")
                .keyCodec(STRING_CODEC).valueCodec(STRING_CODEC)
                .consistency(ConsistencyMode.QUORUM)
                .build();
        cluster.node(1).<String, String>registerBlock("qr")
                .keyCodec(STRING_CODEC).valueCodec(STRING_CODEC).build();
        cluster.node(2).<String, String>registerBlock("qr")
                .keyCodec(STRING_CODEC).valueCodec(STRING_CODEC).build();

        // Block for DSL queries — pre-populate with 10k entries.
        CacheBlock<String, String> dslBlock = cluster.node(0).<String, String>registerBlock("dsl")
                .keyCodec(STRING_CODEC).valueCodec(STRING_CODEC)
                .consistency(ConsistencyMode.EVENTUAL)
                .build();
        cluster.node(1).<String, String>registerBlock("dsl")
                .keyCodec(STRING_CODEC).valueCodec(STRING_CODEC).build();
        cluster.node(2).<String, String>registerBlock("dsl")
                .keyCodec(STRING_CODEC).valueCodec(STRING_CODEC).build();

        for (int i = 0; i < 10_000; i++) {
            dslBlock.put("dsl" + i, (i % 2 == 0) ? "even" : "odd");
        }

        // ── Watch subscriptions ───────────────────────────────────────────────
        LongAdder watchEventCount = new LongAdder();
        List<WatchHandle> handles = new ArrayList<>(20);
        for (int i = 0; i < 20; i++) {
            handles.add(cluster.node(0).watchRegistry("pg")
                    .<String, String>watch()
                    .onPut((k, v) -> watchEventCount.increment())
                    .register());
        }

        // ── Latency sample arrays ─────────────────────────────────────────────
        long[] getNanos  = new long[SAMPLE_COUNT];
        long[] putNanos  = new long[SAMPLE_COUNT];
        int[]  getIdx    = {0};
        int[]  putIdx    = {0};

        // ── Sustained load (warmup first, then measurement window) ────────────
        // measuring=false during warmup so samples are not recorded and indices stay at 0.
        // The GC/heap baseline is taken after warmup so JIT compilation activity is excluded
        // from the measurement window. Experiment confirmed: warmup reduces get() p99 from
        // ~5 µs to ~1.8 µs and enables put() p99 to reflect the true WatchRegistry dispatch
        // cost rather than JIT-noise dominated values.
        AtomicBoolean measuring = new AtomicBoolean(false);
        AtomicBoolean running   = new AtomicBoolean(true);
        ExecutorService pool = Executors.newFixedThreadPool(8);

        // 4 threads: continuous put/get, recording latencies only when measuring is true.
        for (int t = 0; t < 4; t++) {
            final int tid = t;
            pool.submit(() -> {
                int seq = 0;
                while (running.get()) {
                    String key = "k" + tid + "-" + (seq++ % 1000);
                    String val = "v" + seq;

                    long t0 = System.nanoTime();
                    putGetBlock.put(key, val);
                    long t1 = System.nanoTime();
                    putGetBlock.get(key);
                    long t2 = System.nanoTime();

                    if (measuring.get()) {
                        int pi = putIdx[0]++;
                        int gi = getIdx[0]++;
                        if (pi < SAMPLE_COUNT) putNanos[pi] = t1 - t0;
                        if (gi < SAMPLE_COUNT) getNanos[gi]  = t2 - t1;
                    }
                }
            });
        }

        // 2 threads: continuous DSL queries (even-valued entries).
        for (int t = 0; t < 2; t++) {
            pool.submit(() -> {
                while (running.get()) {
                    @SuppressWarnings("unchecked")
                    List<String> r = (List<String>) cluster.node(0).query()
                            .from("dsl", String.class, v -> (String) v)
                            .where(v -> "even".equals(v))
                            .fetch();
                    // consume result to prevent dead-code elimination
                    if (r.isEmpty()) log.warning("DSL returned 0 results");
                }
            });
        }

        // 1 thread: QUORUM writes — sustained replication traffic with ACK round-trips.
        pool.submit(() -> {
            int seq = 0;
            while (running.get()) {
                try {
                    quorumBlock.put("qk" + (seq++ % 500), "qv" + seq);
                } catch (Exception e) {
                    // QUORUM writes may timeout under heavy load — count but don't fail.
                    log.fine("QUORUM write exception (ignored): " + e.getMessage());
                }
            }
        });

        // Warmup phase: threads run all hot paths (put/get/HLC/DSL/WatchRegistry) for
        // WARMUP_MS without recording samples, allowing the JIT to compile all hot paths
        // before the measurement window starts and the GC/heap baseline is captured.
        Thread.sleep(WARMUP_MS);

        // ── Baseline GC and heap snapshot (after warmup, before measurement window) ─
        MemoryMXBean memBean = ManagementFactory.getMemoryMXBean();
        List<GarbageCollectorMXBean> gcBeans = ManagementFactory.getGarbageCollectorMXBeans();
        long gcCountBefore = totalGcCount(gcBeans);
        long gcTimeBefore  = totalGcTime(gcBeans);
        long heapBefore    = memBean.getHeapMemoryUsage().getUsed();
        System.out.println("[GcCrosstalkIT] BASELINE (after " + WARMUP_MS + " ms warmup) — heap: "
                + mb(heapBefore) + " MB, GC count: " + gcCountBefore
                + ", GC time: " + gcTimeBefore + " ms");

        // Start the measurement window.
        measuring.set(true);
        Thread.sleep(LOAD_WINDOW_MS);
        running.set(false);
        pool.shutdown();
        pool.awaitTermination(10, TimeUnit.SECONDS);

        // ── Cancel watch handles ──────────────────────────────────────────────
        for (WatchHandle h : handles) h.cancel();

        // ── Post-load GC and heap snapshot ────────────────────────────────────
        long gcCountAfter = totalGcCount(gcBeans);
        long gcTimeAfter  = totalGcTime(gcBeans);
        long heapAfter    = memBean.getHeapMemoryUsage().getUsed();

        long gcCountDelta = gcCountAfter - gcCountBefore;
        long gcTimeDelta  = gcTimeAfter  - gcTimeBefore;
        double heapRatio  = heapBefore > 0 ? (double) heapAfter / heapBefore : 1.0;

        // ── Compute p99 latencies ─────────────────────────────────────────────
        int getSamples = Math.min(getIdx[0], SAMPLE_COUNT);
        int putSamples = Math.min(putIdx[0], SAMPLE_COUNT);

        long getP99 = p99(getNanos, getSamples);
        long putP99 = p99(putNanos, putSamples);

        // ── Report (always printed, regardless of pass/fail) ──────────────────
        System.out.println("[GcCrosstalkIT] RESULTS after " + LOAD_WINDOW_MS + " ms sustained load:");
        System.out.println("  get() samples : " + getSamples);
        System.out.println("  get() p99     : " + getP99  + " ns  (threshold: " + GET_P99_THRESHOLD_NS  + " ns)");
        System.out.println("  put() samples : " + putSamples);
        System.out.println("  put() p99     : " + putP99  + " ns  (threshold: " + PUT_P99_THRESHOLD_NS  + " ns)");
        System.out.println("  GC collections: " + gcCountDelta);
        System.out.println("  GC pause total: " + gcTimeDelta + " ms  (threshold: " + GC_PAUSE_THRESHOLD_MS + " ms)");
        System.out.println("  Heap before   : " + mb(heapBefore) + " MB");
        System.out.println("  Heap after    : " + mb(heapAfter)  + " MB");
        System.out.println("  Heap ratio    : " + String.format("%.2f", heapRatio) + "×  (threshold: " + MAX_HEAP_GROWTH_RATIO + "×)");
        System.out.println("  Watch events  : " + watchEventCount.sum());

        // ── Assertions ────────────────────────────────────────────────────────
        // (a) get() p99 regression gate
        assertTrue(getSamples > 1000,
                "Must collect at least 1000 get() samples for a meaningful p99; got " + getSamples);
        assertTrue(getP99 <= GET_P99_THRESHOLD_NS,
                "get() p99 regressed: " + getP99 + " ns > threshold " + GET_P99_THRESHOLD_NS + " ns. "
                + "Tuning: increase GET_P99_THRESHOLD_NS if CI machine is consistently slower.");

        // (b) put() p99 regression gate — dominated by watch dispatch scheduling jitter, not HLC
        assertTrue(putSamples > 1000,
                "Must collect at least 1000 put() samples for a meaningful p99; got " + putSamples);
        assertTrue(putP99 <= PUT_P99_THRESHOLD_NS,
                "put() p99 regressed: " + putP99 + " ns > threshold " + PUT_P99_THRESHOLD_NS + " ns. "
                + "Tuning: increase PUT_P99_THRESHOLD_NS if CI machine is consistently slower.");

        // (c) GC pause time budget
        assertTrue(gcTimeDelta <= GC_PAUSE_THRESHOLD_MS,
                "Total GC pause " + gcTimeDelta + " ms exceeded budget " + GC_PAUSE_THRESHOLD_MS + " ms. "
                + "Tuning: increase GC_PAUSE_THRESHOLD_MS or reduce LOAD_WINDOW_MS. "
                + "Note: this threshold is a tunable assumption, not a hard architectural guarantee.");

        // (d) Heap growth guard — no OOM or sustained leak
        assertTrue(heapRatio <= MAX_HEAP_GROWTH_RATIO,
                "Heap grew " + String.format("%.2f", heapRatio) + "× (threshold " + MAX_HEAP_GROWTH_RATIO + "×). "
                + "Heap before: " + mb(heapBefore) + " MB, after: " + mb(heapAfter) + " MB. "
                + "This may indicate a memory leak or unexpectedly large cache growth.");
    }

    // ── Measurement helpers ───────────────────────────────────────────────────

    private static long p99(long[] samples, int count) {
        if (count == 0) return 0L;
        long[] copy = Arrays.copyOf(samples, count);
        Arrays.sort(copy);
        // p99 index: 99th percentile = element at index floor(0.99 * count)
        int idx = (int) (0.99 * count);
        if (idx >= count) idx = count - 1;
        return copy[idx];
    }

    private static long totalGcCount(List<GarbageCollectorMXBean> beans) {
        long total = 0;
        for (GarbageCollectorMXBean b : beans) {
            long c = b.getCollectionCount();
            if (c > 0) total += c;
        }
        return total;
    }

    private static long totalGcTime(List<GarbageCollectorMXBean> beans) {
        long total = 0;
        for (GarbageCollectorMXBean b : beans) {
            long t = b.getCollectionTime();
            if (t > 0) total += t;
        }
        return total;
    }

    private static long mb(long bytes) {
        return bytes / (1024L * 1024L);
    }
}
