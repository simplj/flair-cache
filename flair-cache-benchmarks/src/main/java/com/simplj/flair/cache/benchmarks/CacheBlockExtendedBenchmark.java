package com.simplj.flair.cache.benchmarks;

import com.simplj.flair.cache.store.CacheBlock;
import com.simplj.flair.cache.store.CacheEntry;
import com.simplj.flair.cache.store.EvictionPolicy;
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

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Benchmarks {@link CacheBlock} operations beyond the basic get/put/delete hot path.
 *
 * <p>Uses standalone {@link CacheBlock} instances (no FlairCache required) to isolate
 * store-layer performance from replication and gossip overhead.</p>
 *
 * <p>Operations measured:</p>
 * <ul>
 *   <li>{@link CacheBlock#contains(Object)} — presence check (does not deserialize value)</li>
 *   <li>{@link CacheBlock#putAll(Map)} for 1k and 10k entries — bulk write cost</li>
 *   <li>{@link CacheBlock#snapshot()} — typed copy of all entries (involves deserialization)</li>
 *   <li>{@link CacheBlock#rawSnapshotEntries()} — raw-byte copy (no deserialization)</li>
 *   <li>{@code put()} with TTL configured — measures expiry timestamp computation overhead</li>
 *   <li>{@code put()} on a full LRU-eviction store — measures eviction sampler cost</li>
 * </ul>
 */
@State(Scope.Benchmark)
@BenchmarkMode({Mode.SampleTime, Mode.Throughput})
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class CacheBlockExtendedBenchmark {

    private static final int STORE_SIZE = 10_000;
    private static final int BULK_1K    = 1_000;
    private static final int BULK_10K   = 10_000;
    private static final int LRU_CAP    = 1_000;

    private CacheBlock<String, String> blockNoTtl;
    private CacheBlock<String, String> blockWithTtl;
    private CacheBlock<String, String> blockLru;

    private Map<String, String> bulkMap1k;
    private Map<String, String> bulkMap10k;

    @State(Scope.Thread)
    public static class Counter {
        int i = 0;
    }

    @Setup(Level.Trial)
    public void setup() {
        blockNoTtl = CacheBlock.<String, String>builder()
                .name("ext-no-ttl")
                .keyCodec(BenchmarkCodecs.STRING)
                .valueCodec(BenchmarkCodecs.STRING)
                .build();
        for (int i = 0; i < STORE_SIZE; i++) {
            blockNoTtl.put("key-" + i, "val-" + i);
        }

        blockWithTtl = CacheBlock.<String, String>builder()
                .name("ext-with-ttl")
                .keyCodec(BenchmarkCodecs.STRING)
                .valueCodec(BenchmarkCodecs.STRING)
                .ttl(Duration.ofSeconds(60))
                .build();

        // Pre-fill LRU block to capacity so every new-key put triggers eviction
        blockLru = CacheBlock.<String, String>builder()
                .name("ext-lru")
                .keyCodec(BenchmarkCodecs.STRING)
                .valueCodec(BenchmarkCodecs.STRING)
                .eviction(EvictionPolicy.LRU)
                .maxEntries(LRU_CAP)
                .build();
        for (int i = 0; i < LRU_CAP; i++) {
            blockLru.put("pre-" + i, "v");
        }

        bulkMap1k  = new HashMap<>(BULK_1K);
        for (int i = 0; i < BULK_1K; i++) bulkMap1k.put("bk-" + i, "bv-" + i);

        bulkMap10k = new HashMap<>(BULK_10K);
        for (int i = 0; i < BULK_10K; i++) bulkMap10k.put("bk-" + i, "bv-" + i);
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        if (blockNoTtl  != null) blockNoTtl.close();
        if (blockWithTtl != null) blockWithTtl.close();
        if (blockLru    != null) blockLru.close();
    }

    /**
     * Presence check — no deserialization, just key-bytes lookup in the underlying
     * {@code ConcurrentHashMap}. Should be close to hot-path {@code get()} speed.
     */
    @Benchmark
    public boolean contains() {
        return blockNoTtl.contains("key-5000");
    }

    /** Bulk-write 1k entries. Second and subsequent calls update existing keys. */
    @Benchmark
    public void putAllOneK() {
        blockNoTtl.putAll(bulkMap1k);
    }

    /** Bulk-write 10k entries. Second and subsequent calls update existing keys. */
    @Benchmark
    public void putAllTenK() {
        blockNoTtl.putAll(bulkMap10k);
    }

    /**
     * Point-in-time copy of all 10k entries with full deserialization.
     * Measures: {@code ConcurrentHashMap} bulk copy (via constructor) + 10k decode calls.
     */
    @Benchmark
    public Map<String, String> snapshotTenK() {
        return blockNoTtl.snapshot();
    }

    /**
     * Point-in-time copy of all 10k entries as raw bytes (no deserialization).
     * Compare against {@link #snapshotTenK()} to isolate deserialization cost.
     */
    @Benchmark
    public Map<byte[], CacheEntry> rawSnapshotTenK() {
        return blockNoTtl.rawSnapshotEntries();
    }

    /**
     * Put with TTL configured — measures the overhead of computing the expiry timestamp
     * ({@code System.currentTimeMillis() + ttlMs}) per write, compared to the no-TTL path.
     */
    @Benchmark
    public void putWithTtl(Counter c) {
        blockWithTtl.put("tk-" + (c.i++ & 0xFFFF), "value");
    }

    /**
     * Put on a store at LRU capacity. Every new key triggers the eviction sampler
     * to select and remove the least-recently-used entry before inserting.
     * Measures: normal put cost + LRU candidate selection + eviction.
     */
    @Benchmark
    public void putEvictionLru(Counter c) {
        // Keys cycle through 100k unique strings; the LRU store holds 1k at most.
        // Each put of a not-recently-seen key is a guaranteed new insert → eviction.
        blockLru.put("new-" + (c.i++ % 100_000), "value");
    }

    /**
     * Get hit with LRU eviction enabled.
     *
     * <p>Unlike {@code LocalStoreBenchmark.getHit} (which uses a block with no eviction policy
     * and therefore pays zero access-tracking cost), this benchmark measures the full LRU
     * read path: ConcurrentHashMap lookup + CacheEntry allocation via {@code withAccess()} +
     * CAS {@code replace()} to update the entry's access timestamp and hit count.
     *
     * <p>Run this alongside {@code LocalStoreBenchmark.getHit} to isolate the incremental cost
     * of accurate LRU tracking. The delta is the price of LRU actually meaning LRU.
     */
    @Benchmark
    public String getHitLru() {
        return blockLru.get("pre-500");
    }
}
