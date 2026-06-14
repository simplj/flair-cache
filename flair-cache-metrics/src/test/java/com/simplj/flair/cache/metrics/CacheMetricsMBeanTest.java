package com.simplj.flair.cache.metrics;

import com.simplj.flair.cache.serial.codecs.ByteArrayCodec;
import com.simplj.flair.cache.serial.codecs.StringCodec;
import com.simplj.flair.cache.store.CacheBlock;
import com.simplj.flair.cache.store.EvictionPolicy;
import org.junit.jupiter.api.Test;

import com.simplj.flair.cache.hlc.HLCTimestamp;
import com.simplj.flair.cache.store.CacheEntry;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class CacheMetricsMBeanTest {

    private CacheBlock<String, byte[]> newBlock(String name) {
        return CacheBlock.<String, byte[]>builder()
                .name(name)
                .keyCodec(StringCodec.INSTANCE)
                .valueCodec(ByteArrayCodec.INSTANCE)
                .build();
    }

    @Test
    void hitCountAndMissCount() {
        try (CacheBlock<String, byte[]> block = newBlock("test")) {
            CacheMetricsMBean bean = new CacheMetricsMBean(block);
            block.put("k", "v".getBytes());

            block.get("k");      // hit
            block.get("k");      // hit
            block.get("missing");// miss

            assertEquals(2, bean.getHitCount());
            assertEquals(1, bean.getMissCount());
        }
    }

    @Test
    void hitRatePercentZeroDenominator() {
        try (CacheBlock<String, byte[]> block = newBlock("test")) {
            CacheMetricsMBean bean = new CacheMetricsMBean(block);
            assertEquals(0.0, bean.getHitRatePercent(), 1e-9, "must return 0.0, not NaN");
        }
    }

    @Test
    void hitRatePercentCalculation() {
        try (CacheBlock<String, byte[]> block = newBlock("test")) {
            CacheMetricsMBean bean = new CacheMetricsMBean(block);
            block.put("k", "v".getBytes());
            block.get("k");       // hit
            block.get("missing"); // miss
            // 1 hit, 1 miss → 50.0%
            assertEquals(50.0, bean.getHitRatePercent(), 1e-9);
        }
    }

    @Test
    void putCountTrackedViaListener() {
        try (CacheBlock<String, byte[]> block = newBlock("test")) {
            CacheMetricsMBean bean = new CacheMetricsMBean(block);
            block.put("k1", "v".getBytes());
            block.put("k2", "v".getBytes());
            assertEquals(2, bean.getPutCount());
        }
    }

    @Test
    void deleteCountTrackedViaListener() {
        try (CacheBlock<String, byte[]> block = newBlock("test")) {
            CacheMetricsMBean bean = new CacheMetricsMBean(block);
            block.put("k", "v".getBytes());
            block.delete("k");
            block.delete("absent"); // silent delete — no event
            assertEquals(1, bean.getDeleteCount());
        }
    }

    @Test
    void sizeReflectsCurrentEntryCount() {
        try (CacheBlock<String, byte[]> block = newBlock("test")) {
            CacheMetricsMBean bean = new CacheMetricsMBean(block);
            assertEquals(0, bean.getSize());
            block.put("a", "v".getBytes());
            block.put("b", "v".getBytes());
            assertEquals(2, bean.getSize());
            block.delete("a");
            assertEquals(1, bean.getSize());
        }
    }

    @Test
    void evictionCountTrackedViaStats() {
        try (CacheBlock<String, byte[]> block = CacheBlock.<String, byte[]>builder()
                .name("ev")
                .keyCodec(StringCodec.INSTANCE)
                .valueCodec(ByteArrayCodec.INSTANCE)
                .eviction(EvictionPolicy.LRU)
                .maxEntries(2)
                .build()) {

            CacheMetricsMBean bean = new CacheMetricsMBean(block);
            block.put("k1", "v".getBytes());
            block.put("k2", "v".getBytes());
            block.get("k1"); // make k1 recently used so k2 is evicted
            block.put("k3", "v".getBytes()); // k2 evicted

            assertTrue(bean.getEvictionCount() >= 1, "at least one eviction expected");
        }
    }

    @Test
    void expirationCountTrackedViaStats() throws Exception {
        try (CacheBlock<String, byte[]> block = CacheBlock.<String, byte[]>builder()
                .name("exp")
                .keyCodec(StringCodec.INSTANCE)
                .valueCodec(ByteArrayCodec.INSTANCE)
                .ttl(Duration.ofMillis(80))
                .sweepIntervalMs(50)
                .build()) {

            CacheMetricsMBean bean = new CacheMetricsMBean(block);
            block.put("x", "v".getBytes());
            Thread.sleep(200); // let TTL sweep fire
            block.get("x");    // lazy expiry path also increments counter

            assertTrue(bean.getExpirationCount() >= 1,
                    "at least one expiration expected; got " + bean.getExpirationCount());
        }
    }

    @Test
    void putRawCountedInPutCount() {
        // putRaw() fires notifyPut() in LocalStore (replication/bootstrap write path).
        // getPutCount() must count both local puts and replicated puts —
        // per spec: "Total puts (local + replicated)".
        try (CacheBlock<String, byte[]> block = newBlock("raw-put")) {
            CacheMetricsMBean bean = new CacheMetricsMBean(block);

            block.put("k", "v1".getBytes());
            assertEquals(1L, bean.getPutCount(), "standard put counted");

            // Obtain raw key bytes from the existing entry (avoids manual codec serialization)
            Map.Entry<byte[], CacheEntry> snap =
                    block.rawSnapshotEntries().entrySet().iterator().next();
            byte[] rawKey = snap.getKey();
            HLCTimestamp ts = block.hlcNow();

            block.putRaw(rawKey, new CacheEntry("v2".getBytes(), ts, 0L, 0L, 0L, null));
            assertEquals(2L, bean.getPutCount(),
                    "putRaw() (replicated write) must also increment getPutCount()");
        }
    }

    @Test
    void baselineSubtractedForPreRegistrationHitsAndMisses() {
        try (CacheBlock<String, byte[]> block = newBlock("late-hit")) {
            block.put("k", "v".getBytes());
            block.get("k");       // hit  — before registration
            block.get("missing"); // miss — before registration

            // Register AFTER some operations — pre-registration stats must be subtracted
            CacheMetricsMBean bean = new CacheMetricsMBean(block);

            assertEquals(0L,  bean.getHitCount(),       "pre-registration hits must not appear");
            assertEquals(0L,  bean.getMissCount(),      "pre-registration misses must not appear");
            assertEquals(0.0, bean.getHitRatePercent(), 1e-9, "rate must be 0 when no post-registration ops");

            block.get("k");       // hit  — after registration
            block.get("missing"); // miss — after registration

            assertEquals(1L,   bean.getHitCount());
            assertEquals(1L,   bean.getMissCount());
            assertEquals(50.0, bean.getHitRatePercent(), 1e-9);
        }
    }

    @Test
    void baselineSubtractedForPreRegistrationEvictions() {
        try (CacheBlock<String, byte[]> block = CacheBlock.<String, byte[]>builder()
                .name("late-ev")
                .keyCodec(StringCodec.INSTANCE)
                .valueCodec(ByteArrayCodec.INSTANCE)
                .eviction(EvictionPolicy.LRU)
                .maxEntries(1)
                .build()) {

            block.put("k1", "v".getBytes());
            block.put("k2", "v".getBytes()); // evicts k1 — before registration

            CacheMetricsMBean bean = new CacheMetricsMBean(block);

            assertEquals(0L, bean.getEvictionCount(), "pre-registration evictions must not appear");

            block.put("k3", "v".getBytes()); // evicts k2 — after registration

            assertEquals(1L, bean.getEvictionCount());
        }
    }

    @Test
    void concurrentHitsNoLostIncrements() throws Exception {
        final int threads = 8;
        final int perThread = 125_000;
        final int total = threads * perThread;

        try (CacheBlock<String, byte[]> block = newBlock("concurrent")) {
            block.put("k", "v".getBytes());
            CacheMetricsMBean bean = new CacheMetricsMBean(block);

            CountDownLatch ready = new CountDownLatch(threads);
            CountDownLatch start = new CountDownLatch(1);
            ExecutorService pool = Executors.newFixedThreadPool(threads);
            List<Future<?>> futures = new ArrayList<>();

            for (int t = 0; t < threads; t++) {
                futures.add(pool.submit(() -> {
                    ready.countDown();
                    try { start.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                    for (int i = 0; i < perThread; i++) block.get("k");
                }));
            }

            ready.await();
            start.countDown();
            pool.shutdown();
            assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));

            for (Future<?> f : futures) f.get();
            assertEquals(total, bean.getHitCount(), "no hits must be lost under contention");
        }
    }
}
