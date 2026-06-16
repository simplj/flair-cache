package com.simplj.flair.cache.metrics;

import com.simplj.flair.cache.serial.codecs.ByteArrayCodec;
import com.simplj.flair.cache.serial.codecs.StringCodec;
import com.simplj.flair.cache.store.CacheBlock;
import com.simplj.flair.cache.store.EvictionPolicy;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.*;

class EvictionMetricsMBeanTest {

    private CacheBlock<String, byte[]> block(String name) {
        return CacheBlock.<String, byte[]>builder()
                .name(name)
                .keyCodec(StringCodec.INSTANCE)
                .valueCodec(ByteArrayCodec.INSTANCE)
                .build();
    }

    private CacheBlock<String, byte[]> evictingBlock(String name, int max) {
        return CacheBlock.<String, byte[]>builder()
                .name(name)
                .keyCodec(StringCodec.INSTANCE)
                .valueCodec(ByteArrayCodec.INSTANCE)
                .eviction(EvictionPolicy.LRU)
                .maxEntries(max)
                .build();
    }

    private CacheBlock<String, byte[]> ttlBlock(String name, long ttlMs) {
        return CacheBlock.<String, byte[]>builder()
                .name(name)
                .keyCodec(StringCodec.INSTANCE)
                .valueCodec(ByteArrayCodec.INSTANCE)
                .ttl(Duration.ofMillis(ttlMs))
                .sweepIntervalMs(50)
                .build();
    }

    @Test
    void initialCountsAreZero() {
        ConcurrentHashMap<String, CacheBlock<?, ?>> blocks = new ConcurrentHashMap<>();
        EvictionMetricsMBean bean = new EvictionMetricsMBean(blocks);
        assertEquals(0L, bean.getEvictedEntryCount());
        assertEquals(0L, bean.getExpiredEntryCount());
    }

    @Test
    void aggregatesEvictionsAcrossBlocks() {
        ConcurrentHashMap<String, CacheBlock<?, ?>> blocks = new ConcurrentHashMap<>();
        EvictionMetricsMBean bean = new EvictionMetricsMBean(blocks);

        try (CacheBlock<String, byte[]> b1 = evictingBlock("b1", 1);
             CacheBlock<String, byte[]> b2 = evictingBlock("b2", 1)) {

            blocks.put("b1", b1);
            blocks.put("b2", b2);

            // Each block evicts 1 entry
            b1.put("k1", "v".getBytes());
            b1.put("k2", "v".getBytes()); // evicts k1

            b2.put("k3", "v".getBytes());
            b2.put("k4", "v".getBytes()); // evicts k3

            assertEquals(2L, bean.getEvictedEntryCount(), "evictions from both blocks must aggregate");
        }
    }

    @Test
    void aggregatesExpirationsAcrossBlocks() throws Exception {
        ConcurrentHashMap<String, CacheBlock<?, ?>> blocks = new ConcurrentHashMap<>();
        EvictionMetricsMBean bean = new EvictionMetricsMBean(blocks);

        try (CacheBlock<String, byte[]> b1 = ttlBlock("b1", 80);
             CacheBlock<String, byte[]> b2 = ttlBlock("b2", 80)) {

            blocks.put("b1", b1);
            blocks.put("b2", b2);

            b1.put("x", "v".getBytes());
            b2.put("y", "v".getBytes());

            Thread.sleep(200); // let TTL sweep fire

            // Trigger lazy expiry via get (sweep may have already handled it)
            b1.get("x");
            b2.get("y");

            assertTrue(bean.getExpiredEntryCount() >= 2,
                    "expirations from both blocks must aggregate; got " + bean.getExpiredEntryCount());
        }
    }

    @Test
    void dynamicallyAddedBlockIsImmediatelyTracked() {
        // EvictionMetricsMBean holds a live reference to the shared map.
        // A block added AFTER the bean is constructed must be included in the aggregates
        // on the very next read — no re-registration or refresh call required.
        ConcurrentHashMap<String, CacheBlock<?, ?>> blocks = new ConcurrentHashMap<>();
        EvictionMetricsMBean bean = new EvictionMetricsMBean(blocks);

        try (CacheBlock<String, byte[]> b = evictingBlock("late-add", 1)) {
            blocks.put("late-add", b); // added AFTER bean construction

            b.put("k1", "v".getBytes());
            b.put("k2", "v".getBytes()); // evicts k1

            assertEquals(1L, bean.getEvictedEntryCount(),
                    "Block added to the live map after bean construction must be tracked immediately");
        }
    }

    @Test
    void emptyBlocksReturnZero() {
        ConcurrentHashMap<String, CacheBlock<?, ?>> blocks = new ConcurrentHashMap<>();
        try (CacheBlock<String, byte[]> b = block("b")) {
            blocks.put("b", b);
            EvictionMetricsMBean bean = new EvictionMetricsMBean(blocks);
            assertEquals(0L, bean.getEvictedEntryCount());
            assertEquals(0L, bean.getExpiredEntryCount());
        }
    }
}
