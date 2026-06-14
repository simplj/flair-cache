package com.simplj.flair.cache.metrics;

import com.simplj.flair.cache.store.CacheBlock;
import com.simplj.flair.cache.store.CacheStats;

import java.util.concurrent.ConcurrentHashMap;

public final class EvictionMetricsMBean implements EvictionMetricsMBeanInterface {

    private final ConcurrentHashMap<String, CacheBlock<?, ?>> blocks;

    EvictionMetricsMBean(ConcurrentHashMap<String, CacheBlock<?, ?>> blocks) {
        this.blocks = blocks;
    }

    @Override public long getEvictedEntryCount() {
        long total = 0L;
        for (CacheBlock<?, ?> b : blocks.values()) {
            total += b.stats().evictions();
        }
        return total;
    }

    @Override public long getExpiredEntryCount() {
        long total = 0L;
        for (CacheBlock<?, ?> b : blocks.values()) {
            total += b.stats().expirations();
        }
        return total;
    }
}
