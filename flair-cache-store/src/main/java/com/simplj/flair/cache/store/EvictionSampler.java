package com.simplj.flair.cache.store;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Approximate LRU/LFU/size-based eviction via random sampling.
 * Samples a small constant number of entries to keep eviction O(1).
 */
final class EvictionSampler {

    private EvictionSampler() {}

    /**
     * Selects the worst eviction candidate from a random sample of {@code sampleSize} entries.
     * Returns null if the store is empty.
     */
    static ByteArrayKey sample(ConcurrentHashMap<ByteArrayKey, CacheEntry> store,
                               EvictionPolicy policy, int sampleSize) {
        int size = store.size();
        if (size == 0) return null;

        // Random offset: ensure at least sampleSize entries remain after skip, bound to 16 for O(1)
        int maxSkip = Math.min(Math.max(0, size - sampleSize), 16);
        int skip = maxSkip == 0 ? 0 : ThreadLocalRandom.current().nextInt(maxSkip);

        ByteArrayKey victim = null;
        long lruScore   = Long.MAX_VALUE; // smallest accessEpochMs wins
        long lfuScore   = Long.MAX_VALUE; // smallest hitCount wins
        long sizeScore  = -1L;            // largest value byte length wins

        int seen  = 0;
        int count = 0;
        for (Map.Entry<ByteArrayKey, CacheEntry> e : store.entrySet()) {
            if (seen++ < skip) continue;
            if (count++ >= sampleSize) break;

            CacheEntry entry = e.getValue();
            switch (policy) {
                case LRU:
                    if (entry.accessEpochMs() < lruScore) {
                        lruScore = entry.accessEpochMs();
                        victim = e.getKey();
                    }
                    break;
                case LFU:
                    if (entry.hitCount() < lfuScore) {
                        lfuScore = entry.hitCount();
                        victim = e.getKey();
                    }
                    break;
                case SIZE_BASED:
                    long sz = entry.value() != null ? entry.value().length : 0L;
                    if (sz > sizeScore) {
                        sizeScore = sz;
                        victim = e.getKey();
                    }
                    break;
                default:
                    break;
            }
        }

        // Fallback: if skip left no entries to sample, grab the first available
        if (victim == null) {
            Iterator<Map.Entry<ByteArrayKey, CacheEntry>> it = store.entrySet().iterator();
            if (it.hasNext()) {
                victim = it.next().getKey();
            }
        }

        return victim;
    }
}
