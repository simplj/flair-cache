package com.simplj.flair.cache.store;

/**
 * Callback fired after an entry is removed by the capacity eviction policy (LRU, LFU,
 * or SIZE_BASED). Only fires when {@code maxEntries} is set and the store exceeds that limit
 * after a {@code put()}.
 *
 * <p>Always fires <em>after</em> {@link PutListener#onPut} for the same {@code put()} call —
 * the new entry is fully committed before the eviction candidate is removed.</p>
 *
 * <p><strong>Contract:</strong> the {@code key} byte array is the live backing array of the
 * internal map key. Do not mutate it — doing so corrupts future key equality checks in the store.</p>
 *
 * <p>Invoked on the calling thread. Must not block.</p>
 */
@FunctionalInterface
public interface EvictListener {
    void onEvict(byte[] key, CacheEntry entry);
}
