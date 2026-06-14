package com.simplj.flair.cache.store;

/**
 * Callback fired after every successful {@code put()} or {@code putRaw()} on a {@link CacheBlock}.
 *
 * <p><strong>Contract:</strong> the {@code key} byte array is the live backing array of the
 * internal map key. Do not mutate it — doing so corrupts future key equality checks in the store.</p>
 *
 * <p>Invoked on the calling thread. Must not block.</p>
 */
@FunctionalInterface
public interface PutListener {
    void onPut(byte[] key, CacheEntry entry);
}
