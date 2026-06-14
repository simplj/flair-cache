package com.simplj.flair.cache.store;

/**
 * Callback fired when an entry is removed due to TTL expiry — either by lazy expiry inside
 * {@code get()} / {@code contains()}, or by the background {@code flaircache-expiry-sweep} thread.
 *
 * <p><strong>Contract:</strong> the {@code key} byte array is the live backing array of the
 * internal map key. Do not mutate it — doing so corrupts future key equality checks in the store.</p>
 *
 * <p>When fired from lazy expiry, invoked on the calling thread. When fired from the sweep,
 * invoked on {@code flaircache-expiry-sweep}. Must not block in either case.</p>
 */
@FunctionalInterface
public interface ExpireListener {
    void onExpire(byte[] key, CacheEntry entry);
}
