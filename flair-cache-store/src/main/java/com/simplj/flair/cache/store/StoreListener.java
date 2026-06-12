package com.simplj.flair.cache.store;

/**
 * Callback interface for replication and watch layers to observe store mutations.
 * Eviction events (capacity-based removal) are delivered via {@link #onEvict} —
 * separate from explicit deletes so listeners can choose to ignore them.
 *
 * <p><strong>Contract:</strong> the {@code key} byte array passed to every callback is the
 * live backing array of the internal {@code ByteArrayKey} map key. Implementations
 * <em>must not</em> mutate it — doing so corrupts future key-equality checks in the store.</p>
 *
 * <p>Callbacks are invoked on the calling thread (for put/delete) or on
 * {@code flaircache-expiry-sweep} (for expire). Implementations must not block.</p>
 */
public interface StoreListener {

    void onPut(byte[] key, CacheEntry entry);

    void onDelete(byte[] key, CacheEntry entry);

    void onExpire(byte[] key, CacheEntry entry);

    default void onEvict(byte[] key, CacheEntry entry) {}
}
