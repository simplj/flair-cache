package com.simplj.flair.cache.store;

/**
 * Callback fired after an entry is explicitly removed via {@code delete()} on a {@link CacheBlock},
 * but only when the key was actually present. Silent deletes of absent keys do not fire this event.
 *
 * <p><strong>Contract:</strong> the {@code key} byte array is the live backing array of the
 * internal map key. Do not mutate it — doing so corrupts future key equality checks in the store.</p>
 *
 * <p>Invoked on the calling thread. Must not block.</p>
 */
@FunctionalInterface
public interface DeleteListener {
    void onDelete(byte[] key, CacheEntry entry);
}
