package com.simplj.flair.cache.store;

import com.simplj.flair.cache.hlc.HLCTimestamp;
import com.simplj.flair.cache.hlc.HybridLogicalClock;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.LongAdder;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Raw {@code byte[] → CacheEntry} store. Thread-safe. No type information here —
 * typing lives in {@link CacheBlock}. Callers are responsible for key/value encoding.
 */
final class LocalStore {

    private static final Logger log = Logger.getLogger(LocalStore.class.getName());

    private static final int  EVICTION_SAMPLE_SIZE       = 5;
    static final long         LRU_UPDATE_GRANULARITY_MS  = 1_000L;

    private final ConcurrentHashMap<ByteArrayKey, CacheEntry> store;

    private final HybridLogicalClock hlc;
    private final EvictionPolicy     policy;
    private final int                maxEntries; // 0 = unlimited
    // CopyOnWriteArrayList: addListener() may race with sweep-thread calling notifyExpire()
    private final List<StoreListener> listeners = new CopyOnWriteArrayList<>();

    private final LongAdder hits        = new LongAdder();
    private final LongAdder misses      = new LongAdder();
    private final LongAdder evictions   = new LongAdder();
    private final LongAdder expirations = new LongAdder();

    LocalStore(HybridLogicalClock hlc, EvictionPolicy policy, int maxEntries) {
        this.store      = new ConcurrentHashMap<>();
        this.hlc        = hlc;
        this.policy     = policy;
        this.maxEntries = maxEntries;
    }

    void addListener(StoreListener listener) {
        listeners.add(listener);
    }

    // ── Write path ──────────────────────────────────────────────────────────

    void put(ByteArrayKey key, byte[] value, long expiryEpochMs) {
        HLCTimestamp ts  = hlc.now();
        long         now = System.currentTimeMillis();
        CacheEntry entry = new CacheEntry(value, ts, expiryEpochMs, now, 0L, null);
        store.put(key, entry);
        notifyPut(key.data, entry); // notify BEFORE eviction: listeners see put → evict, not evict → put
        maybeEvict();
    }

    void putRaw(byte[] key, CacheEntry entry) {
        hlc.update(entry.hlc()); // keep local clock consistent with cluster
        ByteArrayKey bk = new ByteArrayKey(key);
        store.put(bk, entry);
        // No eviction triggered: replication/bootstrap may push store past maxEntries temporarily.
        // The caller controls the flow; evicting mid-sync would corrupt the sync.
        notifyPut(key, entry);
    }

    // ── Read path (hot — must be sub-200ns) ─────────────────────────────────

    /**
     * Returns the entry, applying lazy expiry and lazy LRU/LFU access update.
     * Increments hit/miss counters.
     */
    CacheEntry get(ByteArrayKey key) {
        CacheEntry entry = store.get(key);
        if (entry == null) {
            misses.increment();
            return null;
        }
        long now = System.currentTimeMillis();
        if (entry.isExpired(now)) {
            if (store.remove(key, entry)) {
                expirations.increment();
                notifyExpire(key.data, entry);
            }
            misses.increment();
            return null;
        }
        if (policy == EvictionPolicy.LRU || policy == EvictionPolicy.LFU) {
            if (now - entry.accessEpochMs() > LRU_UPDATE_GRANULARITY_MS) {
                // Lazy update — acceptable race: concurrent updates converge to recent nowMs
                store.compute(key, (k, e) -> e == null ? null : e.withAccess(now));
            }
        }
        hits.increment();
        return entry; // value bytes unchanged by access update — safe to return original
    }

    /**
     * Raw read for the replication layer. No expiry check, no counter updates.
     */
    CacheEntry getRaw(byte[] key) {
        return store.get(new ByteArrayKey(key));
    }

    /**
     * Existence check with lazy expiry. Does not update access time or counters.
     */
    boolean contains(ByteArrayKey key) {
        CacheEntry entry = store.get(key);
        if (entry == null) return false;
        long now = System.currentTimeMillis();
        if (entry.isExpired(now)) {
            if (store.remove(key, entry)) {
                expirations.increment();
                notifyExpire(key.data, entry);
            }
            return false;
        }
        return true;
    }

    // ── Delete path ──────────────────────────────────────────────────────────

    void delete(ByteArrayKey key) {
        CacheEntry entry = store.remove(key);
        if (entry != null) {
            notifyDelete(key.data, entry);
        }
    }

    void clear() {
        store.clear();
    }

    // ── Expiry sweep (called by ExpiryManager on flaircache-expiry-sweep) ───

    void sweepExpired(long nowMs) {
        for (Map.Entry<ByteArrayKey, CacheEntry> e : store.entrySet()) {
            CacheEntry entry = e.getValue();
            if (entry.isExpired(nowMs)) {
                // Conditional remove: if get() already lazily expired this entry, remove() returns false
                // and we skip the counter/notification — prevents double-counting and double-notify.
                if (store.remove(e.getKey(), entry)) {
                    expirations.increment();
                    notifyExpire(e.getKey().data, entry);
                }
            }
        }
    }

    // ── Stats & snapshot ─────────────────────────────────────────────────────

    CacheStats stats() {
        return new CacheStats(hits.sum(), misses.sum(), evictions.sum(), expirations.sum(), store.size());
    }

    Map<ByteArrayKey, CacheEntry> rawSnapshot() {
        return new HashMap<>(store);
    }

    int size() {
        return store.size();
    }

    // ── Eviction ─────────────────────────────────────────────────────────────

    private void maybeEvict() {
        if (policy == EvictionPolicy.NONE || maxEntries <= 0) return;
        if (store.size() <= maxEntries) return;

        ByteArrayKey victim = EvictionSampler.sample(store, policy, EVICTION_SAMPLE_SIZE);
        if (victim == null) return;

        CacheEntry evicted = store.remove(victim);
        if (evicted != null) {
            evictions.increment();
            notifyEvict(victim.data, evicted);
        }
    }

    // ── Listener dispatch ────────────────────────────────────────────────────
    // Per-listener try/catch: one bad listener must not silently drop events for subsequent ones.

    private void notifyPut(byte[] key, CacheEntry entry) {
        for (StoreListener l : listeners) {
            try { l.onPut(key, entry); } catch (Exception ex) { log.log(Level.WARNING, "onPut listener threw", ex); }
        }
    }

    private void notifyDelete(byte[] key, CacheEntry entry) {
        for (StoreListener l : listeners) {
            try { l.onDelete(key, entry); } catch (Exception ex) { log.log(Level.WARNING, "onDelete listener threw", ex); }
        }
    }

    private void notifyExpire(byte[] key, CacheEntry entry) {
        for (StoreListener l : listeners) {
            try { l.onExpire(key, entry); } catch (Exception ex) { log.log(Level.WARNING, "onExpire listener threw", ex); }
        }
    }

    private void notifyEvict(byte[] key, CacheEntry entry) {
        for (StoreListener l : listeners) {
            try { l.onEvict(key, entry); } catch (Exception ex) { log.log(Level.WARNING, "onEvict listener threw", ex); }
        }
    }
}
