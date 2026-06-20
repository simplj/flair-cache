package com.simplj.flair.cache.store;

import com.simplj.flair.cache.hlc.HLCTimestamp;
import com.simplj.flair.cache.hlc.HybridLogicalClock;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiFunction;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Raw {@code byte[] → CacheEntry} store. Thread-safe. No type information here —
 * typing lives in {@link CacheBlock}. Callers are responsible for key/value encoding.
 */
final class LocalStore {

    private static final Logger log = Logger.getLogger(LocalStore.class.getName());

    private static final int  EVICTION_SAMPLE_SIZE = 5;

    private final ConcurrentHashMap<ByteArrayKey, CacheEntry> store;

    private final HybridLogicalClock hlc;
    private final EvictionPolicy     policy;
    private final int                maxEntries; // 0 = unlimited
    // Null in standalone mode (no replication). When set, locally-written entries (via put())
    // are stamped with this node's UUID so LWW tiebreaking is consistent across all nodes:
    // every node compares the same two UUIDs rather than one node comparing against UUID(0,0).
    private final UUID               localNodeId;
    // CopyOnWriteArrayList: each list may race with the sweep thread (notifyExpire) or
    // with concurrent puts (notifyPut/notifyEvict) while addXxxListener() is called.
    private final List<PutListener>    putListeners    = new CopyOnWriteArrayList<>();
    private final List<DeleteListener> deleteListeners = new CopyOnWriteArrayList<>();
    private final List<ExpireListener> expireListeners = new CopyOnWriteArrayList<>();
    private final List<EvictListener>  evictListeners  = new CopyOnWriteArrayList<>();

    private final LongAdder hits        = new LongAdder();
    private final LongAdder misses      = new LongAdder();
    private final LongAdder evictions   = new LongAdder();
    private final LongAdder expirations = new LongAdder();

    LocalStore(HybridLogicalClock hlc, EvictionPolicy policy, int maxEntries) {
        this(hlc, policy, maxEntries, null);
    }

    LocalStore(HybridLogicalClock hlc, EvictionPolicy policy, int maxEntries, UUID localNodeId) {
        this.store       = new ConcurrentHashMap<>();
        this.hlc         = hlc;
        this.policy      = policy;
        this.maxEntries  = maxEntries;
        this.localNodeId = localNodeId;
    }

    void addPutListener(PutListener listener)       { putListeners.add(listener); }
    void addDeleteListener(DeleteListener listener) { deleteListeners.add(listener); }
    void addExpireListener(ExpireListener listener) { expireListeners.add(listener); }
    void addEvictListener(EvictListener listener)   { evictListeners.add(listener); }

    // ── Write path ──────────────────────────────────────────────────────────

    void put(ByteArrayKey key, byte[] value, long expiryEpochMs) {
        HLCTimestamp ts  = hlc.now();
        long         now = System.currentTimeMillis();
        CacheEntry entry = new CacheEntry(value, ts, expiryEpochMs, now, 0L, localNodeId);
        store.put(key, entry);
        notifyPut(key.data, entry); // notify BEFORE eviction: listeners see put → evict, not evict → put
        maybeEvict();
    }

    void putRaw(byte[] key, CacheEntry entry) {
        hlc.update(entry.hlc()); // keep local clock consistent with cluster
        ByteArrayKey bk = new ByteArrayKey(key);
        // accessEpochMs and hitCount are local metadata — wire decoders always set both to 0.
        // For new entries: stamp now / start at 0 so they are not immediately LRU/LFU candidates.
        // For existing entries: preserve both fields so eviction reflects actual local read
        // activity, not replication write frequency. A hot key whose value is updated by
        // replication must not lose its accumulated hit count or last-access timestamp.
        CacheEntry existing = store.get(bk);
        long accessMs  = existing != null ? existing.accessEpochMs() : System.currentTimeMillis();
        long hitCount  = existing != null ? existing.hitCount()      : 0L;
        CacheEntry stamped = new CacheEntry(
                entry.value(), entry.hlc(), entry.expiryEpochMs(), accessMs, hitCount, entry.originNodeId());
        store.put(bk, stamped);
        // No eviction triggered: replication/bootstrap may push store past maxEntries temporarily.
        // The caller controls the flow; evicting mid-sync would corrupt the sync.
        notifyPut(key, stamped);
    }

    /**
     * Atomically resolves a conflict and writes the winner. Used by the incoming replication
     * handler where two frames for the same key may be processed concurrently by different
     * worker threads. {@link ConcurrentHashMap#compute} serialises the read-modify-write on
     * each individual key, so LWW is applied consistently without a separate lock.
     *
     * <p>The HLC is always advanced with the incoming timestamp, even when the existing entry
     * wins. {@code notifyPut} is fired only when the incoming entry wins (a write actually occurred).
     * Callers are responsible for setting the INCOMING ThreadLocal flag around this call so that
     * PutListeners registered via the replication engine do not re-replicate the write.</p>
     *
     * @param resolver {@code (existing, incoming) → winner} — same contract as
     *                 {@code ConflictResolver.resolve}; passed as a {@link BiFunction} to keep
     *                 the store module free of replication types.
     */
    void putRawIfBetter(byte[] key, CacheEntry incoming,
                        BiFunction<CacheEntry, CacheEntry, CacheEntry> resolver) {
        hlc.update(incoming.hlc()); // always advance, even if incoming loses
        ByteArrayKey bk = new ByteArrayKey(key);
        CacheEntry[] written = {null};
        store.compute(bk, (k, existing) -> {
            CacheEntry winner = (existing == null) ? incoming : resolver.apply(existing, incoming);
            if (winner == existing) return existing; // existing won — no change
            long accessMs = existing != null ? existing.accessEpochMs() : System.currentTimeMillis();
            long hitCount = existing != null ? existing.hitCount() : 0L;
            CacheEntry stamped = new CacheEntry(
                    winner.value(), winner.hlc(), winner.expiryEpochMs(),
                    accessMs, hitCount, winner.originNodeId());
            written[0] = stamped;
            return stamped;
        });
        if (written[0] != null) {
            notifyPut(key, written[0]);
        }
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
            // Best-effort CAS: if a concurrent write has already replaced this entry,
            // replace() returns false and we skip — acceptable for LRU/LFU approximation.
            // Using replace() instead of compute() keeps the allocation outside the bucket lock.
            store.replace(key, entry, entry.withAccess(now));
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

    void updateClock(HLCTimestamp remote) {
        hlc.update(remote);
    }

    HLCTimestamp hlcNow() {
        return hlc.now();
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
    // Each notify method iterates only the listeners registered for that specific event type.

    private void notifyPut(byte[] key, CacheEntry entry) {
        for (PutListener l : putListeners) {
            try { l.onPut(key, entry); } catch (Exception ex) { log.log(Level.WARNING, "onPut listener threw", ex); }
        }
    }

    private void notifyDelete(byte[] key, CacheEntry entry) {
        for (DeleteListener l : deleteListeners) {
            try { l.onDelete(key, entry); } catch (Exception ex) { log.log(Level.WARNING, "onDelete listener threw", ex); }
        }
    }

    private void notifyExpire(byte[] key, CacheEntry entry) {
        for (ExpireListener l : expireListeners) {
            try { l.onExpire(key, entry); } catch (Exception ex) { log.log(Level.WARNING, "onExpire listener threw", ex); }
        }
    }

    private void notifyEvict(byte[] key, CacheEntry entry) {
        for (EvictListener l : evictListeners) {
            try { l.onEvict(key, entry); } catch (Exception ex) { log.log(Level.WARNING, "onEvict listener threw", ex); }
        }
    }
}
