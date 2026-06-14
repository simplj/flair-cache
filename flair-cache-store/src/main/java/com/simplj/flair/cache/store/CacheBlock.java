package com.simplj.flair.cache.store;

import com.simplj.flair.cache.hlc.HLCTimestamp;
import com.simplj.flair.cache.hlc.HybridLogicalClock;
import com.simplj.flair.cache.serial.Codec;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Typed, thread-safe in-memory cache block. Backed by {@link LocalStore}.
 * Each instance is independently configurable with a TTL, eviction policy,
 * and entry limit. Usable standalone as a fast embedded local cache.
 *
 * <p>Must be closed via {@link #close()} (or try-with-resources) to stop
 * the background expiry sweep thread.</p>
 */
public final class CacheBlock<K, V> implements AutoCloseable {

    private static final Logger log = Logger.getLogger(CacheBlock.class.getName());

    private static final int INITIAL_BUF_CAPACITY = 1024;

    private final String         name;
    private final Codec<K>       keyCodec;
    private final Codec<V>       valueCodec;
    private final long           ttlMs;      // 0 = immortal
    private final LocalStore     store;
    private final ExpiryManager  expiryManager;

    // Thread-local serialization buffer — reused across calls, grown when needed
    private final ThreadLocal<ByteBuffer> threadLocalBuf =
            ThreadLocal.withInitial(() -> ByteBuffer.allocate(INITIAL_BUF_CAPACITY));

    private CacheBlock(Builder<K, V> b) {
        this.name         = b.name;
        this.keyCodec     = b.keyCodec;
        this.valueCodec   = b.valueCodec;
        this.ttlMs        = b.ttlMs;

        HybridLogicalClock hlc = b.hlc != null ? b.hlc : new HybridLogicalClock();
        this.store = new LocalStore(hlc, b.eviction, b.maxEntries);

        if (b.userOnEvict != null) {
            BiConsumer<K, V> cb = b.userOnEvict;
            store.addEvictListener((key, entry) -> {
                try {
                    K k = keyCodec.deserialize(ByteBuffer.wrap(key));
                    V v = entry.value() != null
                            ? valueCodec.deserialize(ByteBuffer.wrap(entry.value()))
                            : null;
                    cb.accept(k, v);
                } catch (Exception ex) {
                    log.log(Level.WARNING, "onEvict callback threw", ex);
                }
            });
        }

        this.expiryManager = new ExpiryManager(store, b.sweepIntervalMs);
        if (b.ttlMs > 0) {
            expiryManager.start();
        }
        // If ttlMs == 0 (no TTL), the sweep is not started. Entries delivered via putRaw()
        // that carry a non-zero expiryEpochMs will still expire lazily on get()/contains(),
        // but will not be proactively reclaimed from memory until accessed.
    }

    // ── Public API ───────────────────────────────────────────────────────────

    public String name() {
        return name;
    }

    public void put(K key, V value) {
        Objects.requireNonNull(key,   "key must not be null");
        Objects.requireNonNull(value, "value must not be null");
        byte[]       keyBytes = serializeKey(key);
        byte[]       valBytes = serializeValue(value);
        long         expiry   = ttlMs > 0 ? System.currentTimeMillis() + ttlMs : 0L;
        store.put(new ByteArrayKey(keyBytes), valBytes, expiry);
    }

    public V get(K key) {
        Objects.requireNonNull(key, "key must not be null");
        byte[]       keyBytes = serializeKey(key);
        CacheEntry   entry    = store.get(new ByteArrayKey(keyBytes));
        if (entry == null) return null;
        return valueCodec.deserialize(ByteBuffer.wrap(entry.value()));
    }

    public boolean contains(K key) {
        Objects.requireNonNull(key, "key must not be null");
        byte[] keyBytes = serializeKey(key);
        return store.contains(new ByteArrayKey(keyBytes));
    }

    public void delete(K key) {
        Objects.requireNonNull(key, "key must not be null");
        byte[] keyBytes = serializeKey(key);
        store.delete(new ByteArrayKey(keyBytes));
    }

    public void clear() {
        store.clear();
    }

    public void putAll(Map<K, V> map) {
        Objects.requireNonNull(map, "map must not be null");
        long expiry = ttlMs > 0 ? System.currentTimeMillis() + ttlMs : 0L;
        for (Map.Entry<K, V> e : map.entrySet()) {
            if (e.getKey() == null || e.getValue() == null) continue;
            byte[] keyBytes = serializeKey(e.getKey());
            byte[] valBytes = serializeValue(e.getValue());
            store.put(new ByteArrayKey(keyBytes), valBytes, expiry);
        }
    }

    /**
     * Returns a point-in-time copy of all non-expired entries as raw byte arrays.
     * Intended for the bootstrap layer to stream the store contents over the wire.
     * Safe under concurrent writes — no ConcurrentModificationException.
     */
    public Map<byte[], CacheEntry> rawSnapshotEntries() {
        Map<ByteArrayKey, CacheEntry> raw = store.rawSnapshot();
        long now = System.currentTimeMillis();
        Map<byte[], CacheEntry> result = new HashMap<>(raw.size());
        for (Map.Entry<ByteArrayKey, CacheEntry> e : raw.entrySet()) {
            if (!e.getValue().isExpired(now)) {
                result.put(e.getKey().data, e.getValue());
            }
        }
        return result;
    }

    /**
     * Returns a point-in-time copy of all non-expired entries.
     * Safe under concurrent writes — no ConcurrentModificationException.
     */
    public Map<K, V> snapshot() {
        Map<ByteArrayKey, CacheEntry> raw = store.rawSnapshot();
        long now = System.currentTimeMillis();
        Map<K, V> result = new HashMap<>(raw.size());
        for (Map.Entry<ByteArrayKey, CacheEntry> e : raw.entrySet()) {
            if (!e.getValue().isExpired(now)) {
                K k = keyCodec.deserialize(ByteBuffer.wrap(e.getKey().data));
                V v = valueCodec.deserialize(ByteBuffer.wrap(e.getValue().value()));
                result.put(k, v);
            }
        }
        return result;
    }

    public CacheStats stats() {
        return store.stats();
    }

    // ── Raw access for replication layer ────────────────────────────────────

    public void putRaw(byte[] key, CacheEntry entry) {
        Objects.requireNonNull(key,   "key must not be null");
        Objects.requireNonNull(entry, "entry must not be null");
        store.putRaw(key, entry);
    }

    public CacheEntry getRaw(byte[] key) {
        Objects.requireNonNull(key, "key must not be null");
        return store.getRaw(key);
    }

    public void deleteRaw(byte[] key) {
        Objects.requireNonNull(key, "key must not be null");
        store.delete(new ByteArrayKey(key));
    }

    public void updateClock(HLCTimestamp remote) {
        Objects.requireNonNull(remote, "remote must not be null");
        store.updateClock(remote);
    }

    /** Returns the current HLC timestamp, advancing the clock. Used by the replication layer for DELETE events. */
    public HLCTimestamp hlcNow() {
        return store.hlcNow();
    }

    // ── Listener registration (for replication and watch layers) ────────────

    public void addPutListener(PutListener listener) {
        Objects.requireNonNull(listener, "listener must not be null");
        store.addPutListener(listener);
    }

    public void addDeleteListener(DeleteListener listener) {
        Objects.requireNonNull(listener, "listener must not be null");
        store.addDeleteListener(listener);
    }

    public void addExpireListener(ExpireListener listener) {
        Objects.requireNonNull(listener, "listener must not be null");
        store.addExpireListener(listener);
    }

    public void addEvictListener(EvictListener listener) {
        Objects.requireNonNull(listener, "listener must not be null");
        store.addEvictListener(listener);
    }

    // ── Lifecycle ────────────────────────────────────────────────────────────

    @Override
    public void close() {
        threadLocalBuf.remove(); // clean up calling thread's buffer entry
        expiryManager.stop();
    }

    // ── Serialization helpers ────────────────────────────────────────────────

    private ByteBuffer acquireBuffer(int required) {
        ByteBuffer buf = threadLocalBuf.get();
        if (buf.capacity() < required) {
            int cap = Integer.highestOneBit(required);
            if (cap < required) cap <<= 1;
            if (cap < 0) cap = Integer.MAX_VALUE; // overflow guard
            buf = ByteBuffer.allocate(cap);
            threadLocalBuf.set(buf);
        }
        buf.clear();
        return buf;
    }

    private byte[] serializeKey(K key) {
        int size = keyCodec.sizeOf(key);
        ByteBuffer buf = acquireBuffer(size);
        keyCodec.serialize(key, buf);
        buf.flip();
        byte[] bytes = new byte[buf.remaining()];
        buf.get(bytes);
        return bytes;
    }

    private byte[] serializeValue(V value) {
        int size = valueCodec.sizeOf(value);
        ByteBuffer buf = acquireBuffer(size);
        valueCodec.serialize(value, buf);
        buf.flip();
        byte[] bytes = new byte[buf.remaining()];
        buf.get(bytes);
        return bytes;
    }

    // ── Builder ──────────────────────────────────────────────────────────────

    public static <K, V> Builder<K, V> builder() {
        return new Builder<>();
    }

    public static final class Builder<K, V> {

        private String             name            = "default";
        private Codec<K>           keyCodec;
        private Codec<V>           valueCodec;
        private long               ttlMs           = 0L;
        private EvictionPolicy     eviction        = EvictionPolicy.NONE;
        private int                maxEntries      = 0;
        private BiConsumer<K, V>   userOnEvict;
        private long               sweepIntervalMs = ExpiryManager.DEFAULT_SWEEP_INTERVAL_MS;
        private HybridLogicalClock hlc;

        private Builder() {}

        public Builder<K, V> name(String name) {
            this.name = Objects.requireNonNull(name, "name must not be null");
            return this;
        }

        public Builder<K, V> keyCodec(Codec<K> keyCodec) {
            this.keyCodec = Objects.requireNonNull(keyCodec, "keyCodec must not be null");
            return this;
        }

        public Builder<K, V> valueCodec(Codec<V> valueCodec) {
            this.valueCodec = Objects.requireNonNull(valueCodec, "valueCodec must not be null");
            return this;
        }

        public Builder<K, V> ttl(Duration ttl) {
            Objects.requireNonNull(ttl, "ttl must not be null");
            if (ttl.isNegative()) throw new IllegalArgumentException("ttl must not be negative");
            this.ttlMs = ttl.toMillis();
            return this;
        }

        public Builder<K, V> eviction(EvictionPolicy eviction) {
            this.eviction = Objects.requireNonNull(eviction, "eviction must not be null");
            return this;
        }

        public Builder<K, V> maxEntries(int maxEntries) {
            if (maxEntries < 0) throw new IllegalArgumentException("maxEntries must be >= 0");
            this.maxEntries = maxEntries;
            return this;
        }

        public Builder<K, V> onEvict(BiConsumer<K, V> onEvict) {
            this.userOnEvict = onEvict;
            return this;
        }

        public Builder<K, V> sweepIntervalMs(long ms) {
            if (ms <= 0) throw new IllegalArgumentException("sweepIntervalMs must be > 0");
            this.sweepIntervalMs = ms;
            return this;
        }

        /** Inject a shared HLC (e.g. from the full FlairCache instance). */
        public Builder<K, V> hlc(HybridLogicalClock hlc) {
            this.hlc = hlc;
            return this;
        }

        public CacheBlock<K, V> build() {
            Objects.requireNonNull(keyCodec,   "keyCodec is required");
            Objects.requireNonNull(valueCodec, "valueCodec is required");
            if (eviction != EvictionPolicy.NONE && maxEntries == 0) {
                Logger.getLogger(CacheBlock.class.getName()).warning(
                        "CacheBlock '" + name + "': eviction policy " + eviction +
                        " is set but maxEntries is 0 — eviction will never fire. " +
                        "Call maxEntries(N) to enable eviction.");
            }
            return new CacheBlock<>(this);
        }
    }
}
