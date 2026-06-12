package com.simplj.flair.cache.store;

import com.simplj.flair.cache.hlc.HLCTimestamp;
import com.simplj.flair.cache.hlc.HybridLogicalClock;
import com.simplj.flair.cache.serial.codecs.ByteArrayCodec;
import com.simplj.flair.cache.serial.codecs.StringCodec;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class CacheBlockTest {

    // ── helpers ──────────────────────────────────────────────────────────────

    private CacheBlock<String, byte[]> block() {
        return CacheBlock.<String, byte[]>builder()
                .name("test")
                .keyCodec(StringCodec.INSTANCE)
                .valueCodec(ByteArrayCodec.INSTANCE)
                .build();
    }

    private CacheBlock<String, byte[]> blockWithTtl(Duration ttl) {
        return CacheBlock.<String, byte[]>builder()
                .name("ttl-test")
                .keyCodec(StringCodec.INSTANCE)
                .valueCodec(ByteArrayCodec.INSTANCE)
                .ttl(ttl)
                .sweepIntervalMs(100)
                .build();
    }

    private byte[] val(String s) {
        return s.getBytes();
    }

    // ── basic CRUD ────────────────────────────────────────────────────────────

    @Test
    void putAndGet() {
        try (CacheBlock<String, byte[]> b = block()) {
            b.put("k1", val("hello"));
            assertArrayEquals(val("hello"), b.get("k1"));
        }
    }

    @Test
    void getMissReturnsNull() {
        try (CacheBlock<String, byte[]> b = block()) {
            assertNull(b.get("absent"));
        }
    }

    @Test
    void containsAfterPut() {
        try (CacheBlock<String, byte[]> b = block()) {
            b.put("k", val("v"));
            assertTrue(b.contains("k"));
            assertFalse(b.contains("other"));
        }
    }

    @Test
    void deleteRemovesEntry() {
        try (CacheBlock<String, byte[]> b = block()) {
            b.put("k", val("v"));
            b.delete("k");
            assertNull(b.get("k"));
            assertFalse(b.contains("k"));
        }
    }

    @Test
    void clearEmptiesStore() {
        try (CacheBlock<String, byte[]> b = block()) {
            b.put("a", val("1"));
            b.put("b", val("2"));
            b.clear();
            assertNull(b.get("a"));
            assertNull(b.get("b"));
            assertEquals(0, b.stats().size());
        }
    }

    @Test
    void putAllAndSnapshot() {
        try (CacheBlock<String, byte[]> b = block()) {
            b.putAll(Map.of("x", val("1"), "y", val("2")));
            Map<String, byte[]> snap = b.snapshot();
            assertEquals(2, snap.size());
            assertArrayEquals(val("1"), snap.get("x"));
            assertArrayEquals(val("2"), snap.get("y"));
        }
    }

    @Test
    void overwriteUpdatesValue() {
        try (CacheBlock<String, byte[]> b = block()) {
            b.put("k", val("old"));
            b.put("k", val("new"));
            assertArrayEquals(val("new"), b.get("k"));
        }
    }

    // ── TTL expiry ────────────────────────────────────────────────────────────

    @Test
    void entryPresentBeforeTtl() {
        try (CacheBlock<String, byte[]> b = blockWithTtl(Duration.ofSeconds(5))) {
            b.put("k", val("v"));
            assertNotNull(b.get("k"));
        }
    }

    @Test
    void entryAbsentAfterTtlOnGet() throws Exception {
        try (CacheBlock<String, byte[]> b = blockWithTtl(Duration.ofMillis(50))) {
            b.put("k", val("v"));
            Thread.sleep(100);
            assertNull(b.get("k"));
        }
    }

    @Test
    void entryAbsentAfterTtlOnContains() throws Exception {
        try (CacheBlock<String, byte[]> b = blockWithTtl(Duration.ofMillis(50))) {
            b.put("k", val("v"));
            Thread.sleep(100);
            assertFalse(b.contains("k"));
        }
    }

    @Test
    void snapshotExcludesExpiredEntries() throws Exception {
        try (CacheBlock<String, byte[]> b = blockWithTtl(Duration.ofMillis(50))) {
            b.put("expired", val("x"));
            Thread.sleep(100);
            b.put("alive", val("y")); // added after expiry of "expired"
            Map<String, byte[]> snap = b.snapshot();
            assertFalse(snap.containsKey("expired"), "expired entry must not appear in snapshot");
            assertTrue(snap.containsKey("alive"));
        }
    }

    // ── LRU eviction ─────────────────────────────────────────────────────────

    @Test
    void lruEvictsLeastRecentlyAccessed() throws Exception {
        try (CacheBlock<String, byte[]> b = CacheBlock.<String, byte[]>builder()
                .name("lru")
                .keyCodec(StringCodec.INSTANCE)
                .valueCodec(ByteArrayCodec.INSTANCE)
                .eviction(EvictionPolicy.LRU)
                .maxEntries(3)
                .build()) {

            b.put("a", val("1"));
            Thread.sleep(LocalStore.LRU_UPDATE_GRANULARITY_MS + 50);
            b.put("b", val("2"));
            Thread.sleep(LocalStore.LRU_UPDATE_GRANULARITY_MS + 50);
            b.put("c", val("3"));

            // access "a" now to refresh its timestamp
            b.get("a");
            Thread.sleep(LocalStore.LRU_UPDATE_GRANULARITY_MS + 50);

            // put a 4th entry to trigger eviction — "b" should be evicted (least recently accessed)
            b.put("d", val("4"));

            // "b" is the oldest-accessed — it should have been evicted
            // (Approximate LRU — we verify the eviction happened and total is <= maxEntries)
            assertTrue(b.stats().size() <= 3, "size must not exceed maxEntries after eviction");
            assertNull(b.get("b"), "LRU victim 'b' should be evicted");
        }
    }

    // ── LFU eviction ─────────────────────────────────────────────────────────

    @Test
    void lfuEvictsLeastFrequentlyUsed() throws Exception {
        try (CacheBlock<String, byte[]> b = CacheBlock.<String, byte[]>builder()
                .name("lfu")
                .keyCodec(StringCodec.INSTANCE)
                .valueCodec(ByteArrayCodec.INSTANCE)
                .eviction(EvictionPolicy.LFU)
                .maxEntries(3)
                .sweepIntervalMs(10_000)
                .build()) {

            b.put("cold", val("0"));
            b.put("warm", val("0"));
            b.put("hot",  val("0"));

            // Drive hit counts — each get() after LRU_UPDATE_GRANULARITY_MS increments hitCount
            Thread.sleep(LocalStore.LRU_UPDATE_GRANULARITY_MS + 50);
            b.get("hot");
            Thread.sleep(LocalStore.LRU_UPDATE_GRANULARITY_MS + 50);
            b.get("hot");
            Thread.sleep(LocalStore.LRU_UPDATE_GRANULARITY_MS + 50);
            b.get("warm");
            Thread.sleep(LocalStore.LRU_UPDATE_GRANULARITY_MS + 50);

            // 4th put triggers eviction — a zero-hit entry ("cold" or the new one) should be evicted.
            // Verify high-frequency entries survive: both "warm" and "hot" must still be present.
            b.put("new", val("x"));

            assertTrue(b.stats().size() <= 3, "size must not exceed maxEntries after eviction");
            // "warm" (1 hit) and "hot" (2 hits) must survive; only a zero-hit entry is evicted
            assertNotNull(b.get("warm"), "warm (1 hit) must survive LFU eviction");
            assertNotNull(b.get("hot"),  "hot (2 hits) must survive LFU eviction");
        }
    }

    // ── onEvict callback ─────────────────────────────────────────────────────

    @Test
    void onEvictCallbackFired() {
        List<String> evicted = new ArrayList<>();

        try (CacheBlock<String, byte[]> b = CacheBlock.<String, byte[]>builder()
                .name("evict-cb")
                .keyCodec(StringCodec.INSTANCE)
                .valueCodec(ByteArrayCodec.INSTANCE)
                .eviction(EvictionPolicy.LRU)
                .maxEntries(2)
                .onEvict((k, v) -> evicted.add(k))
                .build()) {

            b.put("a", val("1"));
            b.put("b", val("2"));
            b.put("c", val("3")); // triggers eviction

            assertFalse(evicted.isEmpty(), "onEvict must fire when capacity is exceeded");
        }
    }

    // ── putRaw / getRaw ──────────────────────────────────────────────────────

    @Test
    void putRawAndGetRawRoundTrip() {
        try (CacheBlock<String, byte[]> b = block()) {
            HLCTimestamp ts = new HLCTimestamp(System.currentTimeMillis(), 0);
            byte[] key   = "rawkey".getBytes();
            byte[] value = val("rawvalue");
            UUID   nodeId = UUID.randomUUID();
            CacheEntry entry = new CacheEntry(value, ts, 0L, System.currentTimeMillis(), 0L, nodeId);

            b.putRaw(key, entry);

            CacheEntry fetched = b.getRaw(key);
            assertNotNull(fetched);
            assertArrayEquals(value, fetched.value());
            assertEquals(ts, fetched.hlc());
            assertEquals(nodeId, fetched.originNodeId());
        }
    }

    @Test
    void putRawAdvancesHlc() {
        HybridLogicalClock hlc = new HybridLogicalClock();
        try (CacheBlock<String, byte[]> b = CacheBlock.<String, byte[]>builder()
                .name("hlc-test")
                .keyCodec(StringCodec.INSTANCE)
                .valueCodec(ByteArrayCodec.INSTANCE)
                .hlc(hlc)
                .build()) {

            HLCTimestamp remote = new HLCTimestamp(System.currentTimeMillis() + 5_000, 0);
            CacheEntry entry = new CacheEntry(val("v"), remote, 0L, System.currentTimeMillis(), 0L, null);
            b.putRaw("rk".getBytes(), entry);

            // After update, hlc.now() must be at least as recent as remote
            HLCTimestamp next = hlc.now();
            assertTrue(next.compareTo(remote) > 0,
                    "HLC must advance past remote timestamp after putRaw");
        }
    }

    // ── Stats ────────────────────────────────────────────────────────────────

    @Test
    void statsCountHitsAndMisses() {
        try (CacheBlock<String, byte[]> b = block()) {
            b.put("k", val("v"));
            b.get("k");
            b.get("k");
            b.get("absent");

            CacheStats s = b.stats();
            assertEquals(2, s.hits());
            assertEquals(1, s.misses());
        }
    }

    @Test
    void hitRateComputed() {
        try (CacheBlock<String, byte[]> b = block()) {
            b.put("k", val("v"));
            b.get("k");   // hit
            b.get("none"); // miss

            double rate = b.stats().hitRate();
            assertEquals(50.0, rate, 0.001);
        }
    }

    @Test
    void hitRateZeroWhenNoAccesses() {
        try (CacheBlock<String, byte[]> b = block()) {
            assertEquals(0.0, b.stats().hitRate(), 0.0);
        }
    }

    // ── Thread safety ─────────────────────────────────────────────────────────

    @Test
    void concurrentPutAndGetNoCorruption() throws Exception {
        int threads = 16;
        int opsPerThread = 500;

        try (CacheBlock<String, byte[]> b = block()) {
            ExecutorService pool = Executors.newFixedThreadPool(threads);
            CountDownLatch ready = new CountDownLatch(threads);
            CountDownLatch done  = new CountDownLatch(threads);
            AtomicInteger errors = new AtomicInteger();

            for (int t = 0; t < threads; t++) {
                final int tid = t;
                pool.submit(() -> {
                    ready.countDown();
                    try { ready.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                    try {
                        for (int i = 0; i < opsPerThread; i++) {
                            String key = "t" + tid + "k" + i;
                            byte[] expected = (key + "-val").getBytes();
                            b.put(key, expected);
                            byte[] actual = b.get(key);
                            // value may be null if another thread evicted (unlikely — no eviction here)
                            if (actual != null) {
                                assertArrayEquals(expected, actual);
                            }
                        }
                    } catch (Throwable ex) {
                        errors.incrementAndGet();
                    } finally {
                        done.countDown();
                    }
                });
            }

            assertTrue(done.await(30, TimeUnit.SECONDS), "Timed out waiting for threads");
            pool.shutdown();
            assertEquals(0, errors.get(), "Concurrent put/get must not corrupt state");
        }
    }

    @Test
    void concurrentPutWithEvictionNoDeadlock() throws Exception {
        int threads = 8;

        try (CacheBlock<String, byte[]> b = CacheBlock.<String, byte[]>builder()
                .name("concurrent-evict")
                .keyCodec(StringCodec.INSTANCE)
                .valueCodec(ByteArrayCodec.INSTANCE)
                .eviction(EvictionPolicy.LRU)
                .maxEntries(50)
                .build()) {

            ExecutorService pool = Executors.newFixedThreadPool(threads);
            CountDownLatch done  = new CountDownLatch(threads);

            for (int t = 0; t < threads; t++) {
                final int tid = t;
                pool.submit(() -> {
                    try {
                        for (int i = 0; i < 200; i++) {
                            b.put("t" + tid + "i" + i, val("v"));
                        }
                    } finally {
                        done.countDown();
                    }
                });
            }

            assertTrue(done.await(30, TimeUnit.SECONDS), "Timed out — possible deadlock");
            pool.shutdown();
            // Approximate eviction: under concurrent puts size can briefly exceed limit before evictions settle
            assertTrue(b.stats().evictions() > 0, "Evictions must have occurred");
        }
    }

    // ── snapshot consistency ─────────────────────────────────────────────────

    @Test
    void snapshotNoConcurrentModificationException() throws Exception {
        try (CacheBlock<String, byte[]> b = block()) {
            for (int i = 0; i < 100; i++) b.put("k" + i, val("v"));

            CountDownLatch done = new CountDownLatch(1);
            Thread writer = new Thread(() -> {
                for (int i = 0; i < 500; i++) {
                    b.put("k" + i, val("updated"));
                }
                done.countDown();
            });
            writer.setDaemon(true);
            writer.start();

            // Snapshot while writer is running — must not throw
            assertDoesNotThrow(b::snapshot);
            done.await(5, TimeUnit.SECONDS);
        }
    }

    // ── StoreListener ────────────────────────────────────────────────────────

    @Test
    void storeListenerReceivesPutAndDelete() {
        List<String> events = new ArrayList<>();

        try (CacheBlock<String, byte[]> b = block()) {
            b.addListener(new StoreListener() {
                @Override public void onPut(byte[] key, CacheEntry entry)    { events.add("put"); }
                @Override public void onDelete(byte[] key, CacheEntry entry) { events.add("delete"); }
                @Override public void onExpire(byte[] key, CacheEntry entry) { events.add("expire"); }
            });

            b.put("k", val("v"));
            b.delete("k");

            assertEquals(List.of("put", "delete"), events);
        }
    }

    @Test
    void storeListenerReceivesExpire() throws Exception {
        List<String> events = new ArrayList<>();

        try (CacheBlock<String, byte[]> b = blockWithTtl(Duration.ofMillis(50))) {
            b.addListener(new StoreListener() {
                @Override public void onPut(byte[] key, CacheEntry entry)    {}
                @Override public void onDelete(byte[] key, CacheEntry entry) {}
                @Override public void onExpire(byte[] key, CacheEntry entry) { events.add("expire"); }
            });

            b.put("k", val("v"));
            Thread.sleep(150); // wait for sweep + lazy expiry

            // Either sweep or lazy get should have triggered onExpire
            b.get("k"); // trigger lazy expiry if sweep hasn't fired yet
            Thread.sleep(50);

            assertFalse(events.isEmpty(), "onExpire must be fired for TTL-expired entries");
        }
    }

    // ── SIZE_BASED eviction ──────────────────────────────────────────────────

    @Test
    void sizeBasedEvictsLargestValue() {
        try (CacheBlock<String, byte[]> b = CacheBlock.<String, byte[]>builder()
                .name("size-based")
                .keyCodec(StringCodec.INSTANCE)
                .valueCodec(ByteArrayCodec.INSTANCE)
                .eviction(EvictionPolicy.SIZE_BASED)
                .maxEntries(2)
                .build()) {

            b.put("small", new byte[10]);
            b.put("large", new byte[1000]);
            b.put("tiny",  new byte[1]); // triggers eviction — sampler sees all 3, "large" wins

            assertTrue(b.stats().evictions() > 0);
            assertNull(b.get("large"), "SIZE_BASED must evict the entry with the largest value");
        }
    }

    // ── Stats: expirations and evictions counters ────────────────────────────

    @Test
    void expirationCounterIncremented() throws Exception {
        try (CacheBlock<String, byte[]> b = blockWithTtl(Duration.ofMillis(50))) {
            b.put("k1", val("v"));
            b.put("k2", val("v"));
            Thread.sleep(200); // sweep fires at least once; both entries are expired

            // Trigger lazy expiry for any the sweep missed — conditional remove ensures
            // exactly one of sweep or lazy expiry wins per entry, so counter stays at 2.
            b.get("k1");
            b.get("k2");

            assertEquals(2, b.stats().expirations(),
                    "expirations counter must reflect exactly the two expired entries");
        }
    }

    @Test
    void evictionCounterIncremented() {
        try (CacheBlock<String, byte[]> b = CacheBlock.<String, byte[]>builder()
                .name("evict-ctr")
                .keyCodec(StringCodec.INSTANCE)
                .valueCodec(ByteArrayCodec.INSTANCE)
                .eviction(EvictionPolicy.LRU)
                .maxEntries(2)
                .build()) {

            b.put("a", val("1"));
            b.put("b", val("2"));
            b.put("c", val("3")); // triggers exactly one eviction

            assertEquals(1, b.stats().evictions());
        }
    }

    // ── StoreListener correctness ────────────────────────────────────────────

    @Test
    void throwingListenerDoesNotSilenceOthers() {
        List<String> received = new ArrayList<>();

        try (CacheBlock<String, byte[]> b = block()) {
            b.addListener(new StoreListener() {
                @Override public void onPut(byte[] key, CacheEntry entry)    { throw new RuntimeException("bad listener"); }
                @Override public void onDelete(byte[] key, CacheEntry entry) {}
                @Override public void onExpire(byte[] key, CacheEntry entry) {}
            });
            b.addListener(new StoreListener() {
                @Override public void onPut(byte[] key, CacheEntry entry)    { received.add("put"); }
                @Override public void onDelete(byte[] key, CacheEntry entry) {}
                @Override public void onExpire(byte[] key, CacheEntry entry) {}
            });

            assertDoesNotThrow(() -> b.put("k", val("v")),
                    "a throwing listener must not propagate to the put() caller");
            assertEquals(List.of("put"), received,
                    "subsequent listeners must still receive events after a preceding listener throws");
        }
    }

    @Test
    void multipleListenersAllReceiveEvents() {
        List<String> first  = new ArrayList<>();
        List<String> second = new ArrayList<>();

        try (CacheBlock<String, byte[]> b = block()) {
            b.addListener(new StoreListener() {
                @Override public void onPut(byte[] key, CacheEntry entry)    { first.add("put"); }
                @Override public void onDelete(byte[] key, CacheEntry entry) { first.add("delete"); }
                @Override public void onExpire(byte[] key, CacheEntry entry) {}
            });
            b.addListener(new StoreListener() {
                @Override public void onPut(byte[] key, CacheEntry entry)    { second.add("put"); }
                @Override public void onDelete(byte[] key, CacheEntry entry) { second.add("delete"); }
                @Override public void onExpire(byte[] key, CacheEntry entry) {}
            });

            b.put("k", val("v"));
            b.delete("k");

            assertEquals(List.of("put", "delete"), first);
            assertEquals(List.of("put", "delete"), second);
        }
    }

    @Test
    void deleteAbsentKeyFiresNoEvent() {
        List<String> events = new ArrayList<>();

        try (CacheBlock<String, byte[]> b = block()) {
            b.addListener(new StoreListener() {
                @Override public void onPut(byte[] key, CacheEntry entry)    {}
                @Override public void onDelete(byte[] key, CacheEntry entry) { events.add("delete"); }
                @Override public void onExpire(byte[] key, CacheEntry entry) {}
            });

            b.delete("nonexistent");
            assertTrue(events.isEmpty(), "delete on absent key must not fire onDelete");
        }
    }

    @Test
    void putRawFiresOnPut() {
        List<String> events = new ArrayList<>();

        try (CacheBlock<String, byte[]> b = block()) {
            b.addListener(new StoreListener() {
                @Override public void onPut(byte[] key, CacheEntry entry)    { events.add("put"); }
                @Override public void onDelete(byte[] key, CacheEntry entry) {}
                @Override public void onExpire(byte[] key, CacheEntry entry) {}
            });

            HLCTimestamp ts = new HLCTimestamp(System.currentTimeMillis(), 0);
            CacheEntry entry = new CacheEntry(val("v"), ts, 0L, System.currentTimeMillis(), 0L, null);
            b.putRaw("k".getBytes(), entry);

            assertEquals(List.of("put"), events, "putRaw must notify listeners via onPut");
        }
    }

    // ── Event ordering ───────────────────────────────────────────────────────

    @Test
    void putEventFiresBeforeEvictEvent() {
        List<String> events = new ArrayList<>();

        try (CacheBlock<String, byte[]> b = CacheBlock.<String, byte[]>builder()
                .name("ordering")
                .keyCodec(StringCodec.INSTANCE)
                .valueCodec(ByteArrayCodec.INSTANCE)
                .eviction(EvictionPolicy.LRU)
                .maxEntries(1)
                .build()) {

            b.put("first", val("1")); // fills capacity, no eviction yet
            b.addListener(new StoreListener() {
                @Override public void onPut(byte[] key, CacheEntry entry)    { events.add("put"); }
                @Override public void onDelete(byte[] key, CacheEntry entry) {}
                @Override public void onExpire(byte[] key, CacheEntry entry) {}
                @Override public void onEvict(byte[] key, CacheEntry entry)  { events.add("evict"); }
            });

            b.put("second", val("2")); // triggers eviction of "first"

            assertEquals(List.of("put", "evict"), events,
                    "onPut must fire before onEvict when a put causes an eviction");
        }
    }

    // ── getRaw bypasses expiry ───────────────────────────────────────────────

    @Test
    void getRawDoesNotApplyExpiryCheck() {
        try (CacheBlock<String, byte[]> b = block()) {
            long pastExpiry = System.currentTimeMillis() - 1; // already expired
            HLCTimestamp ts = new HLCTimestamp(System.currentTimeMillis(), 0);
            byte[] key = "k".getBytes();
            CacheEntry entry = new CacheEntry(val("v"), ts, pastExpiry, System.currentTimeMillis(), 0L, null);

            b.putRaw(key, entry);

            // getRaw must not apply expiry — the replication layer needs to read all entries
            assertNotNull(b.getRaw(key), "getRaw must return entries regardless of expiry");
        }
    }

    // ── Builder validation ───────────────────────────────────────────────────

    @Test
    void builderRequiresCodecs() {
        assertThrows(NullPointerException.class, () ->
                CacheBlock.<String, byte[]>builder()
                        .valueCodec(ByteArrayCodec.INSTANCE)
                        .build(),
                "build() must throw when keyCodec is missing");

        assertThrows(NullPointerException.class, () ->
                CacheBlock.<String, byte[]>builder()
                        .keyCodec(StringCodec.INSTANCE)
                        .build(),
                "build() must throw when valueCodec is missing");
    }
}
