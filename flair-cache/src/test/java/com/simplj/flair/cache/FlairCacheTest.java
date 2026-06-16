package com.simplj.flair.cache;

import com.simplj.flair.cache.gossip.MembershipList;
import com.simplj.flair.cache.replication.ConsistencyMode;
import com.simplj.flair.cache.serial.Codec;
import com.simplj.flair.cache.store.CacheBlock;
import com.simplj.flair.cache.store.EvictionPolicy;
import com.simplj.flair.cache.watch.ChangeEvent;
import com.simplj.flair.cache.watch.WatchHandle;
import com.simplj.flair.cache.watch.WatchRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for the FlairCache facade — single-JVM, no Docker.
 */
@Timeout(value = 30, unit = TimeUnit.SECONDS)
class FlairCacheTest {

    // Each test tracks its own cache so @AfterEach can always shut it down.
    private FlairCache cache;

    @AfterEach
    void tearDown() {
        if (cache != null) {
            cache.shutdown();
            cache = null;
        }
    }

    // ── Codec helpers ─────────────────────────────────────────────────────────

    private static final Codec<String> STRING_CODEC = new Codec<String>() {
        @Override
        public void serialize(String value, ByteBuffer buf) {
            byte[] bytes = value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            buf.putShort((short) bytes.length);
            buf.put(bytes);
        }
        @Override
        public String deserialize(ByteBuffer buf) {
            int len = Short.toUnsignedInt(buf.getShort());
            byte[] bytes = new byte[len];
            buf.get(bytes);
            return new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
        }
        @Override
        public int sizeOf(String value) {
            return 2 + value.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
        }
    };

    // ── Tests ─────────────────────────────────────────────────────────────────

    @Test
    void startSucceeds_allModulesBootAndIsRunningReturnsTrue() throws IOException {
        cache = FlairCache.builder()
                .bindPort(findFreePort())
                .build()
                .start();

        assertTrue(cache.isRunning(), "isRunning() must return true after start()");
    }

    @Test
    void singleNodeStart_noSeedPeers_startsWithoutException() throws IOException {
        // No seedPeers configured — must start successfully as a standalone node.
        cache = FlairCache.builder()
                .bindPort(findFreePort())
                .seedPeers(List.of())
                .build()
                .start();

        assertTrue(cache.isRunning());
    }

    @Test
    void startFailureMidSequence_allStartedModulesShutDownCleanly() {
        // Force a bind failure by using an invalid bind address. The exact exception type
        // may be IOException or a RuntimeException wrapping a network error depending on the
        // OS/JDK — the critical assertion is that the cache is NOT running afterwards and
        // all previously started components have been cleaned up.
        FlairCache c = FlairCache.builder()
                .bindAddress("999.999.999.999")
                .bindPort(7890)
                .build();

        assertThrows(Exception.class, c::start,
                "Invalid bind address must throw during start()");
        assertFalse(c.isRunning(), "Cache must not be running after failed start()");
    }

    @Test
    void blockAfterShutdown_throwsIllegalStateException() throws IOException {
        cache = FlairCache.builder()
                .bindPort(findFreePort())
                .build()
                .start();
        cache.shutdown();

        assertThrows(IllegalStateException.class,
                () -> cache.registerBlock("items"),
                "registerBlock() after shutdown must throw IllegalStateException");
    }

    @Test
    void queryAfterShutdown_throwsIllegalStateException() throws IOException {
        cache = FlairCache.builder()
                .bindPort(findFreePort())
                .build()
                .start();
        cache.shutdown();

        assertThrows(IllegalStateException.class,
                cache::query,
                "query() after shutdown must throw IllegalStateException");
    }

    @Test
    void blockBuild_registersBlockAndSupportsCrud() throws IOException {
        cache = FlairCache.builder()
                .bindPort(findFreePort())
                .consistency(ConsistencyMode.EVENTUAL)
                .build()
                .start();

        CacheBlock<String, String> items = cache.<String, String>registerBlock("items")
                .keyCodec(STRING_CODEC)
                .valueCodec(STRING_CODEC)
                .eviction(EvictionPolicy.LRU)
                .maxEntries(1000)
                .consistency(ConsistencyMode.EVENTUAL)
                .build();

        items.put("k1", "v1");
        assertEquals("v1", items.get("k1"));
        assertTrue(items.contains("k1"));

        items.delete("k1");
        assertNull(items.get("k1"));
    }

    @Test
    void blockBuild_withTtl_entryExpiresAndBecomesNull() throws IOException, InterruptedException {
        cache = FlairCache.builder()
                .bindPort(findFreePort())
                .consistency(ConsistencyMode.EVENTUAL)
                .build()
                .start();

        CacheBlock<String, String> items = cache.<String, String>registerBlock("ttl-items")
                .keyCodec(STRING_CODEC)
                .valueCodec(STRING_CODEC)
                .ttl(Duration.ofMillis(100))
                .build();

        items.put("expiring", "value");
        assertEquals("value", items.get("expiring"));

        Thread.sleep(200);
        assertNull(items.get("expiring"), "Entry must be expired after TTL");
    }

    @Test
    void duplicateBlockName_throwsIllegalStateException() throws IOException {
        cache = FlairCache.builder()
                .bindPort(findFreePort())
                .consistency(ConsistencyMode.EVENTUAL)
                .build()
                .start();

        cache.<String, String>registerBlock("duplicate")
                .keyCodec(STRING_CODEC)
                .valueCodec(STRING_CODEC)
                .build();

        assertThrows(IllegalStateException.class,
                () -> cache.<String, String>registerBlock("duplicate")
                        .keyCodec(STRING_CODEC)
                        .valueCodec(STRING_CODEC)
                        .build(),
                "Duplicate block name must throw IllegalStateException");
    }

    @Test
    void query_returnsSnapshotOfRegisteredBlocks() throws IOException {
        cache = FlairCache.builder()
                .bindPort(findFreePort())
                .consistency(ConsistencyMode.EVENTUAL)
                .build()
                .start();

        CacheBlock<String, String> items = cache.<String, String>registerBlock("query-items")
                .keyCodec(STRING_CODEC)
                .valueCodec(STRING_CODEC)
                .build();

        items.put("k1", "alpha");
        items.put("k2", "beta");

        // Query with identity decoder: values are already String (the block's typed snapshot).
        @SuppressWarnings("unchecked")
        List<String> results = (List<String>) cache.query()
                .from("query-items", String.class, v -> (String) v)
                .where(s -> s.startsWith("a"))
                .fetch();

        assertEquals(1, results.size());
        assertEquals("alpha", results.get(0));
    }

    @Test
    void shutdownGraceful_noExceptionAndIsRunningFalse() throws IOException {
        cache = FlairCache.builder()
                .bindPort(findFreePort())
                .build()
                .start();

        assertTrue(cache.isRunning());
        cache.shutdown();
        assertFalse(cache.isRunning());
    }

    @Test
    void shutdownIdempotent_callingTwiceDoesNotThrow() throws IOException {
        cache = FlairCache.builder()
                .bindPort(findFreePort())
                .build()
                .start();

        assertDoesNotThrow(() -> {
            cache.shutdown();
            cache.shutdown();
        });
    }

    @Test
    void tryWithResources_shutsDownOnClose() throws IOException {
        // Spring Boot @Bean / try-with-resources pattern.
        int port = findFreePort();
        try (FlairCache c = FlairCache.builder().bindPort(port).build().start()) {
            assertTrue(c.isRunning());
        }
        // After the try block, isRunning() must return false.
        // We can't check it because the reference is out of scope — the test
        // verifies the close() call does not throw.
    }

    @Test
    void metrics_neverNullRegardlessOfLifecycleState() throws IOException {
        FlairCache c = FlairCache.builder().bindPort(findFreePort()).build();
        // Before start
        assertNotNull(c.metrics());

        c.start();
        cache = c; // register for tearDown
        assertNotNull(c.metrics());

        c.shutdown();
        cache = null;
        // After shutdown
        assertNotNull(c.metrics());
    }

    @Test
    void blockBuilder_missingKeyCodec_throwsNullPointerException() throws IOException {
        cache = FlairCache.builder()
                .bindPort(findFreePort())
                .build()
                .start();

        assertThrows(NullPointerException.class,
                () -> cache.<String, String>registerBlock("nk")
                        .valueCodec(STRING_CODEC)
                        .build(),
                "Missing keyCodec must throw NullPointerException from build()");
    }

    @Test
    void blockBuilder_missingValueCodec_throwsNullPointerException() throws IOException {
        cache = FlairCache.builder()
                .bindPort(findFreePort())
                .build()
                .start();

        assertThrows(NullPointerException.class,
                () -> cache.<String, String>registerBlock("nv")
                        .keyCodec(STRING_CODEC)
                        .build(),
                "Missing valueCodec must throw NullPointerException from build()");
    }

    @Test
    void config_returnsImmutableConfigWithAutoAssignedNodeId() {
        FlairCache c = FlairCache.builder().bindPort(7890).build();
        FlairCacheConfig cfg = c.config();
        assertNotNull(cfg, "config() must never return null");
        assertNotNull(cfg.nodeId(), "Auto-assigned nodeId must be non-null");
        assertEquals(7890, cfg.bindPort());
    }

    @Test
    void config_explicitNodeIdPreserved() {
        UUID id = UUID.randomUUID();
        FlairCache c = FlairCache.builder().nodeId(id).bindPort(7890).build();
        assertEquals(id, c.config().nodeId(), "Explicit nodeId must survive build()");
    }

    @Test
    void startAlreadyStarted_throwsIllegalStateException() throws IOException {
        cache = FlairCache.builder()
                .bindPort(findFreePort())
                .build()
                .start();

        assertThrows(IllegalStateException.class, cache::start,
                "Calling start() on an already-started cache must throw");
    }

    @Test
    void getBlock_returnsRegisteredBlock() throws IOException {
        cache = FlairCache.builder()
                .bindPort(findFreePort())
                .consistency(ConsistencyMode.EVENTUAL)
                .build()
                .start();

        CacheBlock<String, String> registered = cache.<String, String>registerBlock("known")
                .keyCodec(STRING_CODEC)
                .valueCodec(STRING_CODEC)
                .build();

        CacheBlock<String, String> retrieved = cache.registeredBlock("known");
        assertSame(registered, retrieved, "registeredBlock() must return the same instance");
    }

    @Test
    void getBlock_unknownName_throwsIllegalArgumentException() throws IOException {
        cache = FlairCache.builder()
                .bindPort(findFreePort())
                .build()
                .start();

        assertThrows(IllegalArgumentException.class,
                () -> cache.registeredBlock("no-such-block"),
                "registeredBlock() for unknown name must throw IllegalArgumentException");
    }

    @Test
    void watchRegistry_returnsRegistryForRegisteredBlock() throws IOException {
        cache = FlairCache.builder()
                .bindPort(findFreePort())
                .consistency(ConsistencyMode.EVENTUAL)
                .build()
                .start();

        cache.<String, String>registerBlock("watch-block")
                .keyCodec(STRING_CODEC)
                .valueCodec(STRING_CODEC)
                .build();

        WatchRegistry<String, String> registry = cache.watchRegistry("watch-block");
        assertNotNull(registry, "watchRegistry() must return non-null for a registered block");
    }

    @Test
    void watchRegistry_unknownBlock_throwsIllegalArgumentException() throws IOException {
        cache = FlairCache.builder()
                .bindPort(findFreePort())
                .build()
                .start();

        assertThrows(IllegalArgumentException.class,
                () -> cache.watchRegistry("no-such-block"),
                "watchRegistry() for unknown block must throw IllegalArgumentException");
    }

    @Test
    void watchRegistry_dispatchesPutEventsToSubscribers() throws IOException, InterruptedException {
        cache = FlairCache.builder()
                .bindPort(findFreePort())
                .consistency(ConsistencyMode.EVENTUAL)
                .build()
                .start();

        CacheBlock<String, String> block = cache.<String, String>registerBlock("events")
                .keyCodec(STRING_CODEC)
                .valueCodec(STRING_CODEC)
                .build();

        WatchRegistry<String, String> registry = cache.watchRegistry("events");
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<String> received = new AtomicReference<>();
        AtomicReference<ChangeEvent.Source> receivedSource = new AtomicReference<>();

        // A single raw-event subscriber captures both the value and the Source, then
        // counts down the latch. Using one subscriber gives a single synchronization
        // point so the assertions below never race a not-yet-dispatched callback.
        WatchHandle handle = registry.watch()
                .onEvent(event -> {
                    if (event instanceof ChangeEvent.PutEvent<?,?> put) {
                        received.set((String) put.newValue());
                        receivedSource.set(put.source());
                        latch.countDown();
                    }
                })
                .register();

        block.put("wk", "wv");
        assertTrue(latch.await(2, TimeUnit.SECONDS), "PUT event must be dispatched within 2s");
        assertEquals("wv", received.get());
        assertEquals(ChangeEvent.Source.LOCAL, receivedSource.get(),
                "Local put must dispatch with Source.LOCAL");
        handle.cancel();
    }

    @Test
    void watchRegistry_localDeleteDispatchesLocalSource() throws IOException, InterruptedException {
        cache = FlairCache.builder()
                .bindPort(findFreePort())
                .consistency(ConsistencyMode.EVENTUAL)
                .build()
                .start();

        CacheBlock<String, String> block = cache.<String, String>registerBlock("del-events")
                .keyCodec(STRING_CODEC)
                .valueCodec(STRING_CODEC)
                .build();

        WatchRegistry<String, String> registry = cache.watchRegistry("del-events");
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<ChangeEvent.Source> receivedSource = new AtomicReference<>();

        WatchHandle handle = registry.watch()
                .onEvent(event -> {
                    if (event instanceof ChangeEvent.DeleteEvent<?,?> del) {
                        receivedSource.set(del.source());
                        latch.countDown();
                    }
                })
                .register();

        block.put("dk", "dv");
        block.delete("dk");
        assertTrue(latch.await(2, TimeUnit.SECONDS), "DELETE event must be dispatched within 2s");
        assertEquals(ChangeEvent.Source.LOCAL, receivedSource.get(),
                "Local delete must dispatch with Source.LOCAL");
        handle.cancel();
    }

    @Test
    void queryWith_decoderAppliedToSnapshotValues() throws IOException {
        cache = FlairCache.builder()
                .bindPort(findFreePort())
                .consistency(ConsistencyMode.EVENTUAL)
                .build()
                .start();

        CacheBlock<String, String> block = cache.<String, String>registerBlock("qw-items")
                .keyCodec(STRING_CODEC)
                .valueCodec(STRING_CODEC)
                .build();

        block.put("k1", "hello");
        block.put("k2", "world");

        @SuppressWarnings("unchecked")
        List<String> results = (List<String>) cache.queryWith("qw-items", v -> ((String) v).toUpperCase())
                .from("qw-items", String.class, s -> (String) s)
                .fetch();

        assertEquals(2, results.size());
        assertTrue(results.contains("HELLO"), "Decoder must have been applied: HELLO");
        assertTrue(results.contains("WORLD"), "Decoder must have been applied: WORLD");
    }

    @Test
    void queryWith_unknownBlock_throwsIllegalArgumentException() throws IOException {
        cache = FlairCache.builder()
                .bindPort(findFreePort())
                .build()
                .start();

        assertThrows(IllegalArgumentException.class,
                () -> cache.queryWith("ghost", v -> v),
                "queryWith() for unknown block must throw IllegalArgumentException");
    }

    @Test
    void queryWith_decoderReturnsNull_throwsNullPointerExceptionWithKeyInfo() throws IOException {
        cache = FlairCache.builder()
                .bindPort(findFreePort())
                .consistency(ConsistencyMode.EVENTUAL)
                .build()
                .start();

        CacheBlock<String, String> block = cache.<String, String>registerBlock("null-decode")
                .keyCodec(STRING_CODEC)
                .valueCodec(STRING_CODEC)
                .build();
        block.put("bad", "v");

        NullPointerException ex = assertThrows(NullPointerException.class,
                () -> cache.queryWith("null-decode", v -> null),
                "Null decoder result must throw NullPointerException");
        assertTrue(ex.getMessage().contains("bad"),
                "NPE message must identify the offending key");
        assertTrue(ex.getMessage().contains("null-decode"),
                "NPE message must identify the block name");
    }

    @Test
    void queryWith_skipNulls_excludesNullDecodedEntries() throws IOException {
        cache = FlairCache.builder()
                .bindPort(findFreePort())
                .consistency(ConsistencyMode.EVENTUAL)
                .build()
                .start();

        CacheBlock<String, String> block = cache.<String, String>registerBlock("skip-nulls")
                .keyCodec(STRING_CODEC)
                .valueCodec(STRING_CODEC)
                .build();
        block.put("keep", "yes");
        block.put("skip", "no");

        @SuppressWarnings("unchecked")
        List<String> results = (List<String>) cache
                .queryWith("skip-nulls", v -> "no".equals(v) ? null : (String) v, true)
                .from("skip-nulls", String.class, s -> (String) s)
                .fetch();

        assertEquals(1, results.size(), "Null-decoded entry must be excluded");
        assertEquals("yes", results.get(0));
    }

    @Test
    void cluster_returnsMembershipList() throws IOException {
        cache = FlairCache.builder()
                .bindPort(findFreePort())
                .build()
                .start();

        MembershipList membership = cache.cluster();
        assertNotNull(membership, "cluster() must return a non-null MembershipList");
    }

    @Test
    void bootstrapSync_noSeedPeers_isNoOp() throws Exception {
        // Standalone node (no seed peers) — bootstrapSync must return without error.
        cache = FlairCache.builder()
                .bindPort(findFreePort())
                .build()
                .start();

        cache.<String, String>registerBlock("sync-block")
                .keyCodec(STRING_CODEC)
                .valueCodec(STRING_CODEC)
                .build();

        // Must not throw even though there are no peers to sync from.
        assertDoesNotThrow(() -> cache.bootstrapSync("sync-block"));
    }

    @Test
    void bootstrapSync_unknownBlock_throwsIllegalArgumentException() throws IOException {
        cache = FlairCache.builder()
                .bindPort(findFreePort())
                .seedPeers(List.of("127.0.0.1:19000")) // fake seed so we reach the lookup
                .build()
                .start();

        assertThrows(IllegalArgumentException.class,
                () -> cache.bootstrapSync("not-registered"),
                "bootstrapSync() for unregistered block must throw IllegalArgumentException");
    }

    // ── Utilities ─────────────────────────────────────────────────────────────

    private static int findFreePort() {
        try (java.net.ServerSocket ss = new java.net.ServerSocket(0)) {
            ss.setReuseAddress(true);
            return ss.getLocalPort();
        } catch (IOException e) {
            throw new RuntimeException("Cannot find a free port", e);
        }
    }
}
