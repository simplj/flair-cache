package com.simplj.flair.cache.it;

import com.simplj.flair.cache.FlairCache;
import com.simplj.flair.cache.FlairCluster;
import com.simplj.flair.cache.replication.ConsistencyMode;
import com.simplj.flair.cache.store.CacheBlock;
import com.simplj.flair.cache.watch.ChangeEvent;
import com.simplj.flair.cache.watch.WatchHandle;
import com.simplj.flair.cache.watch.WatchRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.simplj.flair.cache.it.ITSupport.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Scenario 10 — Watch: onPut, onDelete, and onExpire events fire with the correct payload
 * and correct {@link ChangeEvent.Source} (LOCAL vs REPLICATED).
 */
@Timeout(value = 30, unit = TimeUnit.SECONDS)
class WatchIT {

    private FlairCache cache;
    private FlairCluster cluster;

    @AfterEach
    void tearDown() {
        if (cache != null) { cache.shutdown(); cache = null; }
        if (cluster != null) { cluster.shutdown(); cluster = null; }
    }

    // ── onPut — LOCAL source ──────────────────────────────────────────────────

    @Test
    void onPut_localWrite_firesWithLocalSourceAndCorrectPayload() throws IOException, InterruptedException {
        cache = singleNode();

        CacheBlock<String, String> block = cache.<String, String>registerBlock("put-local")
                .keyCodec(STRING_CODEC)
                .valueCodec(STRING_CODEC)
                .build();

        WatchRegistry<String, String> reg = cache.watchRegistry("put-local");
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<String>              receivedKey   = new AtomicReference<>();
        AtomicReference<String>              receivedValue = new AtomicReference<>();
        AtomicReference<ChangeEvent.Source>  receivedSource = new AtomicReference<>();

        WatchHandle handle = reg.watch()
                .onEvent(ev -> {
                    if (ev instanceof ChangeEvent.PutEvent<?,?> put) {
                        receivedKey.set((String) put.key());
                        receivedValue.set((String) put.newValue());
                        receivedSource.set(put.source());
                        latch.countDown();
                    }
                })
                .register();

        block.put("watched-key", "watched-value");

        assertTrue(latch.await(3, TimeUnit.SECONDS), "onPut must fire within 3 s");
        assertEquals("watched-key",   receivedKey.get());
        assertEquals("watched-value", receivedValue.get());
        assertEquals(ChangeEvent.Source.LOCAL, receivedSource.get(),
                "local write must fire with Source.LOCAL");
        handle.cancel();
    }

    // ── onPut — REPLICATED source ─────────────────────────────────────────────

    @Test
    void onPut_replicatedWrite_firesWithReplicatedSource() throws IOException, InterruptedException {
        int basePort = freePort();
        cluster = FlairCluster.builder()
                .basePort(basePort)
                .nodes(2)
                .consistency(ConsistencyMode.EVENTUAL)
                .build()
                .start();

        CacheBlock<String, String> b0 = registerBlock(cluster.node(0), "rep-watch");
        CacheBlock<String, String> b1 = registerBlock(cluster.node(1), "rep-watch");

        WatchRegistry<String, String> reg1 = cluster.node(1).watchRegistry("rep-watch");
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<ChangeEvent.Source> receivedSource = new AtomicReference<>();

        WatchHandle handle = reg1.watch()
                .onEvent(ev -> {
                    if (ev instanceof ChangeEvent.PutEvent<?,?> put) {
                        receivedSource.set(put.source());
                        latch.countDown();
                    }
                })
                .register();

        // Write on node 0 — the event on node 1 must fire as REPLICATED.
        b0.put("rep-key", "rep-val");

        assertTrue(latch.await(5, TimeUnit.SECONDS), "replicated onPut must fire on node 1 within 5 s");
        assertEquals(ChangeEvent.Source.REPLICATED, receivedSource.get(),
                "replicated write must fire with Source.REPLICATED");
        handle.cancel();
    }

    // ── onDelete ──────────────────────────────────────────────────────────────

    @Test
    void onDelete_localDelete_firesWithCorrectKeyAndLocalSource() throws IOException, InterruptedException {
        cache = singleNode();

        CacheBlock<String, String> block = cache.<String, String>registerBlock("del-watch")
                .keyCodec(STRING_CODEC)
                .valueCodec(STRING_CODEC)
                .build();

        WatchRegistry<String, String> reg = cache.watchRegistry("del-watch");
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<String>             receivedKey    = new AtomicReference<>();
        AtomicReference<ChangeEvent.Source> receivedSource = new AtomicReference<>();

        WatchHandle handle = reg.watch()
                .onEvent(ev -> {
                    if (ev instanceof ChangeEvent.DeleteEvent<?,?> del) {
                        receivedKey.set((String) del.key());
                        receivedSource.set(del.source());
                        latch.countDown();
                    }
                })
                .register();

        block.put("del-key", "some-value");
        block.delete("del-key");

        assertTrue(latch.await(3, TimeUnit.SECONDS), "onDelete must fire within 3 s");
        assertEquals("del-key", receivedKey.get());
        assertEquals(ChangeEvent.Source.LOCAL, receivedSource.get(),
                "local delete must fire with Source.LOCAL");
        handle.cancel();
    }

    // ── onExpire ──────────────────────────────────────────────────────────────

    @Test
    void onExpire_ttlExpiry_firesWithCorrectKey() throws IOException, InterruptedException {
        cache = singleNode();

        CacheBlock<String, String> block = cache.<String, String>registerBlock("exp-watch")
                .keyCodec(STRING_CODEC)
                .valueCodec(STRING_CODEC)
                .ttl(Duration.ofMillis(100))
                .build();

        WatchRegistry<String, String> reg = cache.watchRegistry("exp-watch");
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<String> receivedKey = new AtomicReference<>();

        WatchHandle handle = reg.watch()
                .onEvent(ev -> {
                    if (ev instanceof ChangeEvent.ExpireEvent<?,?> exp) {
                        receivedKey.set((String) exp.key());
                        latch.countDown();
                    }
                })
                .register();

        block.put("expiring-key", "value");

        assertTrue(latch.await(3, TimeUnit.SECONDS),
                "onExpire must fire after TTL elapses — waited 3 s, TTL is 100 ms");
        assertEquals("expiring-key", receivedKey.get(),
                "onExpire event must carry the expired key");
        handle.cancel();
    }

    @Test
    void watch_cancelledHandle_noLongerReceivesEvents() throws IOException, InterruptedException {
        cache = singleNode();

        CacheBlock<String, String> block = cache.<String, String>registerBlock("cancel-watch")
                .keyCodec(STRING_CODEC)
                .valueCodec(STRING_CODEC)
                .build();

        WatchRegistry<String, String> reg = cache.watchRegistry("cancel-watch");
        CountDownLatch firstEvent  = new CountDownLatch(1);
        CountDownLatch secondEvent = new CountDownLatch(1);

        WatchHandle handle = reg.watch()
                .onPut((k, v) -> {
                    if ("k1".equals(k)) firstEvent.countDown();
                    if ("k2".equals(k)) secondEvent.countDown();
                })
                .register();

        block.put("k1", "v1");
        assertTrue(firstEvent.await(3, TimeUnit.SECONDS), "first event must fire");

        handle.cancel();
        assertFalse(handle.isActive(), "handle must be inactive after cancel");

        block.put("k2", "v2");
        // Give the dispatch thread time to process — the event must NOT fire on a cancelled handle.
        assertFalse(secondEvent.await(500, TimeUnit.MILLISECONDS),
                "cancelled handle must not receive further events");
    }

    @Test
    void watch_multipleSubscribers_allReceiveEvent() throws IOException, InterruptedException {
        cache = singleNode();

        CacheBlock<String, String> block = cache.<String, String>registerBlock("multi-watch")
                .keyCodec(STRING_CODEC)
                .valueCodec(STRING_CODEC)
                .build();

        WatchRegistry<String, String> reg = cache.watchRegistry("multi-watch");
        int subscriberCount = 5;
        CountDownLatch[] latches = new CountDownLatch[subscriberCount];
        WatchHandle[] handles = new WatchHandle[subscriberCount];

        for (int i = 0; i < subscriberCount; i++) {
            latches[i] = new CountDownLatch(1);
            final int idx = i;
            handles[i] = reg.watch()
                    .onPut((k, v) -> latches[idx].countDown())
                    .register();
        }

        block.put("broadcast", "value");

        for (int i = 0; i < subscriberCount; i++) {
            assertTrue(latches[i].await(3, TimeUnit.SECONDS),
                    "subscriber " + i + " must receive the event");
            handles[i].cancel();
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static FlairCache singleNode() throws IOException {
        return FlairCache.builder()
                .bindAddress("127.0.0.1")
                .bindPort(freePort())
                .consistency(ConsistencyMode.EVENTUAL)
                .build()
                .start();
    }

    private static CacheBlock<String, String> registerBlock(FlairCache node, String name) {
        return node.<String, String>registerBlock(name)
                .keyCodec(STRING_CODEC)
                .valueCodec(STRING_CODEC)
                .build();
    }
}
