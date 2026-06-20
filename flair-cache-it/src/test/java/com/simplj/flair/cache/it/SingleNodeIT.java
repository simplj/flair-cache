package com.simplj.flair.cache.it;

import com.simplj.flair.cache.FlairCache;
import com.simplj.flair.cache.replication.ConsistencyMode;
import com.simplj.flair.cache.store.CacheBlock;
import com.simplj.flair.cache.store.EvictionPolicy;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static com.simplj.flair.cache.it.ITSupport.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Scenario 1 — Single node: put/get/delete/contains, TTL expiry, LRU eviction.
 * All operations are local — no network, no replication.
 */
@Timeout(value = 30, unit = TimeUnit.SECONDS)
class SingleNodeIT {

    private FlairCache cache;

    @AfterEach
    void tearDown() {
        if (cache != null) {
            cache.shutdown();
            cache = null;
        }
    }

    @Test
    void putGetDeleteContains_operationsAreCorrect() throws IOException {
        cache = FlairCache.builder()
                .bindAddress("127.0.0.1")
                .bindPort(freePort())
                .consistency(ConsistencyMode.EVENTUAL)
                .build()
                .start();

        CacheBlock<String, String> block = cache.<String, String>registerBlock("crud")
                .keyCodec(STRING_CODEC)
                .valueCodec(STRING_CODEC)
                .build();

        assertNull(block.get("missing"), "get on absent key must return null");
        assertFalse(block.contains("missing"), "contains on absent key must return false");

        block.put("k1", "v1");
        assertEquals("v1", block.get("k1"));
        assertTrue(block.contains("k1"));

        block.put("k1", "v2");
        assertEquals("v2", block.get("k1"), "overwrite must update the value");

        block.delete("k1");
        assertNull(block.get("k1"), "get after delete must return null");
        assertFalse(block.contains("k1"), "contains after delete must return false");

        // Multiple independent keys
        block.put("a", "alpha");
        block.put("b", "beta");
        block.put("c", "gamma");
        assertEquals("alpha", block.get("a"));
        assertEquals("beta",  block.get("b"));
        assertEquals("gamma", block.get("c"));

        block.delete("b");
        assertNull(block.get("b"));
        assertEquals("alpha", block.get("a"), "sibling keys unaffected by delete");
        assertEquals("gamma", block.get("c"), "sibling keys unaffected by delete");
    }

    @Test
    void ttlExpiry_entryBecomesNullAfterTtlElapsed() throws IOException, InterruptedException {
        cache = FlairCache.builder()
                .bindAddress("127.0.0.1")
                .bindPort(freePort())
                .consistency(ConsistencyMode.EVENTUAL)
                .build()
                .start();

        CacheBlock<String, String> block = cache.<String, String>registerBlock("ttl")
                .keyCodec(STRING_CODEC)
                .valueCodec(STRING_CODEC)
                .ttl(Duration.ofMillis(100))
                .build();

        block.put("expiring", "value");
        assertEquals("value", block.get("expiring"), "entry must be present immediately");

        Thread.sleep(300); // wait well beyond 100ms TTL
        assertNull(block.get("expiring"), "entry must be expired after TTL");
        assertFalse(block.contains("expiring"), "contains must also reflect expiry");
    }

    @Test
    void ttlExpiry_entryWithNoTtlNeverExpires() throws IOException, InterruptedException {
        cache = FlairCache.builder()
                .bindAddress("127.0.0.1")
                .bindPort(freePort())
                .consistency(ConsistencyMode.EVENTUAL)
                .build()
                .start();

        // No TTL on the block — entries are immortal
        CacheBlock<String, String> block = cache.<String, String>registerBlock("no-ttl")
                .keyCodec(STRING_CODEC)
                .valueCodec(STRING_CODEC)
                .build();

        block.put("immortal", "stays");
        Thread.sleep(150);
        assertEquals("stays", block.get("immortal"), "no-TTL entry must not expire");
    }

    @Test
    void lruEviction_oldestEntryEvictedWhenCapacityExceeded() throws IOException, InterruptedException {
        cache = FlairCache.builder()
                .bindAddress("127.0.0.1")
                .bindPort(freePort())
                .consistency(ConsistencyMode.EVENTUAL)
                .build()
                .start();

        CacheBlock<String, String> block = cache.<String, String>registerBlock("lru")
                .keyCodec(STRING_CODEC)
                .valueCodec(STRING_CODEC)
                .eviction(EvictionPolicy.LRU)
                .maxEntries(3)
                .build();

        // Put entries with distinct millisecond timestamps. LRU tracks accessEpochMs, which is
        // set to System.currentTimeMillis() at the moment of put() for new entries. A 10ms gap
        // ensures k1.accessMs < k2.accessMs < k3.accessMs even on low-resolution clocks.
        block.put("k1", "v1");
        Thread.sleep(10);
        block.put("k2", "v2");
        Thread.sleep(10);
        block.put("k3", "v3");

        // Insert a 4th entry — k1 has the oldest accessMs and must be evicted.
        block.put("k4", "v4");

        assertNotNull(block.get("k2"), "k2 must survive eviction");
        assertNotNull(block.get("k3"), "k3 must survive eviction");
        assertNotNull(block.get("k4"), "k4 (just inserted) must survive");
        assertNull(block.get("k1"), "k1 has the oldest access time — must be the LRU eviction victim");
    }
}
