package com.simplj.flair.cache;

import com.simplj.flair.cache.replication.ConsistencyMode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

@Timeout(value = 30, unit = TimeUnit.SECONDS)
class FlairCacheFactoryTest {

    @AfterEach
    void tearDown() {
        // Reset the static registry after every test so tests are isolated.
        FlairCacheFactory.shutdownAll();
    }

    // ── get ───────────────────────────────────────────────────────────────────

    @Test
    void get_unknownName_throwsIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class,
                () -> FlairCacheFactory.get("no-such-cache"),
                "get() for unregistered name must throw");
    }

    @Test
    void get_returnsRegisteredInstance() throws IOException {
        FlairCache cache = FlairCache.builder()
                .bindPort(findFreePort())
                .consistency(ConsistencyMode.EVENTUAL)
                .build()
                .start();
        FlairCacheFactory.getOrCreate("myCache", () -> cache);

        assertSame(cache, FlairCacheFactory.get("myCache"));
    }

    // ── getOrCreate ───────────────────────────────────────────────────────────

    @Test
    void getOrCreate_firstCall_invokesSupplier() throws IOException {
        AtomicInteger callCount = new AtomicInteger();
        FlairCacheFactory.getOrCreate("counted", () -> {
            callCount.incrementAndGet();
            try {
                return FlairCache.builder()
                        .bindPort(findFreePort())
                        .consistency(ConsistencyMode.EVENTUAL)
                        .build()
                        .start();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        assertEquals(1, callCount.get(), "Supplier must be called exactly once on first access");
    }

    @Test
    void getOrCreate_subsequentCalls_returnSameInstanceWithoutInvokingSupplier() throws IOException {
        AtomicInteger callCount = new AtomicInteger();
        FlairCache first = FlairCacheFactory.getOrCreate("stable", () -> {
            callCount.incrementAndGet();
            try {
                return FlairCache.builder()
                        .bindPort(findFreePort())
                        .consistency(ConsistencyMode.EVENTUAL)
                        .build()
                        .start();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        FlairCache second  = FlairCacheFactory.getOrCreate("stable", () -> { throw new AssertionError("must not be called"); });
        FlairCache third   = FlairCacheFactory.getOrCreate("stable", () -> { throw new AssertionError("must not be called"); });

        assertSame(first, second);
        assertSame(first, third);
        assertEquals(1, callCount.get(), "Supplier must not be called again after first registration");
    }

    @Test
    void getOrCreate_nullSupplierReturn_throwsNullPointerException() {
        assertThrows(NullPointerException.class,
                () -> FlairCacheFactory.getOrCreate("null-cache", () -> null),
                "Null supplier return must throw NullPointerException");
    }

    @Test
    void getOrCreate_nullSupplier_throwsNullPointerException() {
        assertThrows(NullPointerException.class,
                () -> FlairCacheFactory.getOrCreate("x", null),
                "Null supplier must throw NullPointerException");
    }

    // ── shutdownAndRemove ─────────────────────────────────────────────────────

    @Test
    void shutdownAndRemove_registeredInstance_returnsTrueAndShutsDown() throws IOException {
        FlairCacheFactory.getOrCreate("to-remove", () -> {
            try {
                return FlairCache.builder()
                        .bindPort(findFreePort())
                        .consistency(ConsistencyMode.EVENTUAL)
                        .build()
                        .start();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        boolean removed = FlairCacheFactory.shutdownAndRemove("to-remove");

        assertTrue(removed, "shutdownAndRemove must return true when instance was found");
        assertEquals(0, FlairCacheFactory.size());
        // Subsequent get must no longer find it.
        assertThrows(IllegalArgumentException.class, () -> FlairCacheFactory.get("to-remove"));
    }

    @Test
    void shutdownAndRemove_unknownName_returnsFalse() {
        boolean removed = FlairCacheFactory.shutdownAndRemove("ghost");
        assertFalse(removed, "shutdownAndRemove must return false when no instance is registered");
    }

    // ── shutdownAll ───────────────────────────────────────────────────────────

    @Test
    void shutdownAll_shutsDownAllInstancesAndClearsRegistry() throws IOException {
        for (int i = 0; i < 3; i++) {
            final int port = findFreePort();
            FlairCacheFactory.getOrCreate("cache-" + i, () -> {
                try {
                    return FlairCache.builder()
                            .bindPort(port)
                            .consistency(ConsistencyMode.EVENTUAL)
                            .build()
                            .start();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }
        assertEquals(3, FlairCacheFactory.size());

        FlairCacheFactory.shutdownAll();

        assertEquals(0, FlairCacheFactory.size(), "Registry must be empty after shutdownAll()");
    }

    @Test
    void shutdownAll_emptyRegistry_isNoOp() {
        assertDoesNotThrow(FlairCacheFactory::shutdownAll,
                "shutdownAll() on empty registry must not throw");
    }

    // ── size ─────────────────────────────────────────────────────────────────

    @Test
    void size_reflectsCurrentRegistrationCount() throws IOException {
        assertEquals(0, FlairCacheFactory.size());

        FlairCacheFactory.getOrCreate("a", () -> {
            try { return FlairCache.builder().bindPort(findFreePort()).build().start(); }
            catch (IOException e) { throw new RuntimeException(e); }
        });
        assertEquals(1, FlairCacheFactory.size());

        FlairCacheFactory.getOrCreate("b", () -> {
            try { return FlairCache.builder().bindPort(findFreePort()).build().start(); }
            catch (IOException e) { throw new RuntimeException(e); }
        });
        assertEquals(2, FlairCacheFactory.size());

        FlairCacheFactory.shutdownAndRemove("a");
        assertEquals(1, FlairCacheFactory.size());
    }

    // ── Utilities ─────────────────────────────────────────────────────────────

    private static int findFreePort() {
        try (ServerSocket ss = new ServerSocket(0)) {
            ss.setReuseAddress(true);
            return ss.getLocalPort();
        } catch (IOException e) {
            throw new RuntimeException("Cannot find a free port", e);
        }
    }
}
