package com.simplj.flair.cache;

import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Static registry of named {@link FlairCache} instances.
 *
 * <p>Intended for non-Spring projects where dependency injection is not available.
 * Any class in the application can retrieve a shared {@link FlairCache} by name
 * without passing the instance through constructors.</p>
 *
 * <pre>{@code
 * // At startup (once):
 * FlairCacheFactory.getOrCreate("orders",
 *     () -> FlairCache.builder().bindPort(7890).build().start());
 *
 * // Anywhere in the application:
 * FlairCache cache = FlairCacheFactory.get("orders");
 *
 * // At shutdown:
 * FlairCacheFactory.shutdownAll();
 * }</pre>
 *
 * <p>The factory is a registry only — it does not own the lifecycle of the instances
 * it holds. Startup and shutdown are the caller's responsibility; the factory merely
 * stores and retrieves references. {@link #shutdownAndRemove(String)} and
 * {@link #shutdownAll()} are provided as convenience methods that delegate
 * to {@link FlairCache#shutdown()} before deregistering.</p>
 *
 * <p>All methods are thread-safe.</p>
 */
public final class FlairCacheFactory {

    private static final Logger log = Logger.getLogger(FlairCacheFactory.class.getName());

    private static final ConcurrentHashMap<String, FlairCache> registry = new ConcurrentHashMap<>();

    private FlairCacheFactory() {}

    /**
     * Returns the {@link FlairCache} registered under {@code name}.
     *
     * @throws IllegalArgumentException if no instance is registered under {@code name}
     */
    public static FlairCache get(String name) {
        Objects.requireNonNull(name, "name must not be null");
        FlairCache cache = registry.get(name);
        if (cache == null) {
            throw new IllegalArgumentException(
                    "No FlairCache registered as: '" + name + "' — call getOrCreate() first");
        }
        return cache;
    }

    /**
     * Returns the {@link FlairCache} registered under {@code name}, creating and registering
     * it via {@code supplier} if no instance exists yet. The supplier is invoked at most once
     * per name, even under concurrent access.
     *
     * <p>The supplier is responsible for the full construction and startup sequence, e.g.:</p>
     * <pre>{@code
     * FlairCacheFactory.getOrCreate("orders",
     *     () -> FlairCache.builder().bindPort(7890).build().start());
     * }</pre>
     *
     * @throws NullPointerException  if {@code name} or {@code supplier} is null,
     *                               or if the supplier returns null
     */
    public static FlairCache getOrCreate(String name, Supplier<FlairCache> supplier) {
        Objects.requireNonNull(name,     "name must not be null");
        Objects.requireNonNull(supplier, "supplier must not be null");
        return registry.computeIfAbsent(name, k -> {
            FlairCache cache = supplier.get();
            return Objects.requireNonNull(cache,
                    "supplier returned null for name: '" + name + "'");
        });
    }

    /**
     * Shuts down the {@link FlairCache} registered under {@code name} and removes it
     * from the registry.
     *
     * @return {@code true} if an instance was found, shut down, and removed;
     *         {@code false} if no instance was registered under {@code name}
     */
    public static boolean shutdownAndRemove(String name) {
        Objects.requireNonNull(name, "name must not be null");
        FlairCache cache = registry.remove(name);
        if (cache == null) return false;
        try {
            cache.shutdown();
        } catch (Exception e) {
            log.log(Level.WARNING, "Error shutting down FlairCache '" + name + "'", e);
        }
        return true;
    }

    /**
     * Shuts down all registered {@link FlairCache} instances and clears the registry.
     * Instances are shut down in iteration order; errors from individual shutdowns are
     * logged but do not abort the remaining shutdowns.
     */
    public static void shutdownAll() {
        Iterator<Map.Entry<String, FlairCache>> it = registry.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, FlairCache> entry = it.next();
            it.remove(); // remove first so a concurrent get() sees it gone immediately
            try {
                entry.getValue().shutdown();
            } catch (Exception e) {
                log.log(Level.WARNING, "Error shutting down FlairCache '" + entry.getKey() + "'", e);
            }
        }
    }

    /** Returns the number of {@link FlairCache} instances currently registered. */
    public static int size() {
        return registry.size();
    }
}
