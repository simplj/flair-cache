package com.simplj.flair.cache.dsl;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Named registry of in-memory data sources. Register any {@link Map} or
 * {@link Collection} under a string name, then pass the registry to
 * {@link QueryEngine#over(DataRegistry)} to query across multiple sources.
 *
 * <p>DSL has no knowledge of CacheBlock or any FLAIR store type.
 * The caller supplies their own Map or Collection.</p>
 *
 * <p>{@link #register} is fully atomic with respect to duplicate-name detection —
 * two concurrent callers racing to register the same name are guaranteed that
 * exactly one succeeds and the other receives {@link IllegalStateException}.</p>
 *
 * <p>{@link #registerOrReplace} is individually atomic (the put itself is atomic),
 * but a read-then-replace sequence across two calls is not; external synchronisation
 * is required if that level of atomicity is needed.</p>
 *
 * <p>Queries executed concurrently against a stable registry (no concurrent registrations
 * in flight) are fully thread-safe.</p>
 */
public final class DataRegistry {

    private final ConcurrentHashMap<String, Collection<?>> sources = new ConcurrentHashMap<>();

    /**
     * Atomically registers a map's values under {@code name}. Keys are discarded;
     * only the value set is exposed to the query engine.
     *
     * <p><b>Live-view semantics:</b> the registry holds a live view of the map's
     * {@link Map#values()} collection. Subsequent mutations to the original map are
     * immediately visible in query results. Pass a defensive copy if snapshot
     * semantics are required: {@code register(name, new HashMap<>(data))}.</p>
     *
     * @throws IllegalStateException if a source is already registered under {@code name};
     *         use {@link #registerOrReplace} for intentional replacement.
     *         Guaranteed atomic — concurrent callers on the same name will not both succeed.
     */
    public <K, V> DataRegistry register(String name, Map<K, V> data) {
        if (name == null) throw new IllegalArgumentException("name must not be null");
        if (data == null) throw new IllegalArgumentException("data must not be null");
        Collection<?> prev = sources.putIfAbsent(name, data.values());
        if (prev != null) {
            throw new IllegalStateException(
                    "A source is already registered as: '" + name + "' — use registerOrReplace() to overwrite");
        }
        return this;
    }

    /**
     * Atomically registers a collection directly under {@code name}.
     *
     * @throws IllegalStateException if a source is already registered under {@code name};
     *         use {@link #registerOrReplace} for intentional replacement.
     *         Guaranteed atomic — concurrent callers on the same name will not both succeed.
     */
    public <V> DataRegistry register(String name, Collection<V> data) {
        if (name == null) throw new IllegalArgumentException("name must not be null");
        if (data == null) throw new IllegalArgumentException("data must not be null");
        Collection<?> prev = sources.putIfAbsent(name, data);
        if (prev != null) {
            throw new IllegalStateException(
                    "A source is already registered as: '" + name + "' — use registerOrReplace() to overwrite");
        }
        return this;
    }

    /**
     * Registers or replaces a map's values under {@code name}.
     * Unlike {@link #register}, this method silently overwrites any existing source.
     * Use for intentional live data reloads.
     */
    public <K, V> DataRegistry registerOrReplace(String name, Map<K, V> data) {
        if (name == null) throw new IllegalArgumentException("name must not be null");
        if (data == null) throw new IllegalArgumentException("data must not be null");
        sources.put(name, data.values());
        return this;
    }

    /**
     * Registers or replaces a collection under {@code name}.
     * Unlike {@link #register}, this method silently overwrites any existing source.
     * Use for intentional live data reloads.
     */
    public <V> DataRegistry registerOrReplace(String name, Collection<V> data) {
        if (name == null) throw new IllegalArgumentException("name must not be null");
        if (data == null) throw new IllegalArgumentException("data must not be null");
        sources.put(name, data);
        return this;
    }

    public boolean contains(String name) {
        return sources.containsKey(name);
    }

    @SuppressWarnings("unchecked")
    Collection<Object> getCollection(String name) {
        Collection<?> c = sources.get(name);
        if (c == null) throw new IllegalArgumentException("No data source registered as: '" + name + "'");
        return (Collection<Object>) c;
    }
}
