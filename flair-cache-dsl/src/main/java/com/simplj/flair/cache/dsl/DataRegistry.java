package com.simplj.flair.cache.dsl;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Named registry of in-memory data sources. Register any {@link Map} or
 * {@link Collection} under a string name, then pass the registry to
 * {@link QueryEngine#over(DataRegistry)} to query across multiple sources.
 *
 * <p>DSL has no knowledge of CacheBlock or any FLAIR store type.
 * The caller supplies their own Map or Collection.</p>
 *
 * <p>{@link #register} fails fast on duplicate names. Use {@link #registerOrReplace}
 * when intentionally refreshing a source (e.g., live data reload).</p>
 */
public final class DataRegistry {

    private final Map<String, Collection<?>> sources = new HashMap<>();

    /**
     * Registers a map's values under {@code name}. Keys are discarded;
     * only the value set is exposed to the query engine.
     *
     * <p><b>Live-view semantics:</b> the registry holds a live view of the map's
     * {@link Map#values()} collection. Subsequent mutations to the original map are
     * immediately visible in query results. Pass a defensive copy if snapshot
     * semantics are required: {@code register(name, new HashMap<>(data))}.</p>
     *
     * @throws IllegalStateException if a source is already registered under {@code name};
     *         use {@link #registerOrReplace} for intentional replacement
     */
    public <K, V> DataRegistry register(String name, Map<K, V> data) {
        if (sources.containsKey(name)) {
            throw new IllegalStateException(
                    "A source is already registered as: '" + name + "' — use registerOrReplace() to overwrite");
        }
        sources.put(name, data.values());
        return this;
    }

    /**
     * Registers a collection directly under {@code name}.
     *
     * @throws IllegalStateException if a source is already registered under {@code name};
     *         use {@link #registerOrReplace} for intentional replacement
     */
    public <V> DataRegistry register(String name, Collection<V> data) {
        if (sources.containsKey(name)) {
            throw new IllegalStateException(
                    "A source is already registered as: '" + name + "' — use registerOrReplace() to overwrite");
        }
        sources.put(name, data);
        return this;
    }

    /**
     * Registers or replaces a map's values under {@code name}.
     * Unlike {@link #register}, this method silently overwrites any existing source.
     * Use for intentional live data reloads.
     */
    public <K, V> DataRegistry registerOrReplace(String name, Map<K, V> data) {
        sources.put(name, data.values());
        return this;
    }

    /**
     * Registers or replaces a collection under {@code name}.
     * Unlike {@link #register}, this method silently overwrites any existing source.
     * Use for intentional live data reloads.
     */
    public <V> DataRegistry registerOrReplace(String name, Collection<V> data) {
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
