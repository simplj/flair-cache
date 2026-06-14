package com.simplj.flair.cache.dsl;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.logging.Logger;

/**
 * Entry point for the FlairCache Query DSL.
 *
 * <p>Create an engine over a single map or a named {@link DataRegistry}, then build
 * queries with {@link #from}. The engine is stateless with respect to query results
 * and may be shared across threads.</p>
 *
 * <p>The dedicated parallel pool is loaded lazily on first use (not at class-load time).
 * If pool creation fails in a restricted environment (e.g., SecurityManager), a
 * single-thread fallback pool is used automatically — sequential queries are unaffected.</p>
 */
public final class QueryEngine {

    /** Default entry count above which {@code .parallel()} is recommended (not enforced). */
    public static final int DEFAULT_PARALLEL_THRESHOLD = 50_000;

    // Lazy holder — PoolHolder is only loaded when PoolHolder.INSTANCE is first accessed.
    // This prevents a static-initializer failure from making QueryEngine permanently unloadable.
    // If ForkJoinPool construction fails, a single-thread fallback keeps parallel() functional.
    private static final class PoolHolder {
        static final ForkJoinPool INSTANCE;
        static {
            int parallelism = Math.max(1, Runtime.getRuntime().availableProcessors());
            ForkJoinPool pool;
            try {
                pool = new ForkJoinPool(
                        parallelism,
                        p -> {
                            ForkJoinWorkerThread t =
                                    ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(p);
                            t.setName("flaircache-user-" + t.getPoolIndex());
                            t.setDaemon(true);
                            return t;
                        },
                        null, false);
            } catch (Exception e) {
                Logger.getLogger(QueryEngine.class.getName())
                      .warning("DSL parallel pool creation failed; falling back to single-thread pool: " + e);
                pool = new ForkJoinPool(1);
            }
            INSTANCE = pool;
        }
    }

    private static final String SINGLE_SOURCE_KEY = "__";

    private final DataRegistry registry;
    private final boolean      singleMode;

    private QueryEngine(DataRegistry registry, boolean singleMode) {
        this.registry   = registry;
        this.singleMode = singleMode;
    }

    // ── factory methods ───────────────────────────────────────────────────────

    /**
     * Creates an engine backed by a single map's values.
     * The {@code name} argument in subsequent {@link #from} calls is ignored.
     * Join operations are not supported in single-source mode.
     */
    public static <K, V> QueryEngine over(Map<K, V> singleMap) {
        DataRegistry reg = new DataRegistry();
        reg.register(SINGLE_SOURCE_KEY, singleMap);
        return new QueryEngine(reg, true);
    }

    /**
     * Creates an engine backed by a named {@link DataRegistry}.
     * Supports queries across multiple named sources and join operations.
     */
    public static QueryEngine over(DataRegistry registry) {
        return new QueryEngine(registry, false);
    }

    // ── query builder ─────────────────────────────────────────────────────────

    /**
     * Starts a query against a named data source.
     *
     * <p>In single-source mode (created via {@link #over(Map)}), {@code name} is a
     * documentation label only; the single registered source is always used.</p>
     *
     * @param name    data source name (looked up in the registry, or ignored in single mode)
     * @param type    target type used for generic inference
     * @param decoder converts raw source values to type {@code T};
     *                use {@link Decoder#identity()} for already-typed maps or
     *                {@link Decoder#typed(Class)} for safer type checking
     */
    public <T> SingleBlockQuery<T> from(String name, Class<T> type, Decoder<T> decoder) {
        String key = singleMode ? SINGLE_SOURCE_KEY : name;
        Collection<Object> data = registry.getCollection(key);
        return new SingleBlockQuery<>(singleMode ? null : registry, data, decoder, PoolHolder.INSTANCE);
    }
}
