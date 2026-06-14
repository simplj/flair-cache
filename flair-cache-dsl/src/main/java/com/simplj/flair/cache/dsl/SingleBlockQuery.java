package com.simplj.flair.cache.dsl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.ToDoubleFunction;
import java.util.logging.Logger;
import java.util.stream.Stream;

/**
 * Fluent query builder for a single data source.
 * All stream operations are lazy; nothing executes until a terminal method is called.
 *
 * <p><b>Immutable builder:</b> every chaining method returns a new {@code SingleBlockQuery}
 * instance with the updated constraint applied. The original instance is unchanged and
 * may be safely reused as a query template — including across threads.</p>
 *
 * <p><b>Caution:</b> {@link #orderBy} materialises the full filtered stream to sort.
 * Always pair with {@link #limit} in production to cap the sort cost.</p>
 */
public final class SingleBlockQuery<T> {

    private static final Logger LOG = Logger.getLogger(SingleBlockQuery.class.getName());

    // null when the engine was created via QueryEngine.over(Map) (single-source mode)
    private final DataRegistry       registry;
    private final Collection<Object> rawData;
    private final Decoder<T>         decoder;
    private final ForkJoinPool       parallelPool;

    private final QueryPredicate<T> predicate;
    private final Comparator<T>     ordering;
    private final int               limit;
    private final int               offset;
    private final boolean           parallel;

    SingleBlockQuery(DataRegistry registry, Collection<Object> rawData,
                     Decoder<T> decoder, ForkJoinPool parallelPool) {
        this(registry, rawData, decoder, parallelPool, null, null, Integer.MAX_VALUE, 0, false);
    }

    private SingleBlockQuery(DataRegistry registry, Collection<Object> rawData,
                              Decoder<T> decoder, ForkJoinPool parallelPool,
                              QueryPredicate<T> predicate, Comparator<T> ordering,
                              int limit, int offset, boolean parallel) {
        this.registry     = registry;
        this.rawData      = rawData;
        this.decoder      = decoder;
        this.parallelPool = parallelPool;
        this.predicate    = predicate;
        this.ordering     = ordering;
        this.limit        = limit;
        this.offset       = offset;
        this.parallel     = parallel;
    }

    // ── filter ────────────────────────────────────────────────────────────────

    /** Adds a filter predicate (ANDed with any existing predicate). */
    public SingleBlockQuery<T> where(Predicate<T> filter) {
        if (filter == null) throw new IllegalArgumentException("filter must not be null");
        QueryPredicate<T> newPred = predicate == null
                ? QueryPredicate.of(filter)
                : predicate.and(QueryPredicate.of(filter));
        return new SingleBlockQuery<>(registry, rawData, decoder, parallelPool,
                newPred, ordering, limit, offset, parallel);
    }

    /** ANDs an additional predicate onto the current filter. */
    public SingleBlockQuery<T> and(Predicate<T> filter) {
        return where(filter);   // null guard is in where()
    }

    /** ORs an additional predicate onto the current filter. */
    public SingleBlockQuery<T> or(Predicate<T> filter) {
        if (filter == null) throw new IllegalArgumentException("filter must not be null");
        QueryPredicate<T> newPred = predicate == null
                ? QueryPredicate.of(filter)
                : predicate.or(QueryPredicate.of(filter));
        return new SingleBlockQuery<>(registry, rawData, decoder, parallelPool,
                newPred, ordering, limit, offset, parallel);
    }

    // ── ordering / pagination ──────────────────────────────────────────────────

    /**
     * Sorts results by {@code keyExtractor}.
     * <b>This materialises the full filtered stream</b> — always use with {@link #limit}.
     *
     * <p>Null keys are treated as the minimum value: they sort before all non-null values
     * in {@link Order#ASC} order and after all non-null values in {@link Order#DESC} order.
     * Use {@link #orderBy(Function, Order, NullOrdering)} to control null placement explicitly.</p>
     */
    public <U extends Comparable<U>> SingleBlockQuery<T> orderBy(
            Function<T, U> keyExtractor, Order order) {
        return orderBy(keyExtractor, order, NullOrdering.NULLS_FIRST);
    }

    /**
     * Sorts results by {@code keyExtractor} with explicit control over where null keys land.
     * <b>This materialises the full filtered stream</b> — always use with {@link #limit}.
     *
     * @param nullOrdering {@link NullOrdering#NULLS_FIRST} places null keys before all
     *                     non-null values; {@link NullOrdering#NULLS_LAST} places them after
     */
    public <U extends Comparable<U>> SingleBlockQuery<T> orderBy(
            Function<T, U> keyExtractor, Order order, NullOrdering nullOrdering) {
        Comparator<U> base = nullOrdering == NullOrdering.NULLS_FIRST
                ? Comparator.nullsFirst(Comparator.naturalOrder())
                : Comparator.nullsLast(Comparator.naturalOrder());
        Comparator<T> cmp = Comparator.comparing(keyExtractor, base);
        Comparator<T> newOrdering = order == Order.DESC ? cmp.reversed() : cmp;
        return new SingleBlockQuery<>(registry, rawData, decoder, parallelPool,
                predicate, newOrdering, limit, offset, parallel);
    }

    public SingleBlockQuery<T> limit(int n) {
        if (n < 0) throw new IllegalArgumentException("limit must be >= 0, got: " + n);
        return new SingleBlockQuery<>(registry, rawData, decoder, parallelPool,
                predicate, ordering, n, offset, parallel);
    }

    public SingleBlockQuery<T> offset(int n) {
        if (n < 0) throw new IllegalArgumentException("offset must be >= 0, got: " + n);
        return new SingleBlockQuery<>(registry, rawData, decoder, parallelPool,
                predicate, ordering, limit, n, parallel);
    }

    // ── parallel ──────────────────────────────────────────────────────────────

    /**
     * Opts into parallel execution on the dedicated DSL worker pool.
     * Results are identical to sequential execution.
     * Joins and groupBy operations initiated from this query remain single-threaded.
     */
    public SingleBlockQuery<T> parallel() {
        return new SingleBlockQuery<>(registry, rawData, decoder, parallelPool,
                predicate, ordering, limit, offset, true);
    }

    // ── join / group-by pivots ─────────────────────────────────────────────────

    /**
     * Starts a join with a named right-side source from the {@link DataRegistry}.
     * Any {@code where()} predicates already chained on this query are forwarded
     * as the initial left-side filter of the join.
     *
     * @throws UnsupportedOperationException if this engine was created via
     *         {@link QueryEngine#over(java.util.Map)} (single-source mode)
     */
    public <R> JoinQuery<T, R> join(String rightName, Class<R> type, Decoder<R> rightDecoder) {
        if (registry == null) {
            throw new UnsupportedOperationException(
                    "join() requires a DataRegistry engine — use QueryEngine.over(DataRegistry)");
        }
        Collection<Object> rightRaw = registry.getCollection(rightName);
        JoinQuery<T, R> jq = new JoinQuery<>(rawData, decoder, rightRaw, rightDecoder);
        return predicate != null ? jq.where(predicate::test) : jq;
    }

    /**
     * Partitions the current result set by {@code keyExtractor}.
     * Any previously chained {@code where()} predicates are respected.
     * Because this instance is immutable, the lambda captures a frozen snapshot of
     * all query state — subsequent chaining on the original query has no effect here.
     *
     * <p><b>V1 limitation:</b> {@link #parallel()} is intentionally ignored here to avoid
     * consuming {@code ForkJoinPool.commonPool()} — running a parallel stream inside
     * {@code GroupByQuery.aggregate()} outside the dedicated DSL pool context would break
     * the pool-isolation guarantee. Grouping always executes sequentially in V1.
     * Parallel groupBy on the dedicated pool is planned for V2.</p>
     */
    public <K> GroupByQuery<T, K> groupBy(Function<T, K> keyExtractor) {
        if (parallel) {
            LOG.warning("parallel() has no effect on groupBy() — groupBy() always executes "
                    + "sequentially in V1 to avoid consuming ForkJoinPool.commonPool(). "
                    + "Parallel groupBy on the dedicated DSL pool is planned for V2.");
        }
        return new GroupByQuery<>(() -> buildStream(false), keyExtractor);
    }

    // ── terminal operations ───────────────────────────────────────────────────

    /**
     * Executes the query and returns all matching entries.
     * When {@link #parallel()} was called the work runs on the dedicated DSL pool.
     */
    public List<T> fetch() {
        if (parallel) {
            return parallelPool.submit(() -> executeFetch(true)).join();
        }
        return executeFetch(false);
    }

    /**
     * Like {@link #fetch()} but also returns execution metadata.
     */
    public QueryResult<T> fetchResult() {
        long start = System.nanoTime();
        List<T> items = fetch();
        return new QueryResult<>(items, System.nanoTime() - start);
    }

    /**
     * Returns the count of entries matching the predicate, regardless of any
     * {@link #offset} or {@link #limit} constraints on this query.
     * Use this to display the total match count for pagination UIs
     * ("showing 10 of N matching results").
     * For a count that respects pagination see {@link #count()}.
     * Routes through the dedicated DSL pool when {@link #parallel()} was called.
     */
    public long countMatching() {
        if (predicate == null) return rawData.size();
        if (parallel) return parallelPool.submit(() -> buildStream(true).count()).join();
        return buildStream(false).count();
    }

    /**
     * Returns the count of matching entries after applying offset and limit.
     * Short-circuits to {@code O(1)} when no predicate and no pagination are set
     * (ordering does not affect cardinality and is ignored for this check).
     * Routes through the parallel pool when {@link #parallel()} was called.
     */
    public long count() {
        if (predicate == null && offset == 0 && limit == Integer.MAX_VALUE) {
            return rawData.size();
        }
        if (parallel) {
            return parallelPool.submit(() -> applyPagination(buildStream(true)).count()).join();
        }
        return applyPagination(buildStream(false)).count();
    }

    /**
     * Returns the first matching entry in encounter order after applying ordering,
     * offset, and limit, or {@link Optional#empty()} if the constrained window is empty.
     * Always executes sequentially — encounter order is preserved.
     * When parallel execution without ordering guarantees is acceptable, use {@link #findAny()}.
     */
    public Optional<T> findFirst() {
        Stream<T> s = buildStream(false);
        if (ordering != null) s = s.sorted(ordering);
        if (offset > 0)               s = s.skip(offset);
        if (limit < Integer.MAX_VALUE) s = s.limit(limit);
        return s.findFirst();
    }

    /**
     * Returns any entry matching the predicate, or {@link Optional#empty()} if nothing matches.
     * When {@link #parallel()} was called the search runs on the dedicated DSL pool — no
     * encounter-order guarantee. When {@link #parallel()} was not called this is equivalent
     * to {@link #findFirst()}.
     * Ordering, offset, and limit are not applied — use {@link #findFirst()} when those matter.
     */
    public Optional<T> findAny() {
        if (parallel) {
            return parallelPool.submit(() -> buildStream(true).findAny()).join();
        }
        return buildStream(false).findAny();
    }

    /**
     * Computes numeric summary statistics for {@code field} across the paginated,
     * filtered result set (offset and limit are respected).
     * Returns {@link SummaryStatistics#EMPTY} when nothing matches.
     * Routes through the dedicated DSL pool when {@link #parallel()} was called.
     */
    public SummaryStatistics summarize(ToDoubleFunction<T> field) {
        if (parallel) return parallelPool.submit(() -> doSummarize(field, true)).join();
        return doSummarize(field, false);
    }

    private SummaryStatistics doSummarize(ToDoubleFunction<T> field, boolean useParallel) {
        Stream<T> s = buildStream(useParallel);
        if (ordering != null) s = s.sorted(ordering);
        java.util.DoubleSummaryStatistics stats = applyPagination(s)
                .mapToDouble(field)
                .summaryStatistics();
        if (stats.getCount() == 0L) return SummaryStatistics.EMPTY;
        return new SummaryStatistics(stats.getCount(), stats.getSum(),
                stats.getMin(), stats.getMax(), stats.getAverage());
    }

    // ── internal ──────────────────────────────────────────────────────────────

    /** Returns the decoded, filtered stream — lazily. Does NOT apply ordering or pagination. */
    Stream<T> buildStream(boolean useParallel) {
        Stream<Object> base = useParallel ? rawData.parallelStream() : rawData.stream();
        Stream<T> decoded   = base.map(decoder::decode);
        return predicate != null ? decoded.filter(predicate::test) : decoded;
    }

    /** Applies offset and limit to an already-filtered stream. Does NOT apply ordering. */
    private Stream<T> applyPagination(Stream<T> s) {
        if (offset > 0)              s = s.skip(offset);
        if (limit < Integer.MAX_VALUE) s = s.limit(limit);
        return s;
    }

    private List<T> executeFetch(boolean useParallelStream) {
        Stream<T> s = buildStream(useParallelStream);
        if (ordering != null) s = s.sorted(ordering);
        s = applyPagination(s);

        // Ordering does not affect cardinality, so the size hint is valid whenever there
        // is no predicate (predicate == null means no filtering, count is deterministic).
        int cap = (predicate == null)
                ? Math.max(0, Math.min(rawData.size() - offset,
                        limit == Integer.MAX_VALUE ? rawData.size() : limit))
                : 16;
        List<T> result = new ArrayList<>(cap);
        // forEachOrdered preserves encounter order on sorted parallel streams
        s.forEachOrdered(result::add);
        return result;
    }
}
