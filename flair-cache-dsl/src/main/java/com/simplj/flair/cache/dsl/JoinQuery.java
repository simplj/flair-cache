package com.simplj.flair.cache.dsl;

import java.util.Collection;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Fluent builder for a two-source join query.
 *
 * <p><b>Immutable builder:</b> every chaining method returns a new {@code JoinQuery}
 * instance. The original instance is unchanged and may be safely stored as a
 * join template and reused across multiple calls with different strategies or filters.</p>
 *
 * <p>Two join strategies are available:
 * <ul>
 *   <li><b>Hash join O(n+m)</b> — call {@link #on(Function, Function)} when the join
 *       condition reduces to a simple key equality. This is the preferred strategy.</li>
 *   <li><b>Nested-loop O(n×m)</b> — call {@link #on(BiPredicate)} for arbitrary
 *       predicates; correct but slower on large data sets.</li>
 * </ul>
 *
 * <p>The two strategies are mutually exclusive — calling {@code on()} with one form
 * returns a new instance with the other strategy cleared.</p>
 *
 * <p>Joins are always single-threaded in V1.</p>
 *
 * <p>Call order: {@code on()} → {@code where()} (optional) → {@code select()} → {@code fetch()}.</p>
 */
public final class JoinQuery<L, R> {

    private final Collection<Object> leftRaw;
    private final Decoder<L>         leftDecoder;
    private final Collection<Object> rightRaw;
    private final Decoder<R>         rightDecoder;

    // Hash-join strategy fields — non-null only when hash-join was chosen via on(leftKey, rightKey)
    private final Function<L, Object> leftKeyFn;
    private final Function<R, Object> rightKeyFn;

    // Nested-loop strategy — non-null only when on(BiPredicate) was chosen
    private final BiPredicate<L, R> joinCondition;

    private final Predicate<L> leftFilter;

    @SuppressWarnings("unchecked")
    JoinQuery(Collection<?> leftRaw,  Decoder<L> leftDecoder,
              Collection<?> rightRaw, Decoder<R> rightDecoder) {
        this((Collection<Object>) leftRaw, leftDecoder,
             (Collection<Object>) rightRaw, rightDecoder,
             null, null, null, null);
    }

    private JoinQuery(Collection<Object> leftRaw,  Decoder<L> leftDecoder,
                      Collection<Object> rightRaw, Decoder<R> rightDecoder,
                      Predicate<L>        leftFilter,
                      BiPredicate<L, R>   joinCondition,
                      Function<L, Object> leftKeyFn,
                      Function<R, Object> rightKeyFn) {
        this.leftRaw      = leftRaw;
        this.leftDecoder  = leftDecoder;
        this.rightRaw     = rightRaw;
        this.rightDecoder = rightDecoder;
        this.leftFilter   = leftFilter;
        this.joinCondition = joinCondition;
        this.leftKeyFn    = leftKeyFn;
        this.rightKeyFn   = rightKeyFn;
    }

    // ── join strategy ──────────────────────────────────────────────────────────

    /**
     * Nested-loop join condition. Correct for any predicate; O(n×m).
     * Returns a new {@code JoinQuery} with this strategy set, clearing any
     * previously set hash-join strategy.
     * Prefer {@link #on(Function, Function)} for equality-key joins.
     */
    public JoinQuery<L, R> on(BiPredicate<L, R> condition) {
        if (condition == null) throw new IllegalArgumentException("condition must not be null");
        return new JoinQuery<>(leftRaw, leftDecoder, rightRaw, rightDecoder,
                leftFilter, condition, null, null);
    }

    /**
     * Hash join using key equality. O(n+m) — preferred over {@link #on(BiPredicate)}.
     * Returns a new {@code JoinQuery} with this strategy set, clearing any
     * previously set nested-loop strategy.
     */
    @SuppressWarnings("unchecked")
    public <K> JoinQuery<L, R> on(Function<L, K> leftKey, Function<R, K> rightKey) {
        if (leftKey == null || rightKey == null)
            throw new IllegalArgumentException("key extractors must not be null");
        return new JoinQuery<>(leftRaw, leftDecoder, rightRaw, rightDecoder,
                leftFilter, null,
                (Function<L, Object>) leftKey, (Function<R, Object>) rightKey);
    }

    // ── filters ───────────────────────────────────────────────────────────────

    /**
     * Filters left-side entries before the join is attempted.
     * Returns a new {@code JoinQuery} with the filter ANDed onto any existing filter.
     */
    public JoinQuery<L, R> where(Predicate<L> filter) {
        if (filter == null) throw new IllegalArgumentException("filter must not be null");
        Predicate<L> newFilter = leftFilter == null ? filter : leftFilter.and(filter);
        return new JoinQuery<>(leftRaw, leftDecoder, rightRaw, rightDecoder,
                newFilter, joinCondition, leftKeyFn, rightKeyFn);
    }

    // ── projection (pivot to terminal stage) ──────────────────────────────────

    /**
     * Projects each matched (left, right) pair into the output type {@code S}.
     * Returns a {@link JoinSelectQuery} whose {@link JoinSelectQuery#fetch()} executes the join.
     * The compiler enforces that {@code select()} must be called before {@code fetch()}.
     */
    public <S> JoinSelectQuery<L, R, S> select(BiFunction<L, R, S> projection) {
        return new JoinSelectQuery<>(leftRaw, leftDecoder, rightRaw, rightDecoder,
                leftFilter, joinCondition, leftKeyFn, rightKeyFn, projection);
    }
}
