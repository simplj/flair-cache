package com.simplj.flair.cache.dsl;

import java.util.Collection;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Fluent builder for a two-source join query.
 *
 * <p>Two join strategies are available:
 * <ul>
 *   <li><b>Hash join O(n+m)</b> — call {@link #on(Function, Function)} when the join
 *       condition reduces to a simple key equality. This is the preferred strategy.</li>
 *   <li><b>Nested-loop O(n×m)</b> — call {@link #on(BiPredicate)} for arbitrary
 *       predicates; correct but slower on large data sets.</li>
 * </ul>
 *
 * <p>The two strategies are mutually exclusive — calling {@code on()} a second time
 * with a different form replaces the previous strategy entirely.</p>
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

    // Hash-join strategy — populated via on(leftKey, rightKey); null when nested-loop chosen
    private Function<L, Object> leftKeyFn;
    private Function<R, Object> rightKeyFn;

    // Nested-loop strategy — populated via on(BiPredicate); null when hash-join chosen
    private BiPredicate<L, R> joinCondition;

    private Predicate<L> leftFilter;

    @SuppressWarnings("unchecked")
    JoinQuery(Collection<?> leftRaw,  Decoder<L> leftDecoder,
              Collection<?> rightRaw, Decoder<R> rightDecoder) {
        this.leftRaw      = (Collection<Object>) leftRaw;
        this.leftDecoder  = leftDecoder;
        this.rightRaw     = (Collection<Object>) rightRaw;
        this.rightDecoder = rightDecoder;
    }

    // ── join strategy ──────────────────────────────────────────────────────────

    /**
     * Nested-loop join condition. Correct for any predicate; O(n×m).
     * Replaces any previously set hash-join strategy.
     * Prefer {@link #on(Function, Function)} for equality-key joins.
     */
    public JoinQuery<L, R> on(BiPredicate<L, R> condition) {
        this.joinCondition = condition;
        this.leftKeyFn     = null;   // strategies are mutually exclusive
        this.rightKeyFn    = null;
        return this;
    }

    /**
     * Hash join using key equality. O(n+m) — preferred over {@link #on(BiPredicate)}.
     * Replaces any previously set nested-loop strategy.
     */
    @SuppressWarnings("unchecked")
    public <K> JoinQuery<L, R> on(Function<L, K> leftKey, Function<R, K> rightKey) {
        this.leftKeyFn     = (Function<L, Object>) leftKey;
        this.rightKeyFn    = (Function<R, Object>) rightKey;
        this.joinCondition = null;   // strategies are mutually exclusive
        return this;
    }

    // ── filters ───────────────────────────────────────────────────────────────

    /** Filters left-side entries before the join is attempted. */
    public JoinQuery<L, R> where(Predicate<L> filter) {
        this.leftFilter = leftFilter == null ? filter : leftFilter.and(filter);
        return this;
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
