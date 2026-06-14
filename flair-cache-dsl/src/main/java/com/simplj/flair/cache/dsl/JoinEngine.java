package com.simplj.flair.cache.dsl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Executes join operations between two decoded collections.
 * Hash join (O(n+m)) is used when key extractors are available.
 * Nested-loop join (O(n×m)) is the fallback for arbitrary BiPredicate conditions.
 */
final class JoinEngine {

    private JoinEngine() {}

    /**
     * Hash join — O(n+m). Build a HashMap over the right side keyed by {@code rightKey},
     * then probe with each left element.
     *
     * <p>Null keys never match (SQL NULL semantics): right entries with a null extracted
     * key are excluded from the index, and left entries with a null extracted key are
     * skipped during the probe phase.</p>
     */
    static <L, R, S> List<S> hashJoin(
            List<L> left,
            List<R> right,
            Function<L, Object> leftKey,
            Function<R, Object> rightKey,
            Predicate<L> leftFilter,
            BiFunction<L, R, S> selector) {

        // Capacity that avoids a resize: n / loadFactor (0.75) + 1, clamped to int range.
        int capacity = (int) Math.min((long)(right.size() / 0.75f) + 1, Integer.MAX_VALUE);
        HashMap<Object, List<R>> index = new HashMap<>(capacity);
        for (R r : right) {
            Object k = rightKey.apply(r);
            if (k == null) continue;  // null keys never match (SQL semantics)
            index.computeIfAbsent(k, ignored -> new ArrayList<>()).add(r);
        }

        // Probe phase
        List<S> results = new ArrayList<>();
        for (L l : left) {
            if (leftFilter != null && !leftFilter.test(l)) continue;
            Object lk = leftKey.apply(l);
            if (lk == null) continue;  // null keys never match (SQL semantics)
            List<R> matches = index.get(lk);
            if (matches == null) continue;
            for (R r : matches) {
                results.add(selector.apply(l, r));
            }
        }
        return results;
    }

    /**
     * Nested-loop join — O(n×m). Used when the join condition is an opaque BiPredicate
     * and no key extractor can be derived.
     */
    static <L, R, S> List<S> nestedLoopJoin(
            List<L> left,
            List<R> right,
            BiPredicate<L, R> joinCondition,
            Predicate<L> leftFilter,
            BiFunction<L, R, S> selector) {

        List<S> results = new ArrayList<>();
        for (L l : left) {
            if (leftFilter != null && !leftFilter.test(l)) continue;
            for (R r : right) {
                if (joinCondition.test(l, r)) {
                    results.add(selector.apply(l, r));
                }
            }
        }
        return results;
    }
}
