package com.simplj.flair.cache.dsl;

import java.util.function.Predicate;

/**
 * Composable predicate tree used internally to represent accumulated filter conditions.
 * Avoids re-parsing lambda references on each execution.
 */
public sealed interface QueryPredicate<T>
        permits QueryPredicate.SimplePredicate,
                QueryPredicate.AndPredicate,
                QueryPredicate.OrPredicate,
                QueryPredicate.NotPredicate {

    boolean test(T value);

    static <T> QueryPredicate<T> of(Predicate<T> fn) {
        return new SimplePredicate<>(fn);
    }

    default QueryPredicate<T> and(QueryPredicate<T> other) {
        return new AndPredicate<>(this, other);
    }

    default QueryPredicate<T> or(QueryPredicate<T> other) {
        return new OrPredicate<>(this, other);
    }

    default QueryPredicate<T> not() {
        return new NotPredicate<>(this);
    }

    record SimplePredicate<T>(Predicate<T> fn) implements QueryPredicate<T> {
        public boolean test(T value) { return fn.test(value); }
    }

    record AndPredicate<T>(QueryPredicate<T> left, QueryPredicate<T> right) implements QueryPredicate<T> {
        public boolean test(T value) { return left.test(value) && right.test(value); }
    }

    record OrPredicate<T>(QueryPredicate<T> left, QueryPredicate<T> right) implements QueryPredicate<T> {
        public boolean test(T value) { return left.test(value) || right.test(value); }
    }

    record NotPredicate<T>(QueryPredicate<T> inner) implements QueryPredicate<T> {
        public boolean test(T value) { return !inner.test(value); }
    }
}
