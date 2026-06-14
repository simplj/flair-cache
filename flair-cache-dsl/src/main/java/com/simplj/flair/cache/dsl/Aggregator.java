package com.simplj.flair.cache.dsl;

import java.util.stream.Stream;

/**
 * Reduces a stream of group members to a single aggregate value.
 * Used with {@link GroupByQuery#aggregate(Aggregator)}.
 * Built-in implementations are available via {@link Aggregators}.
 */
@FunctionalInterface
public interface Aggregator<T, R> {

    R aggregate(Stream<T> items);
}
