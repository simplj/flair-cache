package com.simplj.flair.cache.dsl;

import java.util.Comparator;
import java.util.function.Function;
import java.util.function.ToDoubleFunction;

/**
 * Factory methods for common {@link Aggregator} implementations.
 */
public final class Aggregators {

    private Aggregators() {}

    public static <T> Aggregator<T, Long> count() {
        return stream -> stream.count();
    }

    public static <T> Aggregator<T, Double> sum(ToDoubleFunction<T> field) {
        return stream -> stream.mapToDouble(field).sum();
    }

    public static <T> Aggregator<T, Double> avg(ToDoubleFunction<T> field) {
        return stream -> stream.mapToDouble(field).average().orElse(Double.NaN);
    }

    public static <T> Aggregator<T, Double> min(ToDoubleFunction<T> field) {
        return stream -> stream.mapToDouble(field).min().orElse(Double.NaN);
    }

    public static <T> Aggregator<T, Double> max(ToDoubleFunction<T> field) {
        return stream -> stream.mapToDouble(field).max().orElse(Double.NaN);
    }

    /**
     * Returns the smallest value of {@code field} across the group.
     *
     * <p>Returns {@code null} when the input stream is empty. This is unreachable via
     * {@link GroupByQuery#aggregate} (grouping never produces empty partitions), but
     * callers wiring this aggregator into a custom pipeline should guard for {@code null}
     * when passing an empty stream directly.</p>
     */
    public static <T, R extends Comparable<R>> Aggregator<T, R> minBy(Function<T, R> field) {
        return stream -> stream.map(field).min(Comparator.naturalOrder()).orElse(null);
    }

    /**
     * Returns the largest value of {@code field} across the group.
     *
     * <p>Returns {@code null} when the input stream is empty. This is unreachable via
     * {@link GroupByQuery#aggregate} (grouping never produces empty partitions), but
     * callers wiring this aggregator into a custom pipeline should guard for {@code null}
     * when passing an empty stream directly.</p>
     */
    public static <T, R extends Comparable<R>> Aggregator<T, R> maxBy(Function<T, R> field) {
        return stream -> stream.map(field).max(Comparator.naturalOrder()).orElse(null);
    }
}
