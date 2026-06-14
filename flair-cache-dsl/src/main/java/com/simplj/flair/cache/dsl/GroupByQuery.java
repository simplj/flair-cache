package com.simplj.flair.cache.dsl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Terminal stage of a {@code from().groupBy()} chain.
 * Partitions the query's result set into groups and applies an {@link Aggregator} per group.
 *
 * <p>Execution is always single-threaded in V1, regardless of whether {@code .parallel()}
 * was called on the originating query. Parallelism for groupBy is reserved for V2.</p>
 */
public final class GroupByQuery<T, K> {

    private final Supplier<Stream<T>> streamSupplier;
    private final Function<T, K>      keyExtractor;

    GroupByQuery(Supplier<Stream<T>> streamSupplier, Function<T, K> keyExtractor) {
        this.streamSupplier = streamSupplier;
        this.keyExtractor   = keyExtractor;
    }

    /**
     * Partitions all matching entries by the group key and reduces each partition
     * with {@code aggregator}.
     *
     * @return map from group key → aggregated value; all groups present, none dropped
     */
    public <R> Map<K, R> aggregate(Aggregator<T, R> aggregator) {
        // groupingBy is thread-safe and avoids manual forEach + HashMap race on parallel streams
        Map<K, List<T>> groups = streamSupplier.get()
                .collect(Collectors.groupingBy(keyExtractor));

        Map<K, R> result = new HashMap<>(groups.size() * 2);
        groups.forEach((key, items) -> result.put(key, aggregator.aggregate(items.stream())));
        return result;
    }
}
