package com.simplj.flair.cache.dsl;

import java.util.List;

/**
 * Typed result wrapper returned by {@link SingleBlockQuery#fetchResult()}.
 * Contains the matched, paginated items and execution timing.
 *
 * <p>To obtain the total count of matching entries before pagination is applied
 * (e.g., "showing 10 of 847 results"), call
 * {@link SingleBlockQuery#countMatching()} before chaining offset/limit.</p>
 */
public record QueryResult<T>(List<T> items, long executionNanos) {

    public int size()        { return items.size(); }
    public boolean isEmpty() { return items.isEmpty(); }
}
