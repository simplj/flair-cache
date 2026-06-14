package com.simplj.flair.cache.dsl;

/**
 * Numeric summary for a double-valued field across a result set.
 * Returned by {@link SingleBlockQuery#summarize}.
 */
public record SummaryStatistics(long count, double sum, double min, double max, double avg) {

    /** Sentinel returned when the query matched zero entries. */
    public static final SummaryStatistics EMPTY =
            new SummaryStatistics(0L, 0.0, Double.NaN, Double.NaN, Double.NaN);
}
