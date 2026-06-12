package com.simplj.flair.cache.store;

public record CacheStats(long hits, long misses, long evictions, long expirations, long size) {

    public double hitRate() {
        long total = hits + misses;
        return total == 0 ? 0.0 : hits / (double) total * 100.0;
    }
}
