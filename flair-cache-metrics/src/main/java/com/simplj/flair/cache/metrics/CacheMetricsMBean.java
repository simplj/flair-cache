package com.simplj.flair.cache.metrics;

import com.simplj.flair.cache.store.CacheBlock;
import com.simplj.flair.cache.store.CacheStats;

import java.util.concurrent.atomic.LongAdder;

public final class CacheMetricsMBean implements CacheMetricsMBeanInterface {

    private final CacheBlock<?, ?> block;
    private final LongAdder puts    = new LongAdder();
    private final LongAdder deletes = new LongAdder();

    // Baseline recorded at construction time so all stat counters are relative to when
    // this bean was attached — not when the block was created. Without a baseline,
    // getHitCount() (cumulative since block creation) and getPutCount() (events since
    // listener attachment) would be on different epoch baselines when a block is used
    // before it is registered with metrics.
    private final long baseHits;
    private final long baseMisses;
    private final long baseEvictions;
    private final long baseExpirations;

    CacheMetricsMBean(CacheBlock<?, ?> block) {
        this.block = block;
        CacheStats baseline  = block.stats();
        this.baseHits        = baseline.hits();
        this.baseMisses      = baseline.misses();
        this.baseEvictions   = baseline.evictions();
        this.baseExpirations = baseline.expirations();
        block.addPutListener((k, e)    -> puts.increment());
        block.addDeleteListener((k, e) -> deletes.increment());
    }

    // Each getter takes its own stats snapshot. JMX has no "start of batch read" signal,
    // so cross-attribute consistency is a known monitoring trade-off, not a correctness bug.
    // getHitRatePercent() is internally consistent because it derives both hits and misses
    // from a single snapshot.
    @Override public long getHitCount()  { return block.stats().hits()       - baseHits; }
    @Override public long getMissCount() { return block.stats().misses()      - baseMisses; }

    @Override public double getHitRatePercent() {
        CacheStats s    = block.stats();
        long hits       = s.hits()   - baseHits;
        long misses     = s.misses() - baseMisses;
        long total      = hits + misses;
        return total == 0 ? 0.0 : hits / (double) total * 100.0;
    }

    @Override public long getSize()            { return block.stats().size(); }
    @Override public long getEvictionCount()   { return block.stats().evictions()   - baseEvictions; }
    @Override public long getExpirationCount() { return block.stats().expirations() - baseExpirations; }
    @Override public long getPutCount()        { return puts.sum(); }
    @Override public long getDeleteCount()     { return deletes.sum(); }
}
