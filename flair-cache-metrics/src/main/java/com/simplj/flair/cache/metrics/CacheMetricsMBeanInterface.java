package com.simplj.flair.cache.metrics;

import javax.management.MXBean;

@MXBean
public interface CacheMetricsMBeanInterface {
    long   getHitCount();
    long   getMissCount();
    double getHitRatePercent();
    long   getSize();
    long   getEvictionCount();
    long   getExpirationCount();
    long   getPutCount();
    long   getDeleteCount();
}
