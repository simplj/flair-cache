package com.simplj.flair.cache.metrics;

import javax.management.MXBean;

@MXBean
public interface EvictionMetricsMBeanInterface {
    long getEvictedEntryCount();
    long getExpiredEntryCount();
}
