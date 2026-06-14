package com.simplj.flair.cache.metrics;

import javax.management.MXBean;

@MXBean
public interface ClusterMetricsMBeanInterface {
    int  getAliveNodeCount();
    int  getSuspectedNodeCount();
    long getDeadNodeCount();
    long getGossipTickCount();
    long getBootstrapSyncDurationMs();
}
