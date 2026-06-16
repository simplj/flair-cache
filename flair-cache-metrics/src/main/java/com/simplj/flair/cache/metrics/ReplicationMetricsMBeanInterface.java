package com.simplj.flair.cache.metrics;

import javax.management.MXBean;

@MXBean
public interface ReplicationMetricsMBeanInterface {
    long getAvgReplicationLagMs();
    long getMaxReplicationLagMs();
    long getPendingFrameCount();
    long getPendingAckCount();
    long getDroppedFrameCount();
    long getAckTimeoutCount();
    long getBytesSentTotal();
    long getBytesReceivedTotal();
}
