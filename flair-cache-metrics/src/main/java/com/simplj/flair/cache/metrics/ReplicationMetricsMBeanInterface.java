package com.simplj.flair.cache.metrics;

import javax.management.MXBean;

@MXBean
public interface ReplicationMetricsMBeanInterface {
    long getAvgReplicationLagMs();
    long getMaxReplicationLagMs();
    long getPendingFrameCount();
    long getPendingAckCount();
    /** Frames dispatched by the fanout but not yet flushed to the TCP socket for any peer. */
    long getPendingWriteCount();
    /** Cumulative per-peer frame sends by the fanout (1 frame to N peers = N). */
    long getTotalFramesDistributed();
    /** Cumulative PUT/DELETE frames applied by this node's IncomingHandler. */
    long getTotalFramesApplied();
    long getDroppedFrameCount();
    long getAckTimeoutCount();
    long getBytesSentTotal();
    long getBytesReceivedTotal();
}
