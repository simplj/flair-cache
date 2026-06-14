package com.simplj.flair.cache.metrics;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongSupplier;

public final class ReplicationMetricsMBean implements ReplicationMetricsMBeanInterface {

    private final LongSupplier pendingSupplier;
    private final LongAdder droppedFrames = new LongAdder();
    private final LongAdder ackTimeouts   = new LongAdder();
    private final LongAdder bytesSent     = new LongAdder();
    private final LongAdder bytesReceived = new LongAdder();
    private final LongAdder lagSumMs      = new LongAdder();
    private final LongAdder lagCount      = new LongAdder();
    private final AtomicLong maxLagMs     = new AtomicLong(0L);

    // Package-private: created via MetricsRegistry.withReplication(engine)
    ReplicationMetricsMBean(LongSupplier pendingSupplier) {
        this.pendingSupplier = pendingSupplier;
    }

    public void recordDroppedFrame()            { droppedFrames.increment(); }
    public void recordAckTimeout()              { ackTimeouts.increment(); }
    public void recordBytesSent(long bytes)     { bytesSent.add(bytes); }
    public void recordBytesReceived(long bytes) { bytesReceived.add(bytes); }

    public void recordReplicationLag(long lagMs) {
        // Clamp negative values: clock skew can produce a negative observed lag, which has no
        // physical meaning and would corrupt the running average and max.
        lagMs = Math.max(0L, lagMs);
        // Increment count before sum: getAvgReplicationLagMs() reads count then sum, so if it
        // catches us mid-update it sees sum(N-1)/count(N) — slightly deflated, not inflated.
        lagCount.increment();
        lagSumMs.add(lagMs);
        // Best-effort CAS loop — occasional skipped max updates are acceptable for a monitoring metric
        long cur;
        do { cur = maxLagMs.get(); } while (lagMs > cur && !maxLagMs.compareAndSet(cur, lagMs));
    }

    @Override public long getAvgReplicationLagMs() {
        long count = lagCount.sum();
        return count == 0 ? 0L : lagSumMs.sum() / count;
    }
    @Override public long getMaxReplicationLagMs() { return maxLagMs.get(); }
    @Override public long getPendingFrameCount()   { return pendingSupplier.getAsLong(); }
    @Override public long getDroppedFrameCount()   { return droppedFrames.sum(); }
    @Override public long getAckTimeoutCount()     { return ackTimeouts.sum(); }
    @Override public long getBytesSentTotal()      { return bytesSent.sum(); }
    @Override public long getBytesReceivedTotal()  { return bytesReceived.sum(); }
}
