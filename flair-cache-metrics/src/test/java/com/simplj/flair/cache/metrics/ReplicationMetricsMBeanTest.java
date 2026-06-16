package com.simplj.flair.cache.metrics;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ReplicationMetricsMBeanTest {

    private ReplicationMetricsMBean bean() {
        return new ReplicationMetricsMBean(() -> 0L, () -> 0L);
    }

    @Test
    void initialValuesAreZero() {
        ReplicationMetricsMBean b = bean();
        assertEquals(0L, b.getAvgReplicationLagMs());
        assertEquals(0L, b.getMaxReplicationLagMs());
        assertEquals(0L, b.getPendingFrameCount());
        assertEquals(0L, b.getPendingAckCount());
        assertEquals(0L, b.getDroppedFrameCount());
        assertEquals(0L, b.getAckTimeoutCount());
        assertEquals(0L, b.getBytesSentTotal());
        assertEquals(0L, b.getBytesReceivedTotal());
    }

    @Test
    void avgReplicationLagZeroWhenNoObservations() {
        assertEquals(0L, bean().getAvgReplicationLagMs());
    }

    @Test
    void avgReplicationLagCalculation() {
        ReplicationMetricsMBean b = bean();
        b.recordReplicationLag(10L);
        b.recordReplicationLag(20L);
        b.recordReplicationLag(30L);
        assertEquals(20L, b.getAvgReplicationLagMs());
    }

    @Test
    void maxReplicationLagTracked() {
        ReplicationMetricsMBean b = bean();
        b.recordReplicationLag(5L);
        b.recordReplicationLag(50L);
        b.recordReplicationLag(15L);
        assertEquals(50L, b.getMaxReplicationLagMs());
    }

    @Test
    void droppedFrameCountIncrement() {
        ReplicationMetricsMBean b = bean();
        b.recordDroppedFrame();
        b.recordDroppedFrame();
        assertEquals(2L, b.getDroppedFrameCount());
    }

    @Test
    void ackTimeoutCountIncrement() {
        ReplicationMetricsMBean b = bean();
        b.recordAckTimeout();
        assertEquals(1L, b.getAckTimeoutCount());
    }

    @Test
    void bytesSentAndReceivedAccumulate() {
        ReplicationMetricsMBean b = bean();
        b.recordBytesSent(1024L);
        b.recordBytesSent(512L);
        b.recordBytesReceived(2048L);
        assertEquals(1536L, b.getBytesSentTotal());
        assertEquals(2048L, b.getBytesReceivedTotal());
    }

    @Test
    void pendingFrameCountDelegatestoSupplier() {
        ReplicationMetricsMBean b = new ReplicationMetricsMBean(() -> 42L, () -> 0L);
        assertEquals(42L, b.getPendingFrameCount());
    }

    @Test
    void pendingAckCountDelegatesToSupplier() {
        ReplicationMetricsMBean b = new ReplicationMetricsMBean(() -> 0L, () -> 7L);
        assertEquals(7L, b.getPendingAckCount());
    }

    @Test
    void replicationLagReflectsActualMeasuredLagWithinOneTick() {
        ReplicationMetricsMBean b = bean();
        b.recordReplicationLag(42L);
        assertEquals(42L, b.getAvgReplicationLagMs(),
                "AvgReplicationLagMs must reflect the recorded lag immediately");
    }

    @Test
    void negativeLagClampsToZeroDoesNotCorruptAverage() {
        ReplicationMetricsMBean b = bean();
        b.recordReplicationLag(100L);
        b.recordReplicationLag(-50L); // clock skew — must be treated as 0
        // avg = (100 + 0) / 2 = 50
        assertEquals(50L, b.getAvgReplicationLagMs(),
                "Negative lag must be clamped to 0, not corrupt the average");
    }

    @Test
    void negativeLagDoesNotUpdateMax() {
        ReplicationMetricsMBean b = bean();
        b.recordReplicationLag(-10L);
        assertEquals(0L, b.getMaxReplicationLagMs(),
                "Negative lag clamped to 0 must not update max above initial 0");
    }

    @Test
    void zeroLagRecordedCorrectly() {
        ReplicationMetricsMBean b = bean();
        b.recordReplicationLag(0L);
        assertEquals(0L, b.getAvgReplicationLagMs());
        assertEquals(0L, b.getMaxReplicationLagMs());
    }
}
