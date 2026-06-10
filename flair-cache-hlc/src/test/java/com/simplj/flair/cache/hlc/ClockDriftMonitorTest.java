package com.simplj.flair.cache.hlc;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ClockDriftMonitorTest {

    @Test
    void noDrift_noAlert() {
        ClockDriftMonitor monitor = new ClockDriftMonitor(5_000L);
        monitor.checkDrift(1001L, 1000L);  // 1ms drift
        assertEquals(0, monitor.alertCount());
    }

    @Test
    void driftBelowThreshold_noAlert() {
        ClockDriftMonitor monitor = new ClockDriftMonitor(5_000L);
        monitor.checkDrift(1_004_000L, 1_000_000L);  // 4s drift, threshold 5s
        assertEquals(0, monitor.alertCount());
    }

    @Test
    void driftAboveThreshold_alertFired() {
        ClockDriftMonitor monitor = new ClockDriftMonitor(5_000L);
        monitor.checkDrift(1_006_000L, 1_000_000L);  // 6s drift, threshold 5s
        assertEquals(1, monitor.alertCount());
    }

    @Test
    void backwardJump_noAlert() {
        ClockDriftMonitor monitor = new ClockDriftMonitor(5_000L);
        monitor.checkDrift(900L, 1000L);  // wall behind logical — backward NTP correction
        assertEquals(0, monitor.alertCount());
        assertEquals(0L, monitor.maxObservedDriftMs());
    }

    @Test
    void uninitializedClock_skipped() {
        ClockDriftMonitor monitor = new ClockDriftMonitor(5_000L);
        monitor.checkDrift(1_000_000L, 0L);  // logical = 0 means not yet initialized
        assertEquals(0, monitor.alertCount());
        assertEquals(0L, monitor.maxObservedDriftMs());
    }

    @Test
    void maxObservedDrift_tracksHighWaterMark() {
        ClockDriftMonitor monitor = new ClockDriftMonitor(5_000L);
        monitor.checkDrift(1_002_000L, 1_000_000L);  // 2s
        monitor.checkDrift(1_003_000L, 1_000_000L);  // 3s
        monitor.checkDrift(1_001_000L, 1_000_000L);  // 1s
        assertEquals(3_000L, monitor.maxObservedDriftMs());
    }

    @Test
    void multipleAlerts_eachCounted() {
        ClockDriftMonitor monitor = new ClockDriftMonitor(1_000L);
        monitor.checkDrift(10_000L, 1_000L);   // 9s drift
        monitor.checkDrift(20_000L, 2_000L);   // 18s drift
        monitor.checkDrift(30_000L, 3_000L);   // 27s drift
        assertEquals(3, monitor.alertCount());
    }

    @Test
    void thresholdAccessor_returnsConfiguredValue() {
        ClockDriftMonitor monitor = new ClockDriftMonitor(12_345L);
        assertEquals(12_345L, monitor.maxDriftThresholdMs());
    }

    @Test
    void defaultThreshold_is60Seconds() {
        ClockDriftMonitor monitor = new ClockDriftMonitor();
        assertEquals(60_000L, monitor.maxDriftThresholdMs());
        assertEquals(ClockDriftMonitor.DEFAULT_MAX_DRIFT_MS, monitor.maxDriftThresholdMs());
    }

    // ── checkRemoteDrift ───────────────────────────────────────────────────────

    @Test
    void remoteDrift_aboveThreshold_alertFired() {
        ClockDriftMonitor monitor = new ClockDriftMonitor(5_000L);
        // remote.logical is 10s ahead of our wall — should fire
        monitor.checkRemoteDrift(1_010_000L, 1_000_000L);
        assertEquals(1, monitor.alertCount());
        assertEquals(10_000L, monitor.maxObservedDriftMs());
    }

    @Test
    void remoteDrift_belowThreshold_noAlert() {
        ClockDriftMonitor monitor = new ClockDriftMonitor(5_000L);
        // remote.logical is 2s ahead — below threshold
        monitor.checkRemoteDrift(1_002_000L, 1_000_000L);
        assertEquals(0, monitor.alertCount());
        assertEquals(2_000L, monitor.maxObservedDriftMs());
    }

    @Test
    void remoteDrift_remoteNotAhead_noAlert() {
        ClockDriftMonitor monitor = new ClockDriftMonitor(5_000L);
        // remote.logical <= wall — healthy case, no drift
        monitor.checkRemoteDrift(999L, 1_000L);
        assertEquals(0, monitor.alertCount());
        assertEquals(0L, monitor.maxObservedDriftMs());
    }

    @Test
    void remoteDrift_wallZero_skipped() {
        ClockDriftMonitor monitor = new ClockDriftMonitor(5_000L);
        // wallMs == 0 is the uninitialized guard — must skip entirely
        monitor.checkRemoteDrift(1_000_000L, 0L);
        assertEquals(0, monitor.alertCount());
        assertEquals(0L, monitor.maxObservedDriftMs());
    }

    @Test
    void checkDriftAndRemoteDrift_shareCountersAndHighWatermark() {
        ClockDriftMonitor monitor = new ClockDriftMonitor(5_000L);
        // local drift below threshold — updates high-watermark, no alert
        monitor.checkDrift(1_002_000L, 1_000_000L);   // drift = 2s
        assertEquals(0, monitor.alertCount());
        assertEquals(2_000L, monitor.maxObservedDriftMs());
        // remote drift above threshold — alert fires, high-watermark advances
        monitor.checkRemoteDrift(1_012_000L, 1_000_000L); // drift = 12s
        assertEquals(1, monitor.alertCount());
        assertEquals(12_000L, monitor.maxObservedDriftMs());
    }
}
