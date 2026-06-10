package com.simplj.flair.cache.hlc;

import java.lang.management.ManagementFactory;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.management.MBeanServer;
import javax.management.ObjectName;

public final class ClockDriftMonitor {

    public interface MXBean {
        long getMaxObservedDriftMs();
        long getDriftAlertThresholdMs();
        long getDriftAlertCount();
    }

    public static final long DEFAULT_MAX_DRIFT_MS = 60_000L;

    private static final Logger log = Logger.getLogger(ClockDriftMonitor.class.getName());

    private final long maxDriftMs;
    private final LongAdder alertCount = new LongAdder();
    private final AtomicLong maxObservedDriftMs = new AtomicLong();

    public ClockDriftMonitor() {
        this(DEFAULT_MAX_DRIFT_MS);
    }

    public ClockDriftMonitor(long maxDriftMs) {
        this.maxDriftMs = maxDriftMs;
    }

    void checkDrift(long wallMs, long logicalMs) {
        if (logicalMs == 0) return;
        long drift = wallMs - logicalMs;
        if (drift <= 0) return;
        maxObservedDriftMs.updateAndGet(prev -> drift > prev ? drift : prev);
        if (drift > maxDriftMs) {
            alertCount.increment();
            if (log.isLoggable(Level.WARNING)) {
                log.warning("HLC clock drift: wall=" + wallMs + "ms logical=" + logicalMs
                        + "ms drift=" + drift + "ms threshold=" + maxDriftMs + "ms");
            }
        }
    }

    void checkRemoteDrift(long remoteLogical, long wallMs) {
        if (wallMs == 0) return;
        long drift = remoteLogical - wallMs;
        if (drift <= 0) return;
        maxObservedDriftMs.updateAndGet(prev -> drift > prev ? drift : prev);
        if (drift > maxDriftMs) {
            alertCount.increment();
            if (log.isLoggable(Level.WARNING)) {
                log.warning("HLC remote drift: remote.logical=" + remoteLogical + "ms wall=" + wallMs
                        + "ms drift=" + drift + "ms threshold=" + maxDriftMs + "ms");
            }
        }
    }

    public long maxObservedDriftMs() {
        return maxObservedDriftMs.get();
    }

    public long alertCount() {
        return alertCount.longValue();
    }

    public long maxDriftThresholdMs() {
        return maxDriftMs;
    }

    /**
     * Registers this monitor as a JMX MBean. Safe to call from any node startup sequence.
     * Silently skips registration if the MBeanServer rejects it (e.g., duplicate name).
     */
    public void registerJmx(String nodeId) {
        try {
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            ObjectName name = new ObjectName(
                    "com.simplj.flair.cache:type=ClockDriftMonitor,node=" + ObjectName.quote(nodeId));
            if (!mbs.isRegistered(name)) {
                MXBean view = new MXBean() {
                    @Override public long getMaxObservedDriftMs()   { return maxObservedDriftMs.get(); }
                    @Override public long getDriftAlertThresholdMs() { return maxDriftMs; }
                    @Override public long getDriftAlertCount()       { return alertCount.longValue(); }
                };
                mbs.registerMBean(view, name);
            }
        } catch (Exception e) {
            if (log.isLoggable(Level.FINE)) {
                log.fine("ClockDriftMonitor JMX registration skipped: " + e.getMessage());
            }
        }
    }
}
