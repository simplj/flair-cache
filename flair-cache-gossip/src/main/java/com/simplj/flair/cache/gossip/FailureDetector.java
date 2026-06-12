package com.simplj.flair.cache.gossip;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

final class FailureDetector {

    enum ProbeState { PROBING, INDIRECT_PROBING, SUSPECTED }

    static final class ProbeEntry {
        final UUID       targetId;
        long             probeSentMs;  // reset to indirect-send time on markIndirect; tick-thread only
        volatile ProbeState state;

        ProbeEntry(UUID targetId, long probeSentMs) {
            this.targetId    = targetId;
            this.probeSentMs = probeSentMs;
            this.state       = ProbeState.PROBING;
        }
    }

    private final Map<UUID, ProbeEntry> probes = new ConcurrentHashMap<>();

    void startProbe(UUID targetId) {
        probes.put(targetId, new ProbeEntry(targetId, System.currentTimeMillis()));
    }

    void onPong(UUID targetId) {
        probes.remove(targetId);
    }

    void markIndirect(UUID targetId) {
        ProbeEntry e = probes.get(targetId);
        if (e != null) {
            // Reset timestamp so indirectTimeoutMs is measured from when PING_REQ was dispatched,
            // not from when the original direct PING was sent.
            e.probeSentMs = System.currentTimeMillis();
            e.state = ProbeState.INDIRECT_PROBING;
        }
    }

    void markSuspected(UUID targetId) {
        ProbeEntry e = probes.get(targetId);
        if (e != null) e.state = ProbeState.SUSPECTED;
    }

    void clear(UUID targetId) {
        probes.remove(targetId);
    }

    ProbeEntry get(UUID targetId) {
        return probes.get(targetId);
    }

    boolean isProbing(UUID targetId) {
        return probes.containsKey(targetId);
    }
}
