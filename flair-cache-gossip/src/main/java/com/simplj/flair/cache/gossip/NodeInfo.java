package com.simplj.flair.cache.gossip;

import java.net.InetAddress;
import java.util.UUID;

public record NodeInfo(
        UUID        id,
        InetAddress address,
        int         port,
        NodeStatus  status,
        long        incarnation,
        long        lastSeenEpochMs
) {
    public NodeInfo withStatus(NodeStatus s) {
        return new NodeInfo(id, address, port, s, incarnation, lastSeenEpochMs);
    }

    public NodeInfo withStatusAndIncarnation(NodeStatus s, long inc) {
        return new NodeInfo(id, address, port, s, inc, System.currentTimeMillis());
    }

    public NodeInfo withLastSeen(long epochMs) {
        return new NodeInfo(id, address, port, status, incarnation, epochMs);
    }

    public String addressString() {
        return address.getHostAddress() + ":" + port;
    }
}
