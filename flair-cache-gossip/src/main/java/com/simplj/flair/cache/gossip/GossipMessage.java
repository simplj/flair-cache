package com.simplj.flair.cache.gossip;

import java.util.List;
import java.util.UUID;

final class GossipMessage {
    final GossipMessageType type;
    final UUID              senderId;
    final long              incarnation;
    final UUID              targetId;       // PING_REQ only
    final List<NodeInfo>    piggybacked;

    GossipMessage(GossipMessageType type, UUID senderId, long incarnation,
                  UUID targetId, List<NodeInfo> piggybacked) {
        this.type        = type;
        this.senderId    = senderId;
        this.incarnation = incarnation;
        this.targetId    = targetId;
        this.piggybacked = piggybacked;
    }

    static GossipMessage ping(UUID sid, long inc, List<NodeInfo> pb) {
        return new GossipMessage(GossipMessageType.PING, sid, inc, null, pb);
    }

    static GossipMessage pong(UUID sid, long inc, List<NodeInfo> pb) {
        return new GossipMessage(GossipMessageType.PONG, sid, inc, null, pb);
    }

    static GossipMessage pingReq(UUID sid, long inc, UUID target, List<NodeInfo> pb) {
        return new GossipMessage(GossipMessageType.PING_REQ, sid, inc, target, pb);
    }

    static GossipMessage join(UUID sid, long inc) {
        return new GossipMessage(GossipMessageType.JOIN, sid, inc, null, List.of());
    }

    static GossipMessage joinAck(UUID sid, long inc, List<NodeInfo> members) {
        return new GossipMessage(GossipMessageType.JOIN_ACK, sid, inc, null, members);
    }

    static GossipMessage leave(UUID sid, long inc) {
        return new GossipMessage(GossipMessageType.LEAVE, sid, inc, null, List.of());
    }
}
