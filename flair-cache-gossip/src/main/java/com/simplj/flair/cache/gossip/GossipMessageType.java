package com.simplj.flair.cache.gossip;

public enum GossipMessageType {
    PING    ((byte) 0x01),
    PONG    ((byte) 0x02),
    PING_REQ((byte) 0x03),
    JOIN    ((byte) 0x04),
    JOIN_ACK((byte) 0x05),
    LEAVE   ((byte) 0x06);

    public final byte code;

    GossipMessageType(byte code) { this.code = code; }

    public static GossipMessageType fromCode(byte code) {
        for (GossipMessageType t : values()) {
            if (t.code == code) return t;
        }
        throw new IllegalArgumentException("Unknown gossip type: 0x" + Integer.toHexString(code & 0xFF));
    }
}
