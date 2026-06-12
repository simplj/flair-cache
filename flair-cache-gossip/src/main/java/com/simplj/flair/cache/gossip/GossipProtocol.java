package com.simplj.flair.cache.gossip;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

final class GossipProtocol {

    static final int MAX_PACKET = 1400;

    // type(1) + senderUUID(16) + incarnation(8) = 25
    private static final int HEADER_SIZE   = 25;
    // targetUUID(16) — PING_REQ only
    private static final int PING_REQ_EXTRA = 16;
    // numDeltas(2)
    private static final int NUM_DELTAS_FIELD = 2;
    // nodeId(16) + status(1) + incarnation(8) + addrLen(1) + addr(≤16) + port(2) — IPv6 worst case
    private static final int DELTA_MAX = 44;

    // max deltas that safely fit in MAX_PACKET (conservative: assume IPv6)
    static final int MAX_PIGGYBACKED = (MAX_PACKET - HEADER_SIZE - NUM_DELTAS_FIELD) / DELTA_MAX;

    private GossipProtocol() {}

    static byte[] encode(GossipMessage msg) {
        int cap = HEADER_SIZE + NUM_DELTAS_FIELD;
        if (msg.type == GossipMessageType.PING_REQ) cap += PING_REQ_EXTRA;
        for (NodeInfo n : msg.piggybacked) cap += 16 + 1 + 8 + 1 + n.address().getAddress().length + 2;

        ByteBuffer buf = ByteBuffer.allocate(cap).order(ByteOrder.BIG_ENDIAN);
        buf.put(msg.type.code);
        putUUID(buf, msg.senderId);
        buf.putLong(msg.incarnation);

        if (msg.type == GossipMessageType.PING_REQ && msg.targetId != null) {
            putUUID(buf, msg.targetId);
        }

        List<NodeInfo> deltas = msg.piggybacked;
        buf.putShort((short) deltas.size());
        for (NodeInfo n : deltas) {
            putUUID(buf, n.id());
            buf.put(statusToByte(n.status()));
            buf.putLong(n.incarnation());
            byte[] addr = n.address().getAddress();
            buf.put((byte) addr.length);
            buf.put(addr);
            buf.putShort((short) n.port());
        }

        buf.flip();
        byte[] out = new byte[buf.remaining()];
        buf.get(out);
        return out;
    }

    static GossipMessage decode(byte[] data, int len) throws GossipProtocolException {
        if (len < HEADER_SIZE + NUM_DELTAS_FIELD) {
            throw new GossipProtocolException("Packet too short: " + len);
        }
        ByteBuffer buf = ByteBuffer.wrap(data, 0, len).order(ByteOrder.BIG_ENDIAN);

        GossipMessageType type;
        try {
            type = GossipMessageType.fromCode(buf.get());
        } catch (IllegalArgumentException e) {
            throw new GossipProtocolException("Unknown message type", e);
        }

        UUID senderId    = getUUID(buf);
        long incarnation = buf.getLong();

        UUID targetId = null;
        if (type == GossipMessageType.PING_REQ) {
            if (buf.remaining() < PING_REQ_EXTRA + NUM_DELTAS_FIELD) {
                throw new GossipProtocolException("PING_REQ too short");
            }
            targetId = getUUID(buf);
        }

        int numDeltas = buf.getShort() & 0xFFFF;
        if (numDeltas > MAX_PIGGYBACKED) {
            throw new GossipProtocolException("numDeltas exceeds maximum: " + numDeltas);
        }
        List<NodeInfo> deltas = new ArrayList<>(numDeltas);
        for (int i = 0; i < numDeltas; i++) {
            if (buf.remaining() < 16 + 1 + 8 + 1) {
                throw new GossipProtocolException("Delta truncated at index " + i);
            }
            UUID       nodeId         = getUUID(buf);
            NodeStatus status         = statusFromByte(buf.get());
            long       nodeIncarnation = buf.getLong();
            int        addrLen        = buf.get() & 0xFF;
            if (addrLen != 4 && addrLen != 16) {
                throw new GossipProtocolException("Invalid address length " + addrLen + " at delta " + i);
            }
            if (buf.remaining() < addrLen + 2) {
                throw new GossipProtocolException("Delta address truncated at index " + i);
            }
            byte[] addrBytes = new byte[addrLen];
            buf.get(addrBytes);
            InetAddress address;
            try {
                address = InetAddress.getByAddress(addrBytes);
            } catch (UnknownHostException e) {
                throw new GossipProtocolException("Invalid address at delta " + i, e);
            }
            int port = buf.getShort() & 0xFFFF;
            deltas.add(new NodeInfo(nodeId, address, port, status, nodeIncarnation, 0L));
        }
        return new GossipMessage(type, senderId, incarnation, targetId, deltas);
    }

    private static void putUUID(ByteBuffer buf, UUID id) {
        buf.putLong(id.getMostSignificantBits());
        buf.putLong(id.getLeastSignificantBits());
    }

    private static UUID getUUID(ByteBuffer buf) {
        return new UUID(buf.getLong(), buf.getLong());
    }

    private static byte statusToByte(NodeStatus s) {
        switch (s) {
            case ALIVE:     return 0x01;
            case SUSPECTED: return 0x02;
            case DEAD:      return 0x03;
            default: throw new IllegalArgumentException("Unknown status: " + s);
        }
    }

    private static NodeStatus statusFromByte(byte b) throws GossipProtocolException {
        switch (b) {
            case 0x01: return NodeStatus.ALIVE;
            case 0x02: return NodeStatus.SUSPECTED;
            case 0x03: return NodeStatus.DEAD;
            default: throw new GossipProtocolException(
                    "Unknown status byte: 0x" + Integer.toHexString(b & 0xFF));
        }
    }
}
