package com.simplj.flair.cache.gossip;

import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class GossipProtocolTest {

    private static final InetAddress LOOPBACK;
    static {
        try { LOOPBACK = InetAddress.getByName("127.0.0.1"); }
        catch (Exception e) { throw new ExceptionInInitializerError(e); }
    }

    private static NodeInfo aliveNode() throws Exception {
        return new NodeInfo(UUID.randomUUID(), LOOPBACK, 7891,
                NodeStatus.ALIVE, 1L, System.currentTimeMillis());
    }

    private static NodeInfo suspectedNode() throws Exception {
        return new NodeInfo(UUID.randomUUID(), LOOPBACK, 7892,
                NodeStatus.SUSPECTED, 3L, System.currentTimeMillis());
    }

    private static NodeInfo deadNode() throws Exception {
        return new NodeInfo(UUID.randomUUID(), LOOPBACK, 7893,
                NodeStatus.DEAD, 5L, System.currentTimeMillis());
    }

    private static GossipMessage roundTrip(GossipMessage msg) throws GossipProtocolException {
        byte[] encoded = GossipProtocol.encode(msg);
        return GossipProtocol.decode(encoded, encoded.length);
    }

    // ── PING ─────────────────────────────────────────────────────────────────

    @Test
    void ping_encodeDecodeRoundTrip() throws Exception {
        UUID    senderId = UUID.randomUUID();
        long    inc      = 42L;
        GossipMessage msg = GossipMessage.ping(senderId, inc, List.of());

        GossipMessage decoded = roundTrip(msg);

        assertEquals(GossipMessageType.PING, decoded.type);
        assertEquals(senderId, decoded.senderId);
        assertEquals(inc, decoded.incarnation);
        assertNull(decoded.targetId);
        assertTrue(decoded.piggybacked.isEmpty());
    }

    // ── PONG ─────────────────────────────────────────────────────────────────

    @Test
    void pong_encodeDecodeRoundTrip() throws Exception {
        GossipMessage decoded = roundTrip(GossipMessage.pong(UUID.randomUUID(), 7L, List.of()));
        assertEquals(GossipMessageType.PONG, decoded.type);
    }

    // ── PING_REQ ─────────────────────────────────────────────────────────────

    @Test
    void pingReq_encodeDecodeRoundTrip() throws Exception {
        UUID sender = UUID.randomUUID();
        UUID target = UUID.randomUUID();
        GossipMessage msg     = GossipMessage.pingReq(sender, 2L, target, List.of());
        GossipMessage decoded = roundTrip(msg);

        assertEquals(GossipMessageType.PING_REQ, decoded.type);
        assertEquals(sender, decoded.senderId);
        assertEquals(target, decoded.targetId);
    }

    // ── JOIN / JOIN_ACK / LEAVE ───────────────────────────────────────────────

    @Test
    void join_encodeDecodeRoundTrip() throws Exception {
        GossipMessage decoded = roundTrip(GossipMessage.join(UUID.randomUUID(), 0L));
        assertEquals(GossipMessageType.JOIN, decoded.type);
        assertTrue(decoded.piggybacked.isEmpty());
    }

    @Test
    void joinAck_encodeDecodeRoundTrip() throws Exception {
        List<NodeInfo> members = List.of(aliveNode(), suspectedNode());
        GossipMessage  msg     = GossipMessage.joinAck(UUID.randomUUID(), 1L, members);
        GossipMessage  decoded = roundTrip(msg);

        assertEquals(GossipMessageType.JOIN_ACK, decoded.type);
        assertEquals(2, decoded.piggybacked.size());
    }

    @Test
    void leave_encodeDecodeRoundTrip() throws Exception {
        GossipMessage decoded = roundTrip(GossipMessage.leave(UUID.randomUUID(), 3L));
        assertEquals(GossipMessageType.LEAVE, decoded.type);
    }

    // ── Piggybacked deltas ────────────────────────────────────────────────────

    @Test
    void piggybacked_allStatusesPreserved() throws Exception {
        NodeInfo alive     = aliveNode();
        NodeInfo suspected = suspectedNode();
        NodeInfo dead      = deadNode();
        GossipMessage msg     = GossipMessage.ping(UUID.randomUUID(), 10L,
                List.of(alive, suspected, dead));
        GossipMessage decoded = roundTrip(msg);

        assertEquals(3, decoded.piggybacked.size());
        assertEquals(alive.id(),     decoded.piggybacked.get(0).id());
        assertEquals(NodeStatus.ALIVE,     decoded.piggybacked.get(0).status());
        assertEquals(suspected.id(), decoded.piggybacked.get(1).id());
        assertEquals(NodeStatus.SUSPECTED, decoded.piggybacked.get(1).status());
        assertEquals(dead.id(),      decoded.piggybacked.get(2).id());
        assertEquals(NodeStatus.DEAD,      decoded.piggybacked.get(2).status());
    }

    @Test
    void piggybacked_incarnationAndPortPreserved() throws Exception {
        NodeInfo node = new NodeInfo(UUID.randomUUID(), LOOPBACK, 9999,
                NodeStatus.ALIVE, 77L, 0L);
        GossipMessage decoded = roundTrip(GossipMessage.ping(UUID.randomUUID(), 1L, List.of(node)));

        NodeInfo out = decoded.piggybacked.get(0);
        assertEquals(node.id(),          out.id());
        assertEquals(9999,               out.port());
        assertEquals(77L,                out.incarnation());
        assertEquals(NodeStatus.ALIVE,   out.status());
        assertArrayEquals(LOOPBACK.getAddress(), out.address().getAddress());
    }

    // ── Encoded size stays within MAX_PACKET ──────────────────────────────────

    @Test
    void maxPiggybacked_fitsInMaxPacket() throws Exception {
        int count = GossipProtocol.MAX_PIGGYBACKED;
        List<NodeInfo> deltas = new java.util.ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            deltas.add(new NodeInfo(UUID.randomUUID(), LOOPBACK, 7000 + i,
                    NodeStatus.ALIVE, i, 0L));
        }
        byte[] encoded = GossipProtocol.encode(GossipMessage.ping(UUID.randomUUID(), 1L, deltas));
        assertTrue(encoded.length <= GossipProtocol.MAX_PACKET,
                "Encoded size " + encoded.length + " exceeds MAX_PACKET " + GossipProtocol.MAX_PACKET);
    }

    // ── Decode error handling ─────────────────────────────────────────────────

    @Test
    void decode_tooShort_throws() {
        assertThrows(GossipProtocolException.class, () ->
                GossipProtocol.decode(new byte[3], 3));
    }

    @Test
    void decode_unknownType_throws() {
        byte[] data = new byte[30];
        data[0] = (byte) 0xFF; // unknown type
        assertThrows(GossipProtocolException.class, () ->
                GossipProtocol.decode(data, data.length));
    }

    @Test
    void decode_numDeltasExceedsMax_throws() throws Exception {
        // Start from a valid zero-delta PING header (type+sender+incarnation+numDeltas=0)
        // then overwrite the numDeltas field with MAX_PIGGYBACKED+1.
        byte[] data = GossipProtocol.encode(GossipMessage.ping(UUID.randomUUID(), 0L, List.of()));
        int tooMany = GossipProtocol.MAX_PIGGYBACKED + 1;
        data[data.length - 2] = (byte) ((tooMany >> 8) & 0xFF);
        data[data.length - 1] = (byte) (tooMany & 0xFF);
        assertThrows(GossipProtocolException.class, () ->
                GossipProtocol.decode(data, data.length));
    }

    @Test
    void decode_truncatedDeltaBody_throws() throws Exception {
        // Valid PING header, numDeltas=1, but no delta bytes follow → buffer underflow.
        byte[] data = GossipProtocol.encode(GossipMessage.ping(UUID.randomUUID(), 0L, List.of()));
        data[data.length - 2] = 0x00;
        data[data.length - 1] = 0x01;  // numDeltas=1, zero delta bytes in buffer
        assertThrows(GossipProtocolException.class, () ->
                GossipProtocol.decode(data, data.length));
    }
}
