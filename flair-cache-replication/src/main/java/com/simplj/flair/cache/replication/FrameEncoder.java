package com.simplj.flair.cache.replication;

import com.simplj.flair.cache.store.CacheEntry;
import com.simplj.flair.cache.transport.RawFrame;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

final class FrameEncoder {

    static final byte TYPE_PUT    = 0x01;
    static final byte TYPE_DELETE = 0x02;
    static final byte TYPE_PING   = 0x03;
    static final byte TYPE_PONG   = 0x04;
    static final byte TYPE_ACK    = 0x08;

    private static final byte NEEDS_ACK = 0x01;
    private static final byte NO_ACK    = 0x00;

    private FrameEncoder() {}

    static RawFrame encodePut(long frameId, boolean needsAck,
                              ReplicationEvent.PutEvent event) {
        byte[] blockBytes = event.blockName().getBytes(StandardCharsets.UTF_8);
        byte[] key = event.key();
        CacheEntry entry = event.entry();
        byte[] val = entry.value() != null ? entry.value() : new byte[0];
        UUID nodeId = entry.originNodeId() != null ? entry.originNodeId() : new UUID(0, 0);

        // frameId(8) + needsAck(1) + hlc(16) + blockLen(2) + block + keyLen(2) + key + valLen(4) + val + expiryMs(8) + nodeId(16)
        int size = 8 + 1 + 16 + 2 + blockBytes.length + 2 + key.length + 4 + val.length + 8 + 16;
        ByteBuffer buf = ByteBuffer.allocate(size);
        buf.putLong(frameId);
        buf.put(needsAck ? NEEDS_ACK : NO_ACK);
        entry.hlc().encode(buf);
        buf.putShort((short) blockBytes.length);
        buf.put(blockBytes);
        buf.putShort((short) key.length);
        buf.put(key);
        buf.putInt(val.length);
        buf.put(val);
        buf.putLong(entry.expiryEpochMs());
        buf.putLong(nodeId.getMostSignificantBits());
        buf.putLong(nodeId.getLeastSignificantBits());
        return new RawFrame(TYPE_PUT, buf.array());
    }

    static RawFrame encodeDelete(long frameId, boolean needsAck,
                                 ReplicationEvent.DeleteEvent event) {
        byte[] blockBytes = event.blockName().getBytes(StandardCharsets.UTF_8);
        byte[] key = event.key();
        UUID nodeId = event.originNodeId() != null ? event.originNodeId() : new UUID(0, 0);

        // frameId(8) + needsAck(1) + hlc(16) + blockLen(2) + block + keyLen(2) + key + nodeId(16)
        int size = 8 + 1 + 16 + 2 + blockBytes.length + 2 + key.length + 16;
        ByteBuffer buf = ByteBuffer.allocate(size);
        buf.putLong(frameId);
        buf.put(needsAck ? NEEDS_ACK : NO_ACK);
        event.hlc().encode(buf);
        buf.putShort((short) blockBytes.length);
        buf.put(blockBytes);
        buf.putShort((short) key.length);
        buf.put(key);
        buf.putLong(nodeId.getMostSignificantBits());
        buf.putLong(nodeId.getLeastSignificantBits());
        return new RawFrame(TYPE_DELETE, buf.array());
    }

    static RawFrame encodePing(UUID nodeId) {
        ByteBuffer buf = ByteBuffer.allocate(16);
        buf.putLong(nodeId.getMostSignificantBits());
        buf.putLong(nodeId.getLeastSignificantBits());
        return new RawFrame(TYPE_PING, buf.array());
    }

    static RawFrame encodePong(UUID nodeId) {
        ByteBuffer buf = ByteBuffer.allocate(16);
        buf.putLong(nodeId.getMostSignificantBits());
        buf.putLong(nodeId.getLeastSignificantBits());
        return new RawFrame(TYPE_PONG, buf.array());
    }

    static RawFrame encodeAck(long frameId) {
        ByteBuffer buf = ByteBuffer.allocate(8);
        buf.putLong(frameId);
        return new RawFrame(TYPE_ACK, buf.array());
    }
}
