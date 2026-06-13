package com.simplj.flair.cache.replication;

import com.simplj.flair.cache.hlc.HLCTimestamp;
import com.simplj.flair.cache.store.CacheEntry;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

final class FrameDecoder {

    private static final Logger log = Logger.getLogger(FrameDecoder.class.getName());

    private FrameDecoder() {}

    record DecodedPut(long frameId, boolean needsAck, String blockName, byte[] key, CacheEntry entry) {}

    record DecodedDelete(long frameId, boolean needsAck, String blockName, byte[] key,
                         HLCTimestamp hlc, UUID originNodeId) {}

    static DecodedPut decodePut(byte[] payload) {
        // frameId(8) + needsAck(1) + hlc(16) + blockLen(2) + block + keyLen(2) + key + valLen(4) + val + expiryMs(8) + nodeId(16)
        int minSize = 8 + 1 + 16 + 2 + 2 + 4 + 8 + 16;
        if (payload.length < minSize) {
            log.warning("PUT payload too short: " + payload.length);
            return null;
        }
        try {
            ByteBuffer buf = ByteBuffer.wrap(payload);
            long frameId = buf.getLong();
            boolean needsAck = buf.get() == 0x01;
            HLCTimestamp hlc = HLCTimestamp.decode(buf);

            int blockLen = buf.getShort() & 0xFFFF;
            byte[] blockBytes = new byte[blockLen];
            buf.get(blockBytes);
            String blockName = new String(blockBytes, StandardCharsets.UTF_8);

            int keyLen = buf.getShort() & 0xFFFF;
            byte[] key = new byte[keyLen];
            buf.get(key);

            int valLen = buf.getInt();
            byte[] val = new byte[valLen];
            buf.get(val);

            long expiryMs = buf.getLong();
            UUID nodeId = new UUID(buf.getLong(), buf.getLong());

            CacheEntry entry = new CacheEntry(val, hlc, expiryMs, 0L, 0L, nodeId);
            return new DecodedPut(frameId, needsAck, blockName, key, entry);
        } catch (Exception e) {
            if (log.isLoggable(Level.WARNING)) {
                log.log(Level.WARNING, "Failed to decode PUT payload", e);
            }
            return null;
        }
    }

    static DecodedDelete decodeDelete(byte[] payload) {
        // frameId(8) + needsAck(1) + hlc(16) + blockLen(2) + block + keyLen(2) + key + nodeId(16)
        int minSize = 8 + 1 + 16 + 2 + 2 + 16;
        if (payload.length < minSize) {
            log.warning("DELETE payload too short: " + payload.length);
            return null;
        }
        try {
            ByteBuffer buf = ByteBuffer.wrap(payload);
            long frameId = buf.getLong();
            boolean needsAck = buf.get() == 0x01;
            HLCTimestamp hlc = HLCTimestamp.decode(buf);

            int blockLen = buf.getShort() & 0xFFFF;
            byte[] blockBytes = new byte[blockLen];
            buf.get(blockBytes);
            String blockName = new String(blockBytes, StandardCharsets.UTF_8);

            int keyLen = buf.getShort() & 0xFFFF;
            byte[] key = new byte[keyLen];
            buf.get(key);

            UUID nodeId = new UUID(buf.getLong(), buf.getLong());
            return new DecodedDelete(frameId, needsAck, blockName, key, hlc, nodeId);
        } catch (Exception e) {
            if (log.isLoggable(Level.WARNING)) {
                log.log(Level.WARNING, "Failed to decode DELETE payload", e);
            }
            return null;
        }
    }

    static UUID decodePingPong(byte[] payload) {
        if (payload.length < 16) {
            log.warning("PING/PONG payload too short: " + payload.length);
            return null;
        }
        ByteBuffer buf = ByteBuffer.wrap(payload);
        return new UUID(buf.getLong(), buf.getLong());
    }

    static long decodeAck(byte[] payload) {
        if (payload.length < 8) {
            log.warning("ACK payload too short: " + payload.length);
            return -1L;
        }
        return ByteBuffer.wrap(payload).getLong();
    }
}
