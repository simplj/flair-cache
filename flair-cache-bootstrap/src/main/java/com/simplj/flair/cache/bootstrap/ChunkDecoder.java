package com.simplj.flair.cache.bootstrap;

import com.simplj.flair.cache.hlc.HLCTimestamp;
import com.simplj.flair.cache.store.CacheEntry;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Decodes SYNC_REQ, SYNC_CHUNK, and SYNC_DONE frames on the joiner side.
 */
final class ChunkDecoder {

    private static final Logger log = Logger.getLogger(ChunkDecoder.class.getName());

    record ChunkEntry(String blockName, byte[] key, CacheEntry entry) {}
    record DecodedChunk(int chunkIndex, int totalChunks, List<ChunkEntry> entries) {}

    private ChunkDecoder() {}

    static UUID decodeSyncReq(byte[] payload) {
        if (payload.length < 16) {
            log.warning("SYNC_REQ payload too short: " + payload.length);
            return null;
        }
        ByteBuffer buf = ByteBuffer.wrap(payload);
        return new UUID(buf.getLong(), buf.getLong());
    }

    static long decodeSyncDone(byte[] payload) {
        if (payload.length < 8) {
            log.warning("SYNC_DONE payload too short: " + payload.length);
            return 0L;
        }
        return ByteBuffer.wrap(payload).getLong();
    }

    static DecodedChunk decodeChunk(byte[] payload) {
        try {
            ByteBuffer buf = ByteBuffer.wrap(payload);
            int chunkIndex = buf.getInt();
            int totalChunks = buf.getInt();
            int entryCount = buf.getInt();

            List<ChunkEntry> entries = new ArrayList<>(entryCount);
            for (int i = 0; i < entryCount; i++) {
                int blockLen = buf.getShort() & 0xFFFF;
                byte[] blockBytes = new byte[blockLen];
                buf.get(blockBytes);
                String blockName = new String(blockBytes, StandardCharsets.UTF_8);

                int keyLen = buf.getShort() & 0xFFFF;
                byte[] key = new byte[keyLen];
                buf.get(key);

                HLCTimestamp hlc = HLCTimestamp.decode(buf);
                long expiryMs = buf.getLong();
                UUID originNodeId = new UUID(buf.getLong(), buf.getLong());

                int valLen = buf.getInt();
                byte[] value = new byte[valLen];
                buf.get(value);

                CacheEntry entry = new CacheEntry(value, hlc, expiryMs, 0L, 0L, originNodeId);
                entries.add(new ChunkEntry(blockName, key, entry));
            }
            return new DecodedChunk(chunkIndex, totalChunks, entries);
        } catch (Exception e) {
            if (log.isLoggable(Level.WARNING)) {
                log.log(Level.WARNING, "Failed to decode SYNC_CHUNK payload", e);
            }
            return null;
        }
    }
}
