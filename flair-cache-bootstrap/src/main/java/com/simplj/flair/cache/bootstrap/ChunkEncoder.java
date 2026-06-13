package com.simplj.flair.cache.bootstrap;

import com.simplj.flair.cache.hlc.HLCTimestamp;
import com.simplj.flair.cache.store.CacheEntry;
import com.simplj.flair.cache.transport.RawFrame;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Encodes SYNC_REQ, SYNC_CHUNK, and SYNC_DONE frames for the bootstrap wire protocol.
 *
 * <p>SYNC_CHUNK payload layout:</p>
 * <pre>
 *   chunkIndex (4B) + totalChunks (4B) + entryCount (4B)
 *   for each entry:
 *     blockNameLen(2B)   + blockName(UTF-8)
 *     keyLen(2B)         + key
 *     hlcLogical(8B)     + hlcCounter(8B)
 *     expiryMs(8B)
 *     originNodeIdMsb(8B) + originNodeIdLsb(8B)
 *     valueLen(4B)        + value
 * </pre>
 *
 * <p>{@code originNodeId} is included so that the joiner can apply LWW tie-breaking with the
 * real author UUID rather than a sentinel. Entries originally written without a node identity
 * (e.g. via {@code LocalStore.put()} before replication was wired in) are transmitted as
 * {@code UUID(0, 0)} and decoded back to {@code UUID(0, 0)}.</p>
 */
final class ChunkEncoder {

    static final byte TYPE_SYNC_REQ   = 0x05;
    static final byte TYPE_SYNC_CHUNK = 0x06;
    static final byte TYPE_SYNC_DONE  = 0x07;

    private static final int CHUNK_HEADER_BYTES = 12; // chunkIndex(4) + totalChunks(4) + entryCount(4)
    private static final UUID NULL_NODE_ID = new UUID(0L, 0L);

    record SnapshotEntry(String blockName, byte[] key, CacheEntry entry) {}

    private ChunkEncoder() {}

    static RawFrame encodeSyncReq(UUID localNodeId) {
        ByteBuffer buf = ByteBuffer.allocate(16);
        buf.putLong(localNodeId.getMostSignificantBits());
        buf.putLong(localNodeId.getLeastSignificantBits());
        return new RawFrame(TYPE_SYNC_REQ, buf.array());
    }

    static RawFrame encodeSyncDone(long totalEntries) {
        ByteBuffer buf = ByteBuffer.allocate(8);
        buf.putLong(totalEntries);
        return new RawFrame(TYPE_SYNC_DONE, buf.array());
    }

    /**
     * Encodes one SYNC_CHUNK frame. {@code buf} is cleared and reused — callers must
     * pre-allocate it once per streaming session via {@link #maxChunkBufferSize}.
     */
    static RawFrame encodeChunk(int chunkIndex, int totalChunks,
                                List<SnapshotEntry> entries, ByteBuffer buf) {
        buf.clear();
        buf.putInt(chunkIndex);
        buf.putInt(totalChunks);
        buf.putInt(entries.size());
        for (SnapshotEntry e : entries) {
            byte[] blockBytes = e.blockName().getBytes(StandardCharsets.UTF_8);
            byte[] val = e.entry().value() != null ? e.entry().value() : new byte[0];
            UUID nodeId = e.entry().originNodeId() != null ? e.entry().originNodeId() : NULL_NODE_ID;
            buf.putShort((short) blockBytes.length);
            buf.put(blockBytes);
            buf.putShort((short) e.key().length);
            buf.put(e.key());
            e.entry().hlc().encode(buf);
            buf.putLong(e.entry().expiryEpochMs());
            buf.putLong(nodeId.getMostSignificantBits());
            buf.putLong(nodeId.getLeastSignificantBits());
            buf.putInt(val.length);
            buf.put(val);
        }
        buf.flip();
        byte[] payload = new byte[buf.remaining()];
        buf.get(payload);
        return new RawFrame(TYPE_SYNC_CHUNK, payload);
    }

    /**
     * Partitions {@code entries} into groups whose encoded payload size does not exceed
     * {@code chunkSizeBytes}. A single oversized entry is placed alone in its own chunk.
     */
    static List<List<SnapshotEntry>> partition(List<SnapshotEntry> entries, int chunkSizeBytes) {
        List<List<SnapshotEntry>> result = new ArrayList<>();
        List<SnapshotEntry> current = new ArrayList<>();
        int currentSize = CHUNK_HEADER_BYTES;

        for (SnapshotEntry e : entries) {
            int entrySize = encodedSize(e);
            if (!current.isEmpty() && currentSize + entrySize > chunkSizeBytes) {
                result.add(current);
                current = new ArrayList<>();
                currentSize = CHUNK_HEADER_BYTES;
            }
            current.add(e);
            currentSize += entrySize;
        }
        if (!current.isEmpty()) {
            result.add(current);
        }
        return result;
    }

    /** Returns the buffer capacity needed to encode the largest chunk in {@code chunks}. */
    static int maxChunkBufferSize(List<List<SnapshotEntry>> chunks, int chunkSizeBytes) {
        int max = chunkSizeBytes + CHUNK_HEADER_BYTES;
        for (List<SnapshotEntry> chunk : chunks) {
            int size = CHUNK_HEADER_BYTES;
            for (SnapshotEntry e : chunk) size += encodedSize(e);
            if (size > max) max = size;
        }
        return max;
    }

    private static int encodedSize(SnapshotEntry e) {
        byte[] blockBytes = e.blockName().getBytes(StandardCharsets.UTF_8);
        byte[] val = e.entry().value() != null ? e.entry().value() : new byte[0];
        // blockNameLen(2) + blockName + keyLen(2) + key + hlc(16) + expiryMs(8) + originNodeId(16) + valueLen(4) + value
        return 2 + blockBytes.length + 2 + e.key().length + HLCTimestamp.BYTES + 8 + 16 + 4 + val.length;
    }
}
