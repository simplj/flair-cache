package com.simplj.flair.cache.bootstrap;

import com.simplj.flair.cache.store.CacheBlock;
import com.simplj.flair.cache.store.CacheEntry;
import com.simplj.flair.cache.transport.Connection;
import com.simplj.flair.cache.transport.FrameHandler;
import com.simplj.flair.cache.transport.RawFrame;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Donor-side bootstrap handler. Implement or compose with the TCP server's {@link FrameHandler}.
 *
 * <p>On receiving a {@code SYNC_REQ} frame it takes a point-in-time snapshot of all registered
 * {@link CacheBlock}s, partitions the entries into {@code SYNC_CHUNK} frames, and streams them
 * to the joiner on a dedicated {@code flaircache-bootstrap-sync} thread, followed by
 * {@code SYNC_DONE}. The donor does not pause writes during streaming — the snapshot is a copy.</p>
 *
 * <p>Example registration alongside a replication engine:</p>
 * <pre>{@code
 * BootstrapServer bootstrapServer = new BootstrapServer(blocks, 65536);
 * FrameHandler composite = (conn, frame) -> {
 *     replicationFrameHandler.onFrame(conn, frame);
 *     bootstrapServer.onFrame(conn, frame);
 * };
 * TcpServer server = TcpServer.builder().handler(composite).port(7890).build();
 * }</pre>
 */
public final class BootstrapServer implements FrameHandler {

    private static final Logger log = Logger.getLogger(BootstrapServer.class.getName());

    private final Map<String, CacheBlock<?, ?>> blocks;
    private final int chunkSizeBytes;

    public BootstrapServer(Map<String, CacheBlock<?, ?>> blocks, int chunkSizeBytes) {
        this.blocks = Objects.requireNonNull(blocks, "blocks must not be null");
        if (chunkSizeBytes <= 0) throw new IllegalArgumentException("chunkSizeBytes must be positive");
        this.chunkSizeBytes = chunkSizeBytes;
    }

    @Override
    public void onFrame(Connection source, RawFrame frame) {
        if (frame.type() != ChunkEncoder.TYPE_SYNC_REQ) return;
        UUID requestingNode = ChunkDecoder.decodeSyncReq(frame.payload());
        if (requestingNode == null) {
            log.warning("Received malformed SYNC_REQ — ignoring");
            return;
        }
        if (log.isLoggable(Level.FINE)) {
            log.fine("SYNC_REQ from nodeId=" + requestingNode);
        }
        Thread streamer = new FlairCacheThreadFactory("flaircache-bootstrap-sync")
                .newThread(() -> streamSnapshot(source, requestingNode));
        streamer.start();
    }

    private void streamSnapshot(Connection connection, UUID requestingNode) {
        try {
            List<ChunkEncoder.SnapshotEntry> entries = collectSnapshot();

            List<List<ChunkEncoder.SnapshotEntry>> chunks =
                    ChunkEncoder.partition(entries, chunkSizeBytes);
            int totalChunks = chunks.size();

            // Pre-allocate one buffer per streaming session, reused across all chunks.
            int bufCapacity = chunks.isEmpty()
                    ? chunkSizeBytes
                    : ChunkEncoder.maxChunkBufferSize(chunks, chunkSizeBytes);
            ByteBuffer buf = ByteBuffer.allocate(bufCapacity);

            for (int i = 0; i < totalChunks; i++) {
                connection.send(ChunkEncoder.encodeChunk(i, totalChunks, chunks.get(i), buf));
                if (log.isLoggable(Level.FINEST)) {
                    log.finest("Sent SYNC_CHUNK " + i + "/" + totalChunks
                            + " (" + chunks.get(i).size() + " entries) to " + requestingNode);
                }
            }

            connection.send(ChunkEncoder.encodeSyncDone(entries.size()));
            log.info("Bootstrap sync complete: " + entries.size() + " entries in "
                    + totalChunks + " chunks → nodeId=" + requestingNode);

        } catch (Exception e) {
            log.log(Level.WARNING, "Error during bootstrap sync to " + requestingNode, e);
        }
    }

    private List<ChunkEncoder.SnapshotEntry> collectSnapshot() {
        List<ChunkEncoder.SnapshotEntry> entries = new ArrayList<>();
        for (Map.Entry<String, CacheBlock<?, ?>> blockEntry : blocks.entrySet()) {
            String blockName = blockEntry.getKey();
            Map<byte[], CacheEntry> raw = blockEntry.getValue().rawSnapshotEntries();
            for (Map.Entry<byte[], CacheEntry> e : raw.entrySet()) {
                entries.add(new ChunkEncoder.SnapshotEntry(blockName, e.getKey(), e.getValue()));
            }
        }
        return entries;
    }
}
