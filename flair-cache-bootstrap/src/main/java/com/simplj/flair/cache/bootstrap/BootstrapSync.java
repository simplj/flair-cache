package com.simplj.flair.cache.bootstrap;

import com.simplj.flair.cache.replication.ConflictResolver;
import com.simplj.flair.cache.replication.LWWResolver;
import com.simplj.flair.cache.store.CacheBlock;
import com.simplj.flair.cache.store.CacheEntry;
import com.simplj.flair.cache.transport.FrameHandler;
import com.simplj.flair.cache.transport.RawFrame;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Joiner-side bootstrap sync. Connects to a donor peer via {@link DonorSelector}, sends a
 * {@code SYNC_REQ}, and applies incoming {@code SYNC_CHUNK} frames to the local block map
 * using LWW conflict resolution. After {@code SYNC_DONE}, drains any buffered replication events.
 *
 * <p>Usage:</p>
 * <pre>{@code
 * ReplicationBuffer buffer = new ReplicationBuffer();
 * replicationEngine.onIncoming(buffer::offer);
 *
 * SyncResult result = BootstrapSync.builder()
 *     .blocks(Map.of("users", usersBlock, "orders", ordersBlock))
 *     .localNodeId(myNodeId)
 *     .donorAddress("10.0.0.1", 7890)
 *     .syncTimeoutMs(30_000)
 *     .replicationBuffer(buffer)
 *     .build()
 *     .syncFromPeer();
 * }</pre>
 */
public final class BootstrapSync {

    private static final Logger log = Logger.getLogger(BootstrapSync.class.getName());

    private final Map<String, CacheBlock<?, ?>> blocks;
    private final UUID localNodeId;
    private final DonorSelector donorSelector;
    private final long syncTimeoutMs;
    private final ConflictResolver conflictResolver;
    private final ReplicationBuffer replicationBuffer; // nullable

    private BootstrapSync(Builder b) {
        this.blocks            = b.blocks;
        this.localNodeId       = b.localNodeId;
        this.donorSelector     = b.donorSelector;
        this.syncTimeoutMs     = b.syncTimeoutMs;
        this.conflictResolver  = b.conflictResolver;
        this.replicationBuffer = b.replicationBuffer;
    }

    /**
     * Initiates sync from a donor peer. Blocks until the sync completes or
     * {@code syncTimeoutMs} elapses.
     *
     * @return {@link SyncResult} with totals and timing on success
     * @throws SyncTimeoutException if sync does not complete within the configured timeout
     * @throws IOException          if all donor seeds remain unreachable
     */
    public SyncResult syncFromPeer() throws SyncTimeoutException, IOException {
        long start = System.currentTimeMillis();

        if (replicationBuffer != null) {
            replicationBuffer.startBuffering();
        }

        CountDownLatch doneLatch = new CountDownLatch(1);
        AtomicLong totalEntries = new AtomicLong();
        AtomicInteger chunksReceived = new AtomicInteger();

        FrameHandler handler = (conn, frame) ->
                handleFrame(frame, doneLatch, totalEntries, chunksReceived);

        DonorSelector.DonorConnection donor = donorSelector.select(handler, syncTimeoutMs);
        try {
            donor.connection().send(ChunkEncoder.encodeSyncReq(localNodeId));
            log.info("Bootstrap SYNC_REQ sent to donor, localNodeId=" + localNodeId);

            long elapsed = System.currentTimeMillis() - start;
            long remaining = syncTimeoutMs - elapsed;
            boolean completed = remaining > 0 && doneLatch.await(remaining, TimeUnit.MILLISECONDS);
            if (!completed) {
                throw new SyncTimeoutException(
                        "Bootstrap sync timed out after " + syncTimeoutMs + "ms");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SyncTimeoutException("Bootstrap sync interrupted", e);
        } finally {
            donor.close();
        }

        if (replicationBuffer != null) {
            replicationBuffer.drainAndApply(blocks, conflictResolver);
        }

        long duration = System.currentTimeMillis() - start;
        log.info("Bootstrap sync complete: totalEntries=" + totalEntries.get()
                + " chunks=" + chunksReceived.get() + " durationMs=" + duration);
        return new SyncResult(totalEntries.get(), chunksReceived.get(), duration);
    }

    private void handleFrame(RawFrame frame, CountDownLatch doneLatch,
                             AtomicLong totalEntries, AtomicInteger chunksReceived) {
        byte type = frame.type();
        if (type == ChunkEncoder.TYPE_SYNC_CHUNK) {
            ChunkDecoder.DecodedChunk chunk = ChunkDecoder.decodeChunk(frame.payload());
            if (chunk == null) {
                log.warning("Failed to decode SYNC_CHUNK — skipping");
                return;
            }
            applyChunk(chunk.entries());
            chunksReceived.incrementAndGet();
            if (log.isLoggable(Level.FINEST)) {
                log.finest("Applied SYNC_CHUNK " + chunk.chunkIndex()
                        + "/" + chunk.totalChunks() + " (" + chunk.entries().size() + " entries)");
            }
        } else if (type == ChunkEncoder.TYPE_SYNC_DONE) {
            totalEntries.set(ChunkDecoder.decodeSyncDone(frame.payload()));
            log.fine("SYNC_DONE received: totalEntries=" + totalEntries.get());
            doneLatch.countDown();
        }
    }

    private void applyChunk(List<ChunkDecoder.ChunkEntry> entries) {
        for (ChunkDecoder.ChunkEntry e : entries) {
            CacheBlock<?, ?> block = blocks.get(e.blockName());
            if (block == null) continue;
            CacheEntry existing = block.getRaw(e.key());
            CacheEntry winner = (existing == null)
                    ? e.entry()
                    : conflictResolver.resolve(existing, e.entry());
            if (existing == null || winner != existing) {
                block.putRaw(e.key(), winner);
            }
        }
    }

    public static Builder builder() { return new Builder(); }

    public static final class Builder {

        private Map<String, CacheBlock<?, ?>> blocks;
        private UUID localNodeId = UUID.randomUUID();
        private DonorSelector donorSelector;
        private long syncTimeoutMs = 30_000L;
        private ConflictResolver conflictResolver = LWWResolver.INSTANCE;
        private ReplicationBuffer replicationBuffer;

        public Builder blocks(Map<String, CacheBlock<?, ?>> blocks) {
            this.blocks = Objects.requireNonNull(blocks, "blocks must not be null");
            return this;
        }

        public Builder localNodeId(UUID id) {
            this.localNodeId = Objects.requireNonNull(id, "localNodeId must not be null");
            return this;
        }

        public Builder donorSelector(DonorSelector selector) {
            this.donorSelector = Objects.requireNonNull(selector, "donorSelector must not be null");
            return this;
        }

        /** Convenience: configure a single-seed {@link DonorSelector} inline. */
        public Builder donorAddress(String host, int port) {
            this.donorSelector = DonorSelector.builder().seed(host, port).build();
            return this;
        }

        public Builder syncTimeoutMs(long ms) {
            if (ms <= 0) throw new IllegalArgumentException("syncTimeoutMs must be positive");
            this.syncTimeoutMs = ms;
            return this;
        }

        public Builder conflictResolver(ConflictResolver resolver) {
            this.conflictResolver = Objects.requireNonNull(resolver, "conflictResolver must not be null");
            return this;
        }

        public Builder replicationBuffer(ReplicationBuffer buffer) {
            this.replicationBuffer = buffer;
            return this;
        }

        public BootstrapSync build() {
            Objects.requireNonNull(blocks, "blocks must be set");
            Objects.requireNonNull(donorSelector, "donorSelector or donorAddress must be set");
            return new BootstrapSync(this);
        }
    }
}
