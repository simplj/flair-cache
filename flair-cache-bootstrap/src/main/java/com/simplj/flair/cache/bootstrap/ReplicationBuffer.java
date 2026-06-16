package com.simplj.flair.cache.bootstrap;

import com.simplj.flair.cache.replication.ConflictResolver;
import com.simplj.flair.cache.replication.ReplicationEngine;
import com.simplj.flair.cache.replication.ReplicationEvent;
import com.simplj.flair.cache.store.CacheBlock;
import com.simplj.flair.cache.store.CacheEntry;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Captures incoming replication events during the bootstrap sync window and replays them
 * with LWW conflict resolution after the snapshot is fully applied.
 *
 * <p>Wire this into the {@code ReplicationEngine} before starting bootstrap:</p>
 * <pre>{@code
 * ReplicationBuffer buffer = new ReplicationBuffer();
 * replicationEngine.onIncoming(buffer::offer);
 *
 * BootstrapSync sync = BootstrapSync.builder()
 *     .blocks(blocks)
 *     .replicationBuffer(buffer)
 *     ...
 *     .build();
 * SyncResult result = sync.syncFromPeer();
 * }</pre>
 *
 * <p>When IncomingHandler is already applying events to the store before this callback fires,
 * {@link #drainAndApply} re-applies them idempotently via LWW, ensuring snapshot writes
 * cannot silently overwrite newer replication events.</p>
 */
public final class ReplicationBuffer {

    private final Queue<ReplicationEvent> buffer = new ConcurrentLinkedQueue<>();
    private volatile boolean buffering = false;

    public void startBuffering() {
        buffering = true;
    }

    /**
     * Called by the {@code onIncoming} callback in {@code ReplicationEngine}.
     * Captures the event while buffering; no-op otherwise.
     */
    public void offer(ReplicationEvent event) {
        if (buffering) {
            buffer.offer(event);
        }
    }

    /**
     * Stops buffering and discards all captured events without applying them. Call this when
     * bootstrap is abandoned so the buffer does not grow indefinitely after a failed sync.
     */
    public void stopBuffering() {
        buffering = false;
        buffer.clear();
    }

    /**
     * Stops buffering and applies all captured events to the block map using LWW resolution.
     * Safe to call from any thread; concurrent {@link #offer} calls during drain are also safe.
     */
    public void drainAndApply(Map<String, CacheBlock<?, ?>> blocks, ConflictResolver resolver) {
        buffering = false;
        ReplicationEvent event;
        while ((event = buffer.poll()) != null) {
            applyEvent(event, blocks, resolver);
        }
    }

    private static void applyEvent(ReplicationEvent event,
                                   Map<String, CacheBlock<?, ?>> blocks,
                                   ConflictResolver resolver) {
        CacheBlock<?, ?> block = blocks.get(event.blockName());
        if (block == null) return;

        // Mark replay as INCOMING so PutListener/DeleteListener registered via
        // ReplicationEngine.attachBlock do not re-replicate buffered events back to peers.
        ReplicationEngine.markIncoming(true);
        try {
            if (event instanceof ReplicationEvent.PutEvent put) {
                CacheEntry existing = block.getRaw(put.key());
                CacheEntry winner = (existing == null)
                        ? put.entry()
                        : resolver.resolve(existing, put.entry());
                if (existing == null || winner != existing) {
                    block.putRaw(put.key(), winner);
                }
            } else if (event instanceof ReplicationEvent.DeleteEvent del) {
                // LWW guard: reject stale deletes that arrived in the buffer before
                // a newer snapshot entry was applied by applyChunk(). Without this guard,
                // a DELETE(hlc=t2) buffered before a snapshot PUT(hlc=t3) would silently
                // delete the newer entry during drain, which is the mirror of the PUT LWW
                // check above.
                CacheEntry existing = block.getRaw(del.key());
                if (existing == null || existing.hlc().compareTo(del.hlc()) <= 0) {
                    block.deleteRaw(del.key());
                }
            }
        } finally {
            ReplicationEngine.markIncoming(false);
        }
    }
}
