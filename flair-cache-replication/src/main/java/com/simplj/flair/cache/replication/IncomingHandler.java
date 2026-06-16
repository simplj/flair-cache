package com.simplj.flair.cache.replication;

import com.simplj.flair.cache.store.CacheBlock;
import com.simplj.flair.cache.store.CacheEntry;
import com.simplj.flair.cache.transport.Connection;
import com.simplj.flair.cache.transport.FrameHandler;
import com.simplj.flair.cache.transport.RawFrame;

import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;

final class IncomingHandler implements FrameHandler {

    private static final Logger log = Logger.getLogger(IncomingHandler.class.getName());

    private volatile ReplicationEngine engine;

    void setEngine(ReplicationEngine engine) {
        this.engine = engine;
    }

    @Override
    public void onFrame(Connection source, RawFrame frame) {
        ReplicationEngine e = engine;
        if (e == null) return;

        switch (frame.type()) {
            case FrameEncoder.TYPE_PUT:    handlePut(e, source, frame);    break;
            case FrameEncoder.TYPE_DELETE: handleDelete(e, source, frame); break;
            case FrameEncoder.TYPE_PING:   handlePing(e, source);          break;
            case FrameEncoder.TYPE_PONG:   handlePong(e, frame);           break;
            case FrameEncoder.TYPE_ACK:    handleAck(e, frame);            break;
            default:
                if (log.isLoggable(Level.FINEST)) {
                    log.finest("Unknown frame type: 0x" + Integer.toHexString(frame.type() & 0xFF));
                }
        }
    }

    private void handlePut(ReplicationEngine engine, Connection source, RawFrame frame) {
        FrameDecoder.DecodedPut decoded = FrameDecoder.decodePut(frame.payload());
        if (decoded == null) return;

        boolean applied = false;
        Function<String, CacheBlock<?, ?>> lookup = engine.blockLookup();
        if (lookup != null) {
            CacheBlock<?, ?> block = lookup.apply(decoded.blockName());
            if (block != null) {
                // Always advance local HLC with the remote timestamp — even if the write loses.
                block.updateClock(decoded.entry().hlc());
                CacheEntry existing = block.getRaw(decoded.key());
                CacheEntry winner;
                if (existing == null) {
                    winner = decoded.entry();
                } else {
                    winner = engine.conflictResolver().resolve(existing, decoded.entry());
                }
                // Write if there was no existing entry, or if the resolver chose not to keep existing.
                // Mark the calling thread as performing an incoming apply so that the PutListener
                // registered via attachBlock does not re-replicate the write back to peers.
                if (existing == null || winner != existing) {
                    ReplicationEngine.INCOMING.set(true);
                    try {
                        block.putRaw(decoded.key(), winner);
                    } finally {
                        ReplicationEngine.INCOMING.set(false);
                    }
                }
                applied = true;
            }
        }

        Consumer<ReplicationEvent> cb = engine.incomingCallback();
        if (cb != null) {
            try {
                // ConsistencyMode is meaningless for an incoming replicated event; EVENTUAL signals
                // "applied from wire — no local ACK tracking".
                cb.accept(ReplicationEvent.put(decoded.blockName(), decoded.key(),
                        decoded.entry(), ConsistencyMode.EVENTUAL));
            } catch (Exception ex) {
                log.log(Level.WARNING, "onIncoming callback threw", ex);
            }
        }

        // Only ACK if the write was actually applied — a phantom ACK from a node that doesn't
        // know the block would count toward QUORUM/STRONG without data being stored.
        if (decoded.needsAck() && applied) {
            source.send(FrameEncoder.encodeAck(decoded.frameId()));
            if (log.isLoggable(Level.FINEST)) {
                log.finest("Sent ACK frameId=" + decoded.frameId());
            }
        }
    }

    private void handleDelete(ReplicationEngine engine, Connection source, RawFrame frame) {
        FrameDecoder.DecodedDelete decoded = FrameDecoder.decodeDelete(frame.payload());
        if (decoded == null) return;

        boolean applied = false;
        Function<String, CacheBlock<?, ?>> lookup = engine.blockLookup();
        if (lookup != null) {
            CacheBlock<?, ?> block = lookup.apply(decoded.blockName());
            if (block != null) {
                // Sync HLC with the remote clock before applying — spec requires it for all frames.
                block.updateClock(decoded.hlc());

                // LWW guard: an out-of-order DELETE (older HLC than the entry it would remove)
                // must not silently clobber a newer PUT — that causes cluster divergence. Only
                // apply the delete if it is causally newer-or-equal to the existing entry.
                CacheEntry existing = block.getRaw(decoded.key());
                if (existing != null && existing.hlc().compareTo(decoded.hlc()) > 0) {
                    // Existing entry is newer — stale delete, skip it (but still ACK below so the
                    // sender's QUORUM/STRONG tracking completes; the delete was processed, just lost).
                    if (log.isLoggable(Level.FINEST)) {
                        log.finest("Rejected stale DELETE for key in block " + decoded.blockName()
                                + " — existing HLC newer than incoming");
                    }
                    applied = true;
                } else {
                    // Mark the calling thread so that the DeleteListener registered via attachBlock
                    // does not re-replicate this delete back to peers.
                    ReplicationEngine.INCOMING.set(true);
                    try {
                        block.deleteRaw(decoded.key());
                    } finally {
                        ReplicationEngine.INCOMING.set(false);
                    }
                    applied = true;
                }
            }
        }

        Consumer<ReplicationEvent> cb = engine.incomingCallback();
        if (cb != null) {
            try {
                cb.accept(ReplicationEvent.delete(decoded.blockName(), decoded.key(),
                        decoded.hlc(), decoded.originNodeId(), ConsistencyMode.EVENTUAL));
            } catch (Exception ex) {
                log.log(Level.WARNING, "onIncoming callback threw", ex);
            }
        }

        // Only ACK if the delete was actually applied — same reasoning as PUT.
        if (decoded.needsAck() && applied) {
            source.send(FrameEncoder.encodeAck(decoded.frameId()));
            if (log.isLoggable(Level.FINEST)) {
                log.finest("Sent ACK frameId=" + decoded.frameId());
            }
        }
    }

    private void handlePing(ReplicationEngine engine, Connection source) {
        // Respond regardless of whether we can parse the sender's ID — we know our own
        source.send(FrameEncoder.encodePong(engine.localNodeId()));
        if (log.isLoggable(Level.FINEST)) {
            log.finest("Received PING; sent PONG from " + engine.localNodeId());
        }
    }

    private void handlePong(ReplicationEngine engine, RawFrame frame) {
        UUID senderId = FrameDecoder.decodePingPong(frame.payload());
        if (senderId == null) return;
        PeerRegistry registry = engine.peerRegistry();
        if (registry != null) registry.onPong(senderId);
        if (log.isLoggable(Level.FINEST)) {
            log.finest("Received PONG from " + senderId);
        }
    }

    private void handleAck(ReplicationEngine engine, RawFrame frame) {
        long frameId = FrameDecoder.decodeAck(frame.payload());
        if (frameId >= 0) {
            engine.ackTracker().onAck(frameId);
        }
    }
}
