package com.simplj.flair.cache.replication;

import com.simplj.flair.cache.store.CacheBlock;
import com.simplj.flair.cache.store.CacheEntry;
import com.simplj.flair.cache.transport.Connection;
import com.simplj.flair.cache.transport.FrameHandler;
import com.simplj.flair.cache.transport.RawFrame;

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
                if (existing == null || winner != existing) {
                    block.putRaw(decoded.key(), winner);
                }
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

        if (decoded.needsAck()) {
            source.send(FrameEncoder.encodeAck(decoded.frameId()));
            if (log.isLoggable(Level.FINEST)) {
                log.finest("Sent ACK frameId=" + decoded.frameId());
            }
        }
    }

    private void handleDelete(ReplicationEngine engine, Connection source, RawFrame frame) {
        FrameDecoder.DecodedDelete decoded = FrameDecoder.decodeDelete(frame.payload());
        if (decoded == null) return;

        Function<String, CacheBlock<?, ?>> lookup = engine.blockLookup();
        if (lookup != null) {
            CacheBlock<?, ?> block = lookup.apply(decoded.blockName());
            if (block != null) {
                // Sync HLC with the remote clock before applying — spec requires it for all frames
                block.updateClock(decoded.hlc());
                block.deleteRaw(decoded.key());
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

        if (decoded.needsAck()) {
            source.send(FrameEncoder.encodeAck(decoded.frameId()));
            if (log.isLoggable(Level.FINEST)) {
                log.finest("Sent ACK frameId=" + decoded.frameId());
            }
        }
    }

    private void handleAck(ReplicationEngine engine, RawFrame frame) {
        long frameId = FrameDecoder.decodeAck(frame.payload());
        if (frameId >= 0) {
            engine.ackTracker().onAck(frameId);
        }
    }
}
