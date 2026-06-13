package com.simplj.flair.cache.replication;

import com.simplj.flair.cache.hlc.HLCTimestamp;
import com.simplj.flair.cache.store.CacheEntry;

import java.util.UUID;

public sealed interface ReplicationEvent permits ReplicationEvent.PutEvent, ReplicationEvent.DeleteEvent {

    String blockName();
    byte[] key();
    ConsistencyMode mode();

    record PutEvent(
            String blockName,
            byte[] key,
            CacheEntry entry,
            ConsistencyMode mode
    ) implements ReplicationEvent {}

    record DeleteEvent(
            String blockName,
            byte[] key,
            HLCTimestamp hlc,
            UUID originNodeId,
            ConsistencyMode mode
    ) implements ReplicationEvent {}

    static PutEvent put(String blockName, byte[] key, CacheEntry entry, ConsistencyMode mode) {
        return new PutEvent(blockName, key, entry, mode);
    }

    static DeleteEvent delete(String blockName, byte[] key, HLCTimestamp hlc,
                              UUID originNodeId, ConsistencyMode mode) {
        return new DeleteEvent(blockName, key, hlc, originNodeId, mode);
    }
}
