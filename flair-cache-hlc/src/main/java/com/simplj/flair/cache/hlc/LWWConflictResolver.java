package com.simplj.flair.cache.hlc;

import java.util.UUID;

public final class LWWConflictResolver {

    private LWWConflictResolver() {}

    /**
     * Returns true if the incoming entry should replace the existing one under Last-Write-Wins.
     * When timestamps tie exactly, the node with the lexicographically higher UUID wins,
     * guaranteeing deterministic conflict resolution without coordination.
     */
    // Sentinel used when an entry's originNodeId is null (written before replication was wired in).
    // Must match ChunkEncoder.NULL_NODE_ID so bootstrap and replication tie-breaks are consistent.
    private static final UUID NULL_NODE_ID = new UUID(0L, 0L);

    public static boolean shouldReplace(HLCTimestamp existingTs, UUID existingNode,
                                        HLCTimestamp incomingTs, UUID incomingNode) {
        int cmp = incomingTs.compareTo(existingTs);
        if (cmp > 0) return true;
        if (cmp < 0) return false;
        UUID effExisting = existingNode != null ? existingNode : NULL_NODE_ID;
        UUID effIncoming = incomingNode != null ? incomingNode : NULL_NODE_ID;
        return effIncoming.compareTo(effExisting) > 0;
    }
}
