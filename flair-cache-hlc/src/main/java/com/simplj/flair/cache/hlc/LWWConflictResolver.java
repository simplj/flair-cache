package com.simplj.flair.cache.hlc;

import java.util.UUID;

public final class LWWConflictResolver {

    private LWWConflictResolver() {}

    /**
     * Returns true if the incoming entry should replace the existing one under Last-Write-Wins.
     * When timestamps tie exactly, the node with the lexicographically higher UUID wins,
     * guaranteeing deterministic conflict resolution without coordination.
     */
    public static boolean shouldReplace(HLCTimestamp existingTs, UUID existingNode,
                                        HLCTimestamp incomingTs, UUID incomingNode) {
        int cmp = incomingTs.compareTo(existingTs);
        if (cmp > 0) return true;
        if (cmp < 0) return false;
        return incomingNode.compareTo(existingNode) > 0;
    }
}
