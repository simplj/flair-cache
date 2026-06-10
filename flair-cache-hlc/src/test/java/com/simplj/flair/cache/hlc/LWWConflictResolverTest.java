package com.simplj.flair.cache.hlc;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class LWWConflictResolverTest {

    private static final UUID NODE_A = new UUID(0L, 1L);  // lower UUID
    private static final UUID NODE_B = new UUID(0L, 2L);  // higher UUID

    @Test
    void newerLogical_replaces() {
        HLCTimestamp existing = new HLCTimestamp(1000, 0);
        HLCTimestamp incoming = new HLCTimestamp(2000, 0);
        assertTrue(LWWConflictResolver.shouldReplace(existing, NODE_A, incoming, NODE_A));
    }

    @Test
    void olderLogical_doesNotReplace() {
        HLCTimestamp existing = new HLCTimestamp(2000, 0);
        HLCTimestamp incoming = new HLCTimestamp(1000, 0);
        assertFalse(LWWConflictResolver.shouldReplace(existing, NODE_A, incoming, NODE_A));
    }

    @Test
    void sameLogical_higherCounter_replaces() {
        HLCTimestamp existing = new HLCTimestamp(1000, 1);
        HLCTimestamp incoming = new HLCTimestamp(1000, 2);
        assertTrue(LWWConflictResolver.shouldReplace(existing, NODE_A, incoming, NODE_A));
    }

    @Test
    void sameLogical_lowerCounter_doesNotReplace() {
        HLCTimestamp existing = new HLCTimestamp(1000, 5);
        HLCTimestamp incoming = new HLCTimestamp(1000, 3);
        assertFalse(LWWConflictResolver.shouldReplace(existing, NODE_A, incoming, NODE_A));
    }

    @Test
    void exactTie_higherNodeUuid_replaces() {
        HLCTimestamp ts = new HLCTimestamp(1000, 5);
        // NODE_B > NODE_A so incoming (NODE_B) should replace existing (NODE_A)
        assertTrue(LWWConflictResolver.shouldReplace(ts, NODE_A, ts, NODE_B));
    }

    @Test
    void exactTie_lowerNodeUuid_doesNotReplace() {
        HLCTimestamp ts = new HLCTimestamp(1000, 5);
        // NODE_A < NODE_B so incoming (NODE_A) should not replace existing (NODE_B)
        assertFalse(LWWConflictResolver.shouldReplace(ts, NODE_B, ts, NODE_A));
    }

    @Test
    void exactTie_sameNode_doesNotReplace() {
        HLCTimestamp ts = new HLCTimestamp(1000, 5);
        assertFalse(LWWConflictResolver.shouldReplace(ts, NODE_A, ts, NODE_A));
    }

    @Test
    void uuidTiebreaker_isDeterministic() {
        HLCTimestamp ts = new HLCTimestamp(5000, 0);
        UUID nodeA = UUID.fromString("00000000-0000-0000-0000-000000000001");
        UUID nodeB = UUID.fromString("00000000-0000-0000-0000-000000000002");

        boolean first  = LWWConflictResolver.shouldReplace(ts, nodeA, ts, nodeB);
        boolean second = LWWConflictResolver.shouldReplace(ts, nodeA, ts, nodeB);
        assertEquals(first, second, "Result must be deterministic for identical inputs");
        assertTrue(first, "Node with higher UUID must win the tie");
    }

    @Test
    void uuidTiebreaker_symmetric() {
        HLCTimestamp ts = new HLCTimestamp(5000, 0);
        // Exactly one of (A→B) or (B→A) must replace — never both, never neither
        boolean aToB = LWWConflictResolver.shouldReplace(ts, NODE_A, ts, NODE_B);
        boolean bToA = LWWConflictResolver.shouldReplace(ts, NODE_B, ts, NODE_A);
        assertTrue(aToB ^ bToA, "Exactly one direction must win the tie");
    }
}
