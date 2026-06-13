package com.simplj.flair.cache.replication;

import com.simplj.flair.cache.hlc.HLCTimestamp;
import com.simplj.flair.cache.store.CacheEntry;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertSame;

class LWWResolverTest {

    // Two deterministic UUIDs for tiebreak tests.
    // NODE_HIGH has a larger MSB so it sorts higher than NODE_LOW.
    private static final UUID NODE_LOW  = new UUID(0L, 1L);
    private static final UUID NODE_HIGH = new UUID(1L, 0L);

    // ── Logical clock ordering ────────────────────────────────────────────────

    @Test
    void incoming_higher_logical_wins() {
        CacheEntry existing = entry(10, 0, NODE_LOW);
        CacheEntry incoming = entry(20, 0, NODE_LOW);
        assertSame(incoming, LWWResolver.INSTANCE.resolve(existing, incoming));
    }

    @Test
    void existing_higher_logical_wins() {
        CacheEntry existing = entry(20, 0, NODE_LOW);
        CacheEntry incoming = entry(10, 0, NODE_LOW);
        assertSame(existing, LWWResolver.INSTANCE.resolve(existing, incoming));
    }

    // ── Counter tiebreak (same logical) ──────────────────────────────────────

    @Test
    void same_logical_incoming_higher_counter_wins() {
        CacheEntry existing = entry(10, 1, NODE_LOW);
        CacheEntry incoming = entry(10, 2, NODE_LOW);
        assertSame(incoming, LWWResolver.INSTANCE.resolve(existing, incoming));
    }

    @Test
    void same_logical_existing_higher_counter_wins() {
        CacheEntry existing = entry(10, 5, NODE_LOW);
        CacheEntry incoming = entry(10, 3, NODE_LOW);
        assertSame(existing, LWWResolver.INSTANCE.resolve(existing, incoming));
    }

    // ── NodeId tiebreak (same HLC) ────────────────────────────────────────────

    @Test
    void same_hlc_incoming_higher_nodeId_wins() {
        HLCTimestamp hlc = new HLCTimestamp(10L, 0L);
        CacheEntry existing = entryWithHlc(hlc, NODE_LOW);
        CacheEntry incoming = entryWithHlc(hlc, NODE_HIGH);
        assertSame(incoming, LWWResolver.INSTANCE.resolve(existing, incoming));
    }

    @Test
    void same_hlc_existing_higher_nodeId_wins() {
        HLCTimestamp hlc = new HLCTimestamp(10L, 0L);
        CacheEntry existing = entryWithHlc(hlc, NODE_HIGH);
        CacheEntry incoming = entryWithHlc(hlc, NODE_LOW);
        assertSame(existing, LWWResolver.INSTANCE.resolve(existing, incoming));
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static CacheEntry entry(long logical, long counter, UUID nodeId) {
        return new CacheEntry(new byte[0], new HLCTimestamp(logical, counter), 0L, 0L, 0L, nodeId);
    }

    private static CacheEntry entryWithHlc(HLCTimestamp hlc, UUID nodeId) {
        return new CacheEntry(new byte[0], hlc, 0L, 0L, 0L, nodeId);
    }
}
