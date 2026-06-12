package com.simplj.flair.cache.gossip;

import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class MembershipListTest {

    private static final InetAddress LOOPBACK;
    static {
        try { LOOPBACK = InetAddress.getByName("127.0.0.1"); }
        catch (Exception e) { throw new ExceptionInInitializerError(e); }
    }

    private NodeInfo node(UUID id, NodeStatus status, long inc) {
        return new NodeInfo(id, LOOPBACK, 7891, status, inc, 0L);
    }

    // ── Basic add / find ──────────────────────────────────────────────────────

    @Test
    void addOrUpdate_newNode_returnsTrue() {
        MembershipList ml = new MembershipList();
        UUID id = UUID.randomUUID();
        assertTrue(ml.addOrUpdate(node(id, NodeStatus.ALIVE, 1)));
        assertTrue(ml.find(id).isPresent());
    }

    @Test
    void find_unknownId_returnsEmpty() {
        assertTrue(new MembershipList().find(UUID.randomUUID()).isEmpty());
    }

    // ── Tombstone: admission blocked when inc <= tombstone ────────────────────

    @Test
    void addOrUpdate_blockedByTombstone_lowerInc() {
        MembershipList ml = new MembershipList();
        UUID id = UUID.randomUUID();
        ml.addOrUpdate(node(id, NodeStatus.ALIVE, 5));
        ml.remove(id);                                   // tombstone=5

        assertFalse(ml.addOrUpdate(node(id, NodeStatus.ALIVE, 3)));  // 3 <= 5 blocked
        assertFalse(ml.addOrUpdate(node(id, NodeStatus.ALIVE, 5)));  // 5 <= 5 blocked
        assertTrue(ml.addOrUpdate(node(id, NodeStatus.ALIVE, 6)));   // 6 > 5 admitted
    }

    @Test
    void addOrUpdate_tombstoneAtZero_blocksZeroInc() {
        // Regression: original `<` change allowed inc=N when tombstone=N; correct is `<=`
        MembershipList ml = new MembershipList();
        UUID id = UUID.randomUUID();
        ml.addOrUpdate(node(id, NodeStatus.ALIVE, 0));
        ml.remove(id);                                   // tombstone=0

        assertFalse(ml.addOrUpdate(node(id, NodeStatus.ALIVE, 0)));  // 0 <= 0 blocked
        assertTrue(ml.addOrUpdate(node(id, NodeStatus.ALIVE, 1)));   // 1 > 0 admitted
    }

    @Test
    void addOrUpdate_tombstoneCleared_afterHigherIncAdmission() {
        MembershipList ml = new MembershipList();
        UUID id = UUID.randomUUID();
        ml.addOrUpdate(node(id, NodeStatus.ALIVE, 3));
        ml.remove(id);                                   // tombstone=3
        assertNotNull(ml.getTombstone(id));

        ml.addOrUpdate(node(id, NodeStatus.ALIVE, 5));  // admitted at inc=5
        assertNull(ml.getTombstone(id));                 // tombstone cleared
    }

    // ── Dominance: higher incarnation always wins ─────────────────────────────

    @Test
    void addOrUpdate_higherIncarnationWins() {
        MembershipList ml = new MembershipList();
        UUID id = UUID.randomUUID();
        ml.addOrUpdate(node(id, NodeStatus.ALIVE, 3));

        assertFalse(ml.addOrUpdate(node(id, NodeStatus.ALIVE,     2)));  // stale
        assertFalse(ml.addOrUpdate(node(id, NodeStatus.SUSPECTED, 2)));  // stale inc
        assertTrue(ml.addOrUpdate(node(id, NodeStatus.SUSPECTED,  4)));  // higher inc wins
        assertEquals(4, ml.find(id).get().incarnation());
    }

    // ── Dominance: same incarnation uses status weight ────────────────────────

    @Test
    void addOrUpdate_sameInc_statusWeightDeterminesWinner() {
        MembershipList ml = new MembershipList();
        UUID id = UUID.randomUUID();
        ml.addOrUpdate(node(id, NodeStatus.ALIVE, 5));

        assertFalse(ml.addOrUpdate(node(id, NodeStatus.ALIVE,     5)));  // same → no change
        assertTrue(ml.addOrUpdate(node(id, NodeStatus.SUSPECTED,  5)));  // SUSPECTED > ALIVE
        assertFalse(ml.addOrUpdate(node(id, NodeStatus.ALIVE,     5)));  // ALIVE can't override SUSPECTED
        assertTrue(ml.addOrUpdate(node(id, NodeStatus.DEAD,       5)));  // DEAD > SUSPECTED
    }

    // ── clearSuspicion ────────────────────────────────────────────────────────

    @Test
    void clearSuspicion_suspectedNode_promotesToAlive() {
        MembershipList ml = new MembershipList();
        UUID id = UUID.randomUUID();
        ml.addOrUpdate(node(id, NodeStatus.SUSPECTED, 5));

        assertTrue(ml.clearSuspicion(id));
        NodeInfo n = ml.find(id).orElseThrow();
        assertEquals(NodeStatus.ALIVE, n.status());
        assertEquals(5, n.incarnation());   // incarnation unchanged
    }

    @Test
    void clearSuspicion_aliveNode_isNoop() {
        MembershipList ml = new MembershipList();
        UUID id = UUID.randomUUID();
        ml.addOrUpdate(node(id, NodeStatus.ALIVE, 3));
        assertFalse(ml.clearSuspicion(id));
        assertEquals(NodeStatus.ALIVE, ml.find(id).get().status());
    }

    @Test
    void clearSuspicion_missingNode_isNoop() {
        assertFalse(new MembershipList().clearSuspicion(UUID.randomUUID()));
    }

    // ── writeTombstone ────────────────────────────────────────────────────────

    @Test
    void writeTombstone_highestIncarnationWins() {
        MembershipList ml = new MembershipList();
        UUID id = UUID.randomUUID();

        ml.writeTombstone(id, 5);
        assertEquals(5L, ml.getTombstone(id));

        ml.writeTombstone(id, 3);           // lower → ignored
        assertEquals(5L, ml.getTombstone(id));

        ml.writeTombstone(id, 8);           // higher → replaces
        assertEquals(8L, ml.getTombstone(id));
    }

    @Test
    void writeTombstone_blocksSubsequentAliveAtSameOrLowerInc() {
        MembershipList ml = new MembershipList();
        UUID id = UUID.randomUUID();
        ml.writeTombstone(id, 5);

        assertFalse(ml.addOrUpdate(node(id, NodeStatus.ALIVE, 4)));  // 4 <= 5 blocked
        assertFalse(ml.addOrUpdate(node(id, NodeStatus.ALIVE, 5)));  // 5 <= 5 blocked
        assertTrue(ml.addOrUpdate(node(id, NodeStatus.ALIVE, 6)));   // 6 > 5 admitted
    }

    // ── removeIfSuspected ─────────────────────────────────────────────────────

    @Test
    void removeIfSuspected_onlyWhenSuspected() {
        MembershipList ml = new MembershipList();
        UUID id = UUID.randomUUID();

        assertFalse(ml.removeIfSuspected(id));  // missing

        ml.addOrUpdate(node(id, NodeStatus.ALIVE, 3));
        assertFalse(ml.removeIfSuspected(id));  // ALIVE — rejected
        assertTrue(ml.find(id).isPresent());    // still there

        ml.addOrUpdate(node(id, NodeStatus.SUSPECTED, 3));
        assertTrue(ml.removeIfSuspected(id));   // SUSPECTED — removed
        assertTrue(ml.find(id).isEmpty());
    }

    @Test
    void removeIfSuspected_writesTombstone() {
        MembershipList ml = new MembershipList();
        UUID id = UUID.randomUUID();
        ml.addOrUpdate(node(id, NodeStatus.SUSPECTED, 7));
        ml.removeIfSuspected(id);
        assertEquals(7L, ml.getTombstone(id));
    }

    // ── remove ────────────────────────────────────────────────────────────────

    @Test
    void remove_writesTombstoneAtRemovedIncarnation() {
        MembershipList ml = new MembershipList();
        UUID id = UUID.randomUUID();
        ml.addOrUpdate(node(id, NodeStatus.ALIVE, 9));
        assertTrue(ml.remove(id));
        assertEquals(9L, ml.getTombstone(id));
        assertTrue(ml.find(id).isEmpty());
    }

    @Test
    void remove_absentNode_returnsFalse() {
        MembershipList ml = new MembershipList();
        assertFalse(ml.remove(UUID.randomUUID()));
    }

    // ── Size and view queries ─────────────────────────────────────────────────

    @Test
    void size_alive_suspected_counts() {
        MembershipList ml = new MembershipList();
        UUID a = UUID.randomUUID(), b = UUID.randomUUID(), c = UUID.randomUUID();
        ml.addOrUpdate(new NodeInfo(a, LOOPBACK, 1, NodeStatus.ALIVE,     1, 0L));
        ml.addOrUpdate(new NodeInfo(b, LOOPBACK, 2, NodeStatus.SUSPECTED, 1, 0L));
        ml.addOrUpdate(new NodeInfo(c, LOOPBACK, 3, NodeStatus.ALIVE,     1, 0L));

        assertEquals(3, ml.size());
        assertEquals(2, ml.alive().size());
        assertEquals(1, ml.suspected().size());
    }

    @Test
    void randomAlive_excludesSelfAndNonAlive() {
        MembershipList ml = new MembershipList();
        UUID self = UUID.randomUUID();
        UUID peer = UUID.randomUUID();
        UUID susp = UUID.randomUUID();
        ml.addOrUpdate(new NodeInfo(self, LOOPBACK, 1, NodeStatus.ALIVE,     1, 0L));
        ml.addOrUpdate(new NodeInfo(peer, LOOPBACK, 2, NodeStatus.ALIVE,     1, 0L));
        ml.addOrUpdate(new NodeInfo(susp, LOOPBACK, 3, NodeStatus.SUSPECTED, 1, 0L));

        java.util.List<NodeInfo> result = ml.randomAlive(10, self);
        assertEquals(1, result.size());
        assertEquals(peer, result.get(0).id());
    }
}
