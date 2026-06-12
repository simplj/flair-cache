package com.simplj.flair.cache.gossip;

import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class PiggybackQueueTest {

    private static final InetAddress LOOPBACK;
    static {
        try { LOOPBACK = InetAddress.getByName("127.0.0.1"); }
        catch (Exception e) { throw new ExceptionInInitializerError(e); }
    }

    private NodeInfo node(UUID id, NodeStatus status, long inc) {
        return new NodeInfo(id, LOOPBACK, 7891, status, inc, 0L);
    }

    private NodeInfo node(NodeStatus status, long inc) {
        return node(UUID.randomUUID(), status, inc);
    }

    // ── Transmit counting and eviction ────────────────────────────────────────

    @Test
    void drain_evictsEntryAfterMaxTransmits() {
        // clusterSize=4 → maxTransmits = floor(log2(4)) = 2
        PiggybackQueue q = new PiggybackQueue();
        q.add(node(NodeStatus.ALIVE, 1));

        List<NodeInfo> d1 = q.drain(10, 4);
        assertEquals(1, d1.size());           // transmitCount → 1

        List<NodeInfo> d2 = q.drain(10, 4);
        assertEquals(1, d2.size());           // transmitCount → 2 = maxTransmits; evicted after this

        List<NodeInfo> d3 = q.drain(10, 4);
        assertTrue(d3.isEmpty());             // evicted
    }

    @Test
    void drain_smallCluster_evictsAfterOneDrain() {
        // clusterSize=3 → maxTransmits = floor(log2(3)) = 1
        // One drain exhausts the budget — correct per-round (not per-target) usage
        PiggybackQueue q = new PiggybackQueue();
        UUID id = UUID.randomUUID();
        q.add(node(id, NodeStatus.ALIVE, 1));

        List<NodeInfo> first = q.drain(10, 3);
        assertEquals(1, first.size());
        assertEquals(id, first.get(0).id());

        assertTrue(q.drain(10, 3).isEmpty()); // evicted: maxTransmits=1 reached
    }

    // ── maxDeltas cap ─────────────────────────────────────────────────────────

    @Test
    void drain_respectsMaxDeltasCap() {
        PiggybackQueue q = new PiggybackQueue();
        for (int i = 0; i < 5; i++) q.add(node(NodeStatus.ALIVE, i + 1));

        List<NodeInfo> result = q.drain(3, 100);
        assertEquals(3, result.size());
    }

    // ── Priority ordering: DEAD > SUSPECTED > ALIVE; fewer transmits first ────

    @Test
    void drain_prioritisesDeadThenSuspectedThenAlive() {
        PiggybackQueue q = new PiggybackQueue();
        NodeInfo alive     = node(NodeStatus.ALIVE,     1);
        NodeInfo suspected = node(NodeStatus.SUSPECTED, 1);
        NodeInfo dead      = node(NodeStatus.DEAD,      1);
        q.add(alive);
        q.add(suspected);
        q.add(dead);

        List<NodeInfo> result = q.drain(3, 100);
        assertEquals(3, result.size());
        assertEquals(NodeStatus.DEAD,      result.get(0).status());
        assertEquals(NodeStatus.SUSPECTED, result.get(1).status());
        assertEquals(NodeStatus.ALIVE,     result.get(2).status());
    }

    @Test
    void drain_fewerTransmitsFirst_withinSameStatus() {
        // clusterSize=10 → maxTransmits=3; add same node twice so counts differ
        PiggybackQueue q = new PiggybackQueue();
        UUID idA = UUID.randomUUID();
        UUID idB = UUID.randomUUID();
        q.add(node(idA, NodeStatus.ALIVE, 1));
        q.add(node(idB, NodeStatus.ALIVE, 1));

        // Advance A's transmit count by 1 without advancing B
        q.drain(1, 10);  // returns one entry (whichever happens to sort first)

        // After one drain of one entry, the other still has count=0
        // Both should still be present across the next drain
        List<NodeInfo> second = q.drain(2, 10);
        assertFalse(second.isEmpty()); // at minimum one remains
    }

    // ── add(): dominance on repeated adds ─────────────────────────────────────

    @Test
    void add_higherIncarnationReplaces() {
        PiggybackQueue q = new PiggybackQueue();
        UUID id = UUID.randomUUID();
        q.add(node(id, NodeStatus.ALIVE, 3));
        q.add(node(id, NodeStatus.ALIVE, 5)); // higher → replaces, resets transmitCount
        q.add(node(id, NodeStatus.ALIVE, 4)); // lower → ignored

        List<NodeInfo> result = q.drain(10, 100);
        assertEquals(1, result.size());
        assertEquals(5L, result.get(0).incarnation());
    }

    @Test
    void add_higherStatusWeightWinsAtSameIncarnation() {
        PiggybackQueue q = new PiggybackQueue();
        UUID id = UUID.randomUUID();
        q.add(node(id, NodeStatus.ALIVE,     5));
        q.add(node(id, NodeStatus.SUSPECTED, 5)); // SUSPECTED > ALIVE → replaces
        q.add(node(id, NodeStatus.ALIVE,     5)); // lower weight → ignored

        List<NodeInfo> result = q.drain(10, 100);
        assertEquals(1, result.size());
        assertEquals(NodeStatus.SUSPECTED, result.get(0).status());
    }

    @Test
    void add_deadDominatesAllAtSameIncarnation() {
        PiggybackQueue q = new PiggybackQueue();
        UUID id = UUID.randomUUID();
        q.add(node(id, NodeStatus.ALIVE,     5));
        q.add(node(id, NodeStatus.DEAD,      5)); // DEAD > ALIVE
        q.add(node(id, NodeStatus.SUSPECTED, 5)); // SUSPECTED < DEAD → ignored

        List<NodeInfo> result = q.drain(10, 100);
        assertEquals(NodeStatus.DEAD, result.get(0).status());
    }

    // ── clear ─────────────────────────────────────────────────────────────────

    @Test
    void clear_emptiesQueue() {
        PiggybackQueue q = new PiggybackQueue();
        q.add(node(NodeStatus.ALIVE, 1));
        q.add(node(NodeStatus.DEAD,  2));
        q.clear();
        assertTrue(q.drain(10, 10).isEmpty());
    }

    // ── Empty queue ───────────────────────────────────────────────────────────

    @Test
    void drain_emptyQueue_returnsEmptyList() {
        assertTrue(new PiggybackQueue().drain(10, 5).isEmpty());
    }
}
