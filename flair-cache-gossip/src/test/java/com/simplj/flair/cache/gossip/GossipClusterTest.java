package com.simplj.flair.cache.gossip;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

@Timeout(30)
class GossipClusterTest {

    private final List<GossipNode> started = new CopyOnWriteArrayList<>();

    @AfterEach
    void tearDown() {
        for (GossipNode n : started) {
            try { n.shutdown(); } catch (Exception ignored) {}
        }
        started.clear();
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private GossipNode startNode(int port, List<String> seeds, MembershipListener listener)
            throws IOException {
        GossipNode node = GossipNode.builder()
                .nodeId(UUID.randomUUID())
                .bindAddress("127.0.0.1")
                .bindPort(port)
                .seedPeers(seeds)
                .tickIntervalMs(100)
                .probeTimeoutMs(300)
                .indirectTimeoutMs(600)
                .suspicionTimeoutMs(1000)
                .fanout(3)
                .listener(listener)
                .build();
        node.start();
        started.add(node);
        return node;
    }

    private GossipNode startNode(int port, List<String> seeds) throws IOException {
        return startNode(port, seeds, null);
    }

    private GossipNode startNodeWithId(UUID id, int port, List<String> seeds) throws IOException {
        GossipNode node = GossipNode.builder()
                .nodeId(id)
                .bindAddress("127.0.0.1")
                .bindPort(port)
                .seedPeers(seeds)
                .tickIntervalMs(100)
                .probeTimeoutMs(300)
                .indirectTimeoutMs(600)
                .suspicionTimeoutMs(1000)
                .fanout(3)
                .build();
        node.start();
        started.add(node);
        return node;
    }

    private static void awaitCondition(String description, long timeoutMs, Condition c) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (c.test()) return;
            Thread.sleep(50);
        }
        fail("Timeout waiting for: " + description);
    }

    @FunctionalInterface
    interface Condition { boolean test() throws Exception; }

    // ── 5-node cluster: join propagates to all ────────────────────────────────

    @Test
    void fiveNodes_joinPropagatesToAll() throws Exception {
        int basePort = 28001;
        List<String> seeds = List.of("127.0.0.1:" + basePort);

        GossipNode seed = startNode(basePort, List.of());
        for (int i = 1; i < 5; i++) {
            startNode(basePort + i, seeds);
        }

        awaitCondition("all 5 nodes see 5 alive members", 5000, () ->
                started.stream().allMatch(n -> n.members().alive().size() == 5));
    }

    // ── onJoin fires for new nodes ────────────────────────────────────────────

    @Test
    void onJoin_firesWhenPeerJoins() throws Exception {
        int basePort = 28010;
        AtomicInteger joinCount = new AtomicInteger();
        CountDownLatch joined = new CountDownLatch(3); // seed sees 3 peers join

        startNode(basePort, List.of(), new MembershipListener() {
            public void onJoin(NodeInfo node)    { joinCount.incrementAndGet(); joined.countDown(); }
            public void onSuspect(NodeInfo node) {}
            public void onRecover(NodeInfo node) {}
            public void onLeave(NodeInfo node)   {}
            public void onDead(NodeInfo node)    {}
        });

        List<String> seeds = List.of("127.0.0.1:" + basePort);
        for (int i = 1; i <= 3; i++) {
            startNode(basePort + i, seeds);
        }

        assertTrue(joined.await(5, TimeUnit.SECONDS),
                "seed did not receive 3 onJoin events; got " + joinCount.get());
    }

    // ── Graceful leave ────────────────────────────────────────────────────────

    @Test
    void gracefulLeave_peerRemovedByOthers() throws Exception {
        int basePort = 28020;
        List<String> seeds = List.of("127.0.0.1:" + basePort);

        GossipNode seed    = startNode(basePort, List.of());
        GossipNode leaver  = startNode(basePort + 1, seeds);
        GossipNode watcher = startNode(basePort + 2, seeds);

        awaitCondition("cluster of 3 formed", 5000, () ->
                seed.members().alive().size() == 3);

        // Capture leaver's UUID before shutdown
        int leaverPort = leaver.localPort();
        UUID leaverId = leaver.members().alive().stream()
                .filter(n -> n.port() == leaverPort)
                .findFirst().map(NodeInfo::id).orElseThrow();

        leaver.shutdown();
        started.remove(leaver);

        awaitCondition("watcher sees only 2 alive after leave", 5000, () ->
                watcher.members().alive().size() == 2
                && watcher.members().find(leaverId).isEmpty());
    }

    // ── Graceful leave fires onLeave ──────────────────────────────────────────

    @Test
    void gracefulLeave_onLeave_fires() throws Exception {
        int basePort = 28030;
        CountDownLatch leaveLatch = new CountDownLatch(1);

        startNode(basePort, List.of(), new MembershipListener() {
            public void onJoin(NodeInfo n)    {}
            public void onSuspect(NodeInfo n) {}
            public void onRecover(NodeInfo n) {}
            public void onLeave(NodeInfo n)   { leaveLatch.countDown(); }
            public void onDead(NodeInfo n)    {}
        });

        GossipNode leaver = startNode(basePort + 1, List.of("127.0.0.1:" + basePort));
        awaitCondition("2 nodes formed", 3000, () ->
                started.get(0).members().alive().size() == 2);

        leaver.shutdown();
        started.remove(leaver);

        assertTrue(leaveLatch.await(3, TimeUnit.SECONDS), "onLeave not fired");
    }

    // ── Node failure detection → DEAD ─────────────────────────────────────────

    @Test
    void nodeFailure_detectedAsDead() throws Exception {
        int basePort = 28040;
        List<String> seeds = List.of("127.0.0.1:" + basePort);

        GossipNode seed   = startNode(basePort, List.of());
        GossipNode victim = startNode(basePort + 1, seeds);
        GossipNode watcher = startNode(basePort + 2, seeds);

        // Record victim's UUID so we can check after it's gone from membership
        awaitCondition("3-node cluster formed", 5000, () ->
                seed.members().alive().size() == 3);

        UUID victimId = victim.members().alive().stream()
                .filter(n -> n.port() == victim.localPort())
                .findFirst()
                .map(NodeInfo::id)
                .orElseThrow();

        // Simulate crash — no LEAVE broadcast
        victim.simulateFail();
        started.remove(victim);

        // probeTimeout=300ms, indirectTimeout=600ms, suspicionTimeout=1000ms → total ~2s
        awaitCondition("victim declared dead (removed from alive)", 8000, () ->
                watcher.members().alive().stream().noneMatch(n -> n.id().equals(victimId))
                && seed.members().alive().stream().noneMatch(n -> n.id().equals(victimId)));
    }

    // ── Refutation: SUSPECTED node recovers ──────────────────────────────────

    @Test
    void refutation_suspectedNodeBecomesAlive() throws Exception {
        int basePort = 28050;
        CountDownLatch recoverLatch = new CountDownLatch(1);

        GossipNode seed    = startNode(basePort, List.of(), new MembershipListener() {
            public void onJoin(NodeInfo n)    {}
            public void onSuspect(NodeInfo n) {}
            public void onRecover(NodeInfo n) { recoverLatch.countDown(); }
            public void onLeave(NodeInfo n)   {}
            public void onDead(NodeInfo n)    {}
        });
        GossipNode suspect = startNode(basePort + 1, List.of("127.0.0.1:" + basePort));

        awaitCondition("2-node cluster formed", 3000, () ->
                seed.members().alive().size() == 2);

        UUID suspectId = suspect.members().alive().stream()
                .filter(n -> n.port() == suspect.localPort())
                .findFirst()
                .map(NodeInfo::id)
                .orElseThrow();

        // Inject SUSPECTED into seed's membership AND piggyback queue.
        // The next PING from seed to suspect carries SUSPECTED(suspect) piggybacked.
        // suspect sees itself SUSPECTED, increments incarnation, broadcasts ALIVE(suspect, newInc).
        // seed then reconciles via PONG piggybacking → onRecover fires.
        seed.injectSuspectForTest(suspectId);

        assertTrue(recoverLatch.await(5, TimeUnit.SECONDS),
                "onRecover never fired after refutation");
        assertEquals(NodeStatus.ALIVE, seed.members().find(suspectId)
                .map(NodeInfo::status).orElse(NodeStatus.DEAD));
    }

    // ── onDead fires after failure detection ──────────────────────────────────

    @Test
    void nodeFailure_onDead_fires() throws Exception {
        int basePort = 28070;
        CountDownLatch deadLatch = new CountDownLatch(1);

        GossipNode seed = startNode(basePort, List.of(), new MembershipListener() {
            public void onJoin(NodeInfo n)    {}
            public void onSuspect(NodeInfo n) {}
            public void onRecover(NodeInfo n) {}
            public void onLeave(NodeInfo n)   {}
            public void onDead(NodeInfo n)    { deadLatch.countDown(); }
        });
        GossipNode victim  = startNode(basePort + 1, List.of("127.0.0.1:" + basePort));
        GossipNode watcher = startNode(basePort + 2, List.of("127.0.0.1:" + basePort));

        awaitCondition("3-node cluster formed", 5000, () ->
                seed.members().alive().size() == 3);

        victim.simulateFail();
        started.remove(victim);

        assertTrue(deadLatch.await(8, TimeUnit.SECONDS), "onDead never fired after crash");
    }

    // ── Tombstone rejoin: crashed node re-admitted after being tombstoned ──────

    @Test
    void tombstoneRejoin_nodeRejoinableAfterDeath() throws Exception {
        int basePort = 28080;
        List<String> seeds = List.of("127.0.0.1:" + basePort);

        GossipNode seed   = startNode(basePort,     List.of());
        GossipNode victim = startNode(basePort + 1, seeds);

        awaitCondition("2-node cluster formed", 5000, () ->
                seed.members().alive().size() == 2);

        UUID victimId = victim.members().alive().stream()
                .filter(n -> n.port() == victim.localPort())
                .findFirst().map(NodeInfo::id).orElseThrow();

        victim.simulateFail();
        started.remove(victim);

        // Wait until seed tombstones the victim (removes from alive list)
        awaitCondition("victim declared dead and tombstoned", 8000, () ->
                seed.members().find(victimId).isEmpty());

        // Rejoin with the same UUID on a different port. The fresh IncarnationClock starts
        // at 0, which is <= the tombstone incarnation. handleJoin bumps to tombstone+1 so
        // the re-admission succeeds and stale DEAD deltas cannot kill the rejoiner.
        GossipNode rejoiner = startNodeWithId(victimId, basePort + 2, seeds);

        awaitCondition("rejoiner visible in seed's alive list", 6000, () ->
                seed.members().alive().stream().anyMatch(n -> n.id().equals(victimId)));
    }

    // ── Late joiner learns all members via piggybacking ───────────────────────

    @Test
    void piggybacking_lateJoinerLearnsAllMembers() throws Exception {
        int basePort = 28090;
        List<String> seeds = List.of("127.0.0.1:" + basePort);

        // Form a 3-node cluster first
        GossipNode n0 = startNode(basePort,     List.of());
        GossipNode n1 = startNode(basePort + 1, seeds);
        GossipNode n2 = startNode(basePort + 2, seeds);

        awaitCondition("3-node cluster formed", 5000, () ->
                n0.members().alive().size() == 3);

        // Late joiner only seeds off n0 but must learn about n1 and n2 via piggybacked deltas
        GossipNode late = startNode(basePort + 3, seeds);

        awaitCondition("late joiner sees all 4 members", 6000, () ->
                late.members().alive().size() == 4);
    }

    // ── 10-node cluster: membership stabilizes ───────────────────────────────

    @Test
    void tenNodes_membershipStabilizes() throws Exception {
        int basePort = 28060;
        List<String> seeds = List.of("127.0.0.1:" + basePort);

        startNode(basePort, List.of());
        for (int i = 1; i < 10; i++) {
            startNode(basePort + i, seeds);
            Thread.sleep(20); // slight stagger to reduce initial port contention
        }

        awaitCondition("all 10 nodes see 10 alive members", 10000, () ->
                started.stream().allMatch(n -> n.members().alive().size() == 10));
    }
}
