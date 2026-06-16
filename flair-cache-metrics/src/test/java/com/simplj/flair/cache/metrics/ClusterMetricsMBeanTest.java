package com.simplj.flair.cache.metrics;

import com.simplj.flair.cache.gossip.GossipNode;
import com.simplj.flair.cache.gossip.NodeInfo;
import com.simplj.flair.cache.gossip.NodeStatus;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class ClusterMetricsMBeanTest {

    private static GossipNode newNode() throws IOException {
        // build() allocates internal data structures but does NOT bind a UDP socket;
        // start() is the method that binds. A non-started node is safe for unit tests.
        return GossipNode.builder().build();
    }

    private NodeInfo aliveNode() throws UnknownHostException {
        return new NodeInfo(UUID.randomUUID(), InetAddress.getByName("127.0.0.1"),
                7890, NodeStatus.ALIVE, 1L, System.currentTimeMillis());
    }

    @Test
    void nullGossipNodeThrowsNullPointerException() {
        assertThrows(NullPointerException.class, () -> new ClusterMetricsMBean(null));
    }

    @Test
    void deadCountInitiallyZero() throws IOException {
        GossipNode node = newNode();
        ClusterMetricsMBean bean = new ClusterMetricsMBean(node);
        assertEquals(0L, bean.getDeadNodeCount());
    }

    @Test
    void onDeadIncrementsDeadCount() throws Exception {
        GossipNode node = newNode();
        ClusterMetricsMBean bean = new ClusterMetricsMBean(node);

        NodeInfo dead = aliveNode();
        bean.onDead(dead);
        bean.onDead(aliveNode());

        assertEquals(2L, bean.getDeadNodeCount());
    }

    @Test
    void onLeaveDoesNotIncrementDeadCount() throws Exception {
        GossipNode node = newNode();
        ClusterMetricsMBean bean = new ClusterMetricsMBean(node);

        bean.onLeave(aliveNode()); // voluntary leave must not count as a death

        assertEquals(0L, bean.getDeadNodeCount());
    }

    @Test
    void onJoinOnSuspectOnRecoverAreNoOps() throws Exception {
        GossipNode node = newNode();
        ClusterMetricsMBean bean = new ClusterMetricsMBean(node);

        bean.onJoin(aliveNode());
        bean.onSuspect(aliveNode());
        bean.onRecover(aliveNode());

        assertEquals(0L, bean.getDeadNodeCount());
    }

    @Test
    void bootstrapSyncDurationMsDefaultZeroAndSettable() throws IOException {
        GossipNode node = newNode();
        ClusterMetricsMBean bean = new ClusterMetricsMBean(node);

        assertEquals(0L, bean.getBootstrapSyncDurationMs());
        bean.setBootstrapSyncDurationMs(1500L);
        assertEquals(1500L, bean.getBootstrapSyncDurationMs());
    }

    @Test
    void aliveAndSuspectedCountsDelegateToMembershipList() throws IOException {
        GossipNode node = newNode();
        ClusterMetricsMBean bean = new ClusterMetricsMBean(node);

        // A non-started node has an empty membership list
        assertEquals(0, bean.getAliveNodeCount());
        assertEquals(0, bean.getSuspectedNodeCount());
    }

    @Test
    void gossipTickCountInitiallyZero() throws IOException {
        GossipNode node = newNode();
        ClusterMetricsMBean bean = new ClusterMetricsMBean(node);
        assertEquals(0L, bean.getGossipTickCount());
    }
}
