package com.simplj.flair.cache.metrics;

import com.simplj.flair.cache.gossip.GossipNode;
import com.simplj.flair.cache.gossip.MembershipListener;
import com.simplj.flair.cache.gossip.NodeInfo;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

public final class ClusterMetricsMBean implements ClusterMetricsMBeanInterface, MembershipListener {

    private final GossipNode gossipNode;
    private final LongAdder deadCount             = new LongAdder();
    private final AtomicLong bootstrapSyncMs      = new AtomicLong(0L);

    ClusterMetricsMBean(GossipNode gossipNode) {
        this.gossipNode = Objects.requireNonNull(gossipNode, "gossipNode must not be null");
    }

    public void setBootstrapSyncDurationMs(long durationMs) {
        bootstrapSyncMs.set(durationMs);
    }

    @Override public int  getAliveNodeCount()          { return gossipNode.members().alive().size(); }
    @Override public int  getSuspectedNodeCount()      { return gossipNode.members().suspected().size(); }
    @Override public long getDeadNodeCount()           { return deadCount.sum(); }
    @Override public long getGossipTickCount()         { return gossipNode.gossipTickCount(); }
    @Override public long getBootstrapSyncDurationMs() { return bootstrapSyncMs.get(); }

    // MembershipListener — count only failure-detected deaths, not voluntary leaves
    @Override public void onJoin(NodeInfo node)    {}
    @Override public void onSuspect(NodeInfo node) {}
    @Override public void onRecover(NodeInfo node) {}
    @Override public void onLeave(NodeInfo node)   {}
    @Override public void onDead(NodeInfo node)    { deadCount.increment(); }
}
