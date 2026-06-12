package com.simplj.flair.cache.gossip;

public interface MembershipListener {
    void onJoin(NodeInfo node);
    void onSuspect(NodeInfo node);
    void onRecover(NodeInfo node);
    void onLeave(NodeInfo node);
    void onDead(NodeInfo node);
}
