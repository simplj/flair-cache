package com.simplj.flair.cache.gossip;

import java.util.List;

public final class GossipConfig {
    public final long   tickIntervalMs;
    public final long   probeTimeoutMs;
    public final long   indirectTimeoutMs;
    public final long   suspicionTimeoutMs;
    public final int    fanout;
    public final List<String> seedPeers;

    private GossipConfig(Builder b) {
        this.tickIntervalMs      = b.tickIntervalMs;
        this.probeTimeoutMs      = b.probeTimeoutMs;
        this.indirectTimeoutMs   = b.indirectTimeoutMs;
        this.suspicionTimeoutMs  = b.suspicionTimeoutMs;
        this.fanout              = b.fanout;
        this.seedPeers           = List.copyOf(b.seedPeers);
    }

    public static Builder builder() { return new Builder(); }

    public static final class Builder {
        private long         tickIntervalMs     = 500;
        private long         probeTimeoutMs     = 2000;
        private long         indirectTimeoutMs  = 4000;
        private long         suspicionTimeoutMs = 10000;
        private int          fanout             = 3;
        private List<String> seedPeers          = List.of();

        public Builder tickIntervalMs(long v)     { tickIntervalMs = v;     return this; }
        public Builder probeTimeoutMs(long v)     { probeTimeoutMs = v;     return this; }
        public Builder indirectTimeoutMs(long v)  { indirectTimeoutMs = v;  return this; }
        public Builder suspicionTimeoutMs(long v) { suspicionTimeoutMs = v; return this; }
        public Builder fanout(int v)              { fanout = v;             return this; }
        public Builder seedPeers(List<String> v)  { seedPeers = v;          return this; }

        public GossipConfig build() { return new GossipConfig(this); }
    }
}
