package com.simplj.flair.cache;

import com.simplj.flair.cache.replication.ConsistencyMode;
import com.simplj.flair.cache.transport.TlsConfig;

import java.util.List;
import java.util.UUID;

/**
 * Immutable configuration for a {@link FlairCache} instance.
 * Obtained via {@link FlairCache#builder()}.
 */
public final class FlairCacheConfig {

    private final UUID            nodeId;
    private final String          bindAddress;
    private final int             bindPort;
    private final List<String>    seedPeers;
    private final ConsistencyMode defaultConsistency;
    private final TlsConfig       tls;
    private final long            ackTimeoutMs;
    private final long            batchWindowMs;
    private final int             batchMaxFrames;
    private final long            keepaliveIntervalMs;
    private final long            keepalivePongTimeoutMs;
    private final int             selectorThreads;

    FlairCacheConfig(FlairCacheBuilder b) {
        this.nodeId                 = b.nodeId;
        this.bindAddress            = b.bindAddress;
        this.bindPort               = b.bindPort;
        this.seedPeers              = List.copyOf(b.seedPeers);
        this.defaultConsistency     = b.defaultConsistency;
        this.tls                    = b.tls;
        this.ackTimeoutMs           = b.ackTimeoutMs;
        this.batchWindowMs          = b.batchWindowMs;
        this.batchMaxFrames         = b.batchMaxFrames;
        this.keepaliveIntervalMs    = b.keepaliveIntervalMs;
        this.keepalivePongTimeoutMs = b.keepalivePongTimeoutMs;
        this.selectorThreads        = b.selectorThreads;
    }

    public UUID nodeId()                  { return nodeId; }
    public String bindAddress()           { return bindAddress; }
    public int bindPort()                 { return bindPort; }
    public List<String> seedPeers()       { return seedPeers; }
    public ConsistencyMode defaultConsistency() { return defaultConsistency; }
    public TlsConfig tls()                { return tls; }
    public long ackTimeoutMs()            { return ackTimeoutMs; }
    public long batchWindowMs()           { return batchWindowMs; }
    public int batchMaxFrames()           { return batchMaxFrames; }
    public long keepaliveIntervalMs()     { return keepaliveIntervalMs; }
    public long keepalivePongTimeoutMs()  { return keepalivePongTimeoutMs; }
    public int selectorThreads()          { return selectorThreads; }

    @Override
    public String toString() {
        return "FlairCacheConfig{nodeId=" + nodeId
                + ", bindAddress=" + bindAddress
                + ", bindPort=" + bindPort
                + ", seedPeers=" + seedPeers
                + ", defaultConsistency=" + defaultConsistency
                + ", tls.enabled=" + tls.isEnabled()
                + "}";
    }
}
