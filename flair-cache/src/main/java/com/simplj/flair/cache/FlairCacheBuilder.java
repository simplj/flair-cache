package com.simplj.flair.cache;

import com.simplj.flair.cache.replication.ConsistencyMode;
import com.simplj.flair.cache.transport.TlsConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * Fluent builder for {@link FlairCache}. Obtain via {@link FlairCache#builder()}.
 *
 * <p>All fields have production-ready defaults; only {@link #bindPort(int)} and
 * {@link #seedPeers(List)} typically need to be set per deployment.</p>
 */
public final class FlairCacheBuilder {

    // Package-visible: FlairCacheConfig reads these directly to avoid exposing getters
    // that would make the builder appear to be a config object.
    UUID            nodeId                 = UUID.randomUUID();
    String          bindAddress            = "0.0.0.0";
    int             bindPort               = 7890;
    List<String>    seedPeers              = new ArrayList<>();
    ConsistencyMode defaultConsistency     = ConsistencyMode.QUORUM;
    TlsConfig       tls                    = TlsConfig.disabled();
    long            ackTimeoutMs           = 500L;
    long            batchWindowMs          = 2L;
    int             batchMaxFrames         = 64;
    long            keepaliveIntervalMs    = 5_000L;
    long            keepalivePongTimeoutMs = 15_000L;
    int             selectorThreads        = 1;

    FlairCacheBuilder() {}

    /** Optional — auto-generated UUID used if not specified. */
    public FlairCacheBuilder nodeId(UUID nodeId) {
        this.nodeId = Objects.requireNonNull(nodeId, "nodeId must not be null");
        return this;
    }

    /** Network address to bind. Default: {@code "0.0.0.0"} (all interfaces). */
    public FlairCacheBuilder bindAddress(String bindAddress) {
        this.bindAddress = Objects.requireNonNull(bindAddress, "bindAddress must not be null");
        return this;
    }

    /** TCP and UDP port to bind. Default: {@code 7890}. */
    public FlairCacheBuilder bindPort(int bindPort) {
        if (bindPort <= 0 || bindPort > 65535) {
            throw new IllegalArgumentException("bindPort must be in [1, 65535]");
        }
        this.bindPort = bindPort;
        return this;
    }

    /**
     * Seed peer addresses in {@code "host:port"} format. Used for both gossip JOIN
     * and replication peer discovery. Omit (or pass an empty list) for a standalone node.
     */
    public FlairCacheBuilder seedPeers(List<String> seedPeers) {
        Objects.requireNonNull(seedPeers, "seedPeers must not be null");
        this.seedPeers = new ArrayList<>(seedPeers);
        return this;
    }

    /** Default consistency mode applied to all blocks unless overridden per-block. Default: {@link ConsistencyMode#QUORUM}. */
    public FlairCacheBuilder consistency(ConsistencyMode consistency) {
        this.defaultConsistency = Objects.requireNonNull(consistency, "consistency must not be null");
        return this;
    }

    /** TLS configuration for the TCP server. Default: {@link TlsConfig#disabled()}. */
    public FlairCacheBuilder tls(TlsConfig tls) {
        this.tls = Objects.requireNonNull(tls, "tls must not be null");
        return this;
    }

    /** Milliseconds to wait for QUORUM/STRONG ACKs before throwing. Default: {@code 500}. */
    public FlairCacheBuilder ackTimeoutMs(long ms) {
        if (ms <= 0) throw new IllegalArgumentException("ackTimeoutMs must be positive");
        this.ackTimeoutMs = ms;
        return this;
    }

    /** Replication batch flush window. Default: {@code 2}ms. */
    public FlairCacheBuilder batchWindowMs(long ms) {
        if (ms <= 0) throw new IllegalArgumentException("batchWindowMs must be positive");
        this.batchWindowMs = ms;
        return this;
    }

    /** Maximum frames per replication batch. Default: {@code 64}. */
    public FlairCacheBuilder batchMaxFrames(int frames) {
        if (frames <= 0) throw new IllegalArgumentException("batchMaxFrames must be positive");
        this.batchMaxFrames = frames;
        return this;
    }

    /**
     * Number of NIO selector threads. Default: {@code 1} (sufficient for clusters up to ~20 nodes).
     * Increase to 2–4 for clusters of 50–100+ nodes.
     */
    public FlairCacheBuilder selectorThreads(int threads) {
        if (threads < 1) throw new IllegalArgumentException("selectorThreads must be >= 1");
        this.selectorThreads = threads;
        return this;
    }

    /** Creates the {@link FlairCacheConfig} and wraps it in a {@link FlairCache} ready to {@link FlairCache#start()}. */
    public FlairCache build() {
        return new FlairCache(new FlairCacheConfig(this));
    }
}
