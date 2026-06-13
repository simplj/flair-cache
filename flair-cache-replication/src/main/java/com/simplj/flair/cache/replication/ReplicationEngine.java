package com.simplj.flair.cache.replication;

import com.simplj.flair.cache.gossip.GossipNode;
import com.simplj.flair.cache.gossip.NodeInfo;
import com.simplj.flair.cache.store.CacheBlock;
import com.simplj.flair.cache.transport.FrameHandler;
import com.simplj.flair.cache.transport.TcpServer;

import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.logging.Logger;

public final class ReplicationEngine {

    private static final Logger log = Logger.getLogger(ReplicationEngine.class.getName());

    private final UUID localNodeId;
    private final TcpServer transport;
    private final GossipNode cluster;
    private final ConflictResolver conflictResolver;
    private final Function<String, CacheBlock<?, ?>> blockLookup;
    private final long batchWindowMs;
    private final int batchMaxFrames;
    private final long ackTimeoutMs;
    private final long keepaliveIntervalMs;
    private final long keepalivePongTimeoutMs;

    private final IncomingHandler incomingHandler;
    private final AckTracker ackTracker;
    private final AtomicLong frameIdGen = new AtomicLong(0);
    private final AtomicBoolean started = new AtomicBoolean(false);

    private volatile PeerRegistry peerRegistry;
    private volatile ReplicationFanout fanout;
    private volatile ScheduledExecutorService keepaliveScheduler;
    private volatile Consumer<ReplicationEvent> incomingCallback;

    private ReplicationEngine(Builder b) {
        this.localNodeId      = b.localNodeId;
        this.transport        = b.transport;
        this.cluster          = b.cluster;
        this.conflictResolver = b.conflictResolver;
        this.blockLookup      = b.blockLookup;
        this.batchWindowMs          = b.batchWindowMs;
        this.batchMaxFrames         = b.batchMaxFrames;
        this.ackTimeoutMs           = b.ackTimeoutMs;
        this.keepaliveIntervalMs    = b.keepaliveIntervalMs;
        this.keepalivePongTimeoutMs = b.keepalivePongTimeoutMs;
        this.incomingHandler        = b.incomingHandler;
        this.ackTracker       = new AckTracker();
    }

    // ── Lifecycle ─────────────────────────────────────────────────────────────

    public void start() {
        if (!started.compareAndSet(false, true)) {
            throw new IllegalStateException("ReplicationEngine already started");
        }
        // Pass the server's event loop so all outgoing connections share the single
        // flaircache-nio-selector thread rather than each spawning their own.
        peerRegistry = new PeerRegistry(localNodeId, transport.eventLoop());
        fanout = new ReplicationFanout(peerRegistry, cluster.members(), localNodeId,
                batchWindowMs, batchMaxFrames);

        // Register PeerRegistry so join/leave/dead events trigger immediate connect/disconnect.
        // reconcilePeers() in ReplicationFanout provides polling-based fallback.
        cluster.addMembershipListener(peerRegistry);

        incomingHandler.setEngine(this);
        fanout.start();

        keepaliveScheduler = Executors.newSingleThreadScheduledExecutor(
                new FlairCacheThreadFactory("flaircache-keepalive", true));
        keepaliveScheduler.scheduleWithFixedDelay(this::keepaliveTick,
                keepaliveIntervalMs, keepaliveIntervalMs, TimeUnit.MILLISECONDS);

        // Connect to peers already known alive at startup
        List<NodeInfo> alive = cluster.members().alive();
        for (NodeInfo node : alive) {
            if (!node.id().equals(localNodeId)) {
                peerRegistry.connectAsync(node);
            }
        }

        log.info("ReplicationEngine started: localNodeId=" + localNodeId);
    }

    public void shutdown() {
        if (keepaliveScheduler != null) keepaliveScheduler.shutdownNow();
        if (fanout != null) fanout.shutdown();
        if (peerRegistry != null) peerRegistry.shutdown();
        ackTracker.shutdown();
        incomingHandler.setEngine(null);
        log.info("ReplicationEngine stopped");
    }

    private void keepaliveTick() {
        PeerRegistry registry = peerRegistry;
        if (registry == null) return;
        registry.disconnectStalePeers(keepalivePongTimeoutMs);
        registry.sendPingToAll(localNodeId);
    }

    // ── Replication API ───────────────────────────────────────────────────────

    /**
     * Replicates the event to all alive peers.
     * <ul>
     *   <li>EVENTUAL: non-blocking, returns immediately after enqueuing.</li>
     *   <li>QUORUM: blocks until a strict majority of all nodes (including self) have
     *       applied the write, i.e. until {@code ceil((alivePeers+1)/2)} peers ACK.</li>
     *   <li>STRONG: blocks until all alive peers ACK.</li>
     * </ul>
     *
     * @throws ReplicationTimeoutException if QUORUM/STRONG ACKs are not received in time
     */
    public void replicate(ReplicationEvent event) throws ReplicationTimeoutException {
        if (fanout == null) {
            throw new IllegalStateException("ReplicationEngine not started — call start() first");
        }
        ConsistencyMode mode = event.mode();

        if (mode == ConsistencyMode.EVENTUAL) {
            long frameId = frameIdGen.incrementAndGet();
            if (!fanout.offer(new ReplicationFanout.QueuedEvent(event, frameId, false))) {
                log.warning("Replication queue full — EVENTUAL frame dropped: frameId=" + frameId
                        + " block=" + event.blockName());
            }
            return;
        }

        // QUORUM / STRONG: snapshot alive peer count, register PendingWrite, then enqueue.
        long frameId = frameIdGen.incrementAndGet();
        int alivePeers = peerRegistry.aliveCount();

        int required;
        if (mode == ConsistencyMode.QUORUM) {
            // Quorum = strict majority of all nodes (self + peers).
            // totalNodes = alivePeers + 1 (self). Strict majority = floor(totalNodes/2) + 1.
            // Self has already written locally, so required peer ACKs = floor(totalNodes/2).
            // Java integer division truncates, so (alivePeers + 1) / 2 = floor(totalNodes / 2).
            // e.g. 51 nodes (50 peers): floor(51/2) = 25 peer ACKs → 26 of 51 total ✓
            // e.g.  5 nodes  (4 peers): floor( 5/2) =  2 peer ACKs →  3 of  5 total ✓
            // e.g.  4 nodes  (3 peers): floor( 4/2) =  2 peer ACKs →  3 of  4 total ✓
            // e.g.  3 nodes  (2 peers): floor( 3/2) =  1 peer ACK  →  2 of  3 total ✓
            required = (alivePeers + 1) / 2;
        } else {
            required = alivePeers; // STRONG: all peers must ACK
        }

        if (required <= 0) {
            // No alive peers — local write is sufficient; fire-and-forget.
            if (!fanout.offer(new ReplicationFanout.QueuedEvent(event, frameId, false))) {
                log.warning("Replication queue full — frame dropped: frameId=" + frameId
                        + " mode=" + mode + " block=" + event.blockName());
            }
            return;
        }

        long expiryMs = System.currentTimeMillis() + ackTimeoutMs;
        PendingWrite pw = new PendingWrite(frameId, required, expiryMs);
        ackTracker.track(pw);

        if (!fanout.offer(new ReplicationFanout.QueuedEvent(event, frameId, true))) {
            ackTracker.cancel(frameId);
            throw new ReplicationTimeoutException(
                    "Replication queue full — frame dropped: frameId=" + frameId
                    + " mode=" + mode + " block=" + event.blockName());
        }

        try {
            pw.future.get(ackTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            // Remove from pending so no late ACK can complete the abandoned future
            ackTracker.cancel(frameId);
            throw new ReplicationTimeoutException(
                    "Replication timed out after " + ackTimeoutMs + "ms for frameId=" + frameId
                    + " mode=" + mode + " required=" + required
                    + " received=" + pw.receivedAcks.get());
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof ReplicationTimeoutException) {
                throw (ReplicationTimeoutException) cause;
            }
            throw new ReplicationTimeoutException(
                    "Replication failed for frameId=" + frameId, cause);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            ackTracker.cancel(frameId);
            throw new ReplicationTimeoutException(
                    "Replication interrupted for frameId=" + frameId);
        }
    }

    /**
     * Registers a callback invoked after each incoming replication event is applied to the
     * local store. Intended for the watch layer and metrics. The callback must not block.
     * The event's {@link ReplicationEvent#mode()} is always {@link ConsistencyMode#EVENTUAL}
     * for incoming events — it reflects that the write came from the wire, not a local caller.
     */
    public void onIncoming(Consumer<ReplicationEvent> callback) {
        this.incomingCallback = callback;
    }

    // ── Package-private accessors ─────────────────────────────────────────────

    UUID localNodeId() { return localNodeId; }

    PeerRegistry peerRegistry() { return peerRegistry; }

    ConflictResolver conflictResolver() { return conflictResolver; }

    Function<String, CacheBlock<?, ?>> blockLookup() { return blockLookup; }

    Consumer<ReplicationEvent> incomingCallback() { return incomingCallback; }

    AckTracker ackTracker() { return ackTracker; }

    IncomingHandler incomingHandler() { return incomingHandler; }

    /** Initiate an outgoing connection to a peer. Used by bootstrap and tests. */
    void connectAsync(NodeInfo peer) {
        if (peerRegistry != null) peerRegistry.connectAsync(peer);
    }

    // ── Builder ───────────────────────────────────────────────────────────────

    public static Builder builder() { return new Builder(); }

    public static final class Builder {

        private UUID                              localNodeId           = UUID.randomUUID();
        private TcpServer                         transport;
        private GossipNode                        cluster;
        private ConflictResolver                  conflictResolver      = LWWResolver.INSTANCE;
        private Function<String, CacheBlock<?, ?>> blockLookup         = null;
        private long                              batchWindowMs         = 2L;
        private int                               batchMaxFrames        = 64;
        private long                              ackTimeoutMs          = 500L;
        private long                              keepaliveIntervalMs   = 5_000L;
        private long                              keepalivePongTimeoutMs = 15_000L;

        private final IncomingHandler incomingHandler = new IncomingHandler();

        private Builder() {}

        public Builder localNodeId(UUID nodeId) {
            this.localNodeId = Objects.requireNonNull(nodeId, "localNodeId must not be null");
            return this;
        }

        public Builder transport(TcpServer transport) {
            this.transport = Objects.requireNonNull(transport, "transport must not be null");
            return this;
        }

        public Builder cluster(GossipNode cluster) {
            this.cluster = Objects.requireNonNull(cluster, "cluster must not be null");
            return this;
        }

        public Builder conflictResolver(ConflictResolver resolver) {
            this.conflictResolver = Objects.requireNonNull(resolver, "conflictResolver must not be null");
            return this;
        }

        public Builder blockLookup(Function<String, CacheBlock<?, ?>> lookup) {
            this.blockLookup = Objects.requireNonNull(lookup, "blockLookup must not be null");
            return this;
        }

        public Builder batchWindowMs(long ms) {
            if (ms <= 0) throw new IllegalArgumentException("batchWindowMs must be positive");
            this.batchWindowMs = ms;
            return this;
        }

        public Builder batchMaxFrames(int frames) {
            if (frames <= 0) throw new IllegalArgumentException("batchMaxFrames must be positive");
            this.batchMaxFrames = frames;
            return this;
        }

        public Builder ackTimeoutMs(long ms) {
            if (ms <= 0) throw new IllegalArgumentException("ackTimeoutMs must be positive");
            this.ackTimeoutMs = ms;
            return this;
        }

        public Builder keepaliveIntervalMs(long ms) {
            if (ms <= 0) throw new IllegalArgumentException("keepaliveIntervalMs must be positive");
            this.keepaliveIntervalMs = ms;
            return this;
        }

        public Builder keepalivePongTimeoutMs(long ms) {
            if (ms <= 0) throw new IllegalArgumentException("keepalivePongTimeoutMs must be positive");
            this.keepalivePongTimeoutMs = ms;
            return this;
        }

        /**
         * Returns the {@link FrameHandler} to wire into {@link TcpServer.Builder#handler(FrameHandler)}.
         * Call this before building the {@link TcpServer} so incoming frames reach the engine.
         * The engine is activated on {@link ReplicationEngine#start()}.
         *
         * <pre>{@code
         * ReplicationEngine.Builder eb = ReplicationEngine.builder()
         *         .localNodeId(nodeId).cluster(gossip);
         *
         * TcpServer server = TcpServer.builder()
         *         .handler(eb.frameHandler()).port(7890).build();
         *
         * ReplicationEngine engine = eb.transport(server).build();
         * server.start();
         * engine.start();
         * }</pre>
         */
        public FrameHandler frameHandler() {
            return incomingHandler;
        }

        public ReplicationEngine build() {
            Objects.requireNonNull(transport,    "transport must be set");
            Objects.requireNonNull(cluster,      "cluster must be set");
            Objects.requireNonNull(blockLookup,  "blockLookup must be set");
            return new ReplicationEngine(this);
        }
    }
}
