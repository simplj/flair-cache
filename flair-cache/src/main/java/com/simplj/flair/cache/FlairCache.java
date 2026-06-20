package com.simplj.flair.cache;

import com.simplj.flair.cache.bootstrap.BootstrapServer;
import com.simplj.flair.cache.bootstrap.BootstrapSync;
import com.simplj.flair.cache.bootstrap.DonorSelector;
import com.simplj.flair.cache.bootstrap.ReplicationBuffer;
import com.simplj.flair.cache.bootstrap.SyncTimeoutException;
import com.simplj.flair.cache.dsl.DataRegistry;
import com.simplj.flair.cache.dsl.Decoder;
import com.simplj.flair.cache.dsl.QueryEngine;
import com.simplj.flair.cache.gossip.GossipNode;
import com.simplj.flair.cache.gossip.MembershipList;
import com.simplj.flair.cache.hlc.HybridLogicalClock;
import com.simplj.flair.cache.metrics.MetricsRegistry;
import com.simplj.flair.cache.metrics.ReplicationMetricsMBean;
import com.simplj.flair.cache.replication.ReplicationEvent;
import com.simplj.flair.cache.replication.ConsistencyMode;
import com.simplj.flair.cache.replication.ReplicationEngine;
import com.simplj.flair.cache.serial.Codec;
import com.simplj.flair.cache.store.CacheBlock;
import com.simplj.flair.cache.store.EvictionPolicy;
import com.simplj.flair.cache.transport.FrameHandler;
import com.simplj.flair.cache.transport.TcpServer;
import com.simplj.flair.cache.watch.ChangeEvent;
import com.simplj.flair.cache.watch.WatchRegistry;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.nio.ByteBuffer;
import java.util.Enumeration;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Single entry point for the FlairCache distributed in-memory cache.
 *
 * <p>Obtain an instance via {@link #builder()}, configure it, then call {@link #start()}.
 * Register typed cache blocks with {@link #registerBlock(String)}, run DSL queries with
 * {@link #query()}, and clean up with {@link #shutdown()} (or use try-with-resources via
 * {@link Closeable}).</p>
 *
 * <pre>{@code
 * FlairCache cache = FlairCache.builder()
 *     .bindPort(7890)
 *     .seedPeers(List.of("10.0.0.1:7890"))
 *     .build()
 *     .start();
 *
 * CacheBlock<String, byte[]> items = cache.<String, byte[]>registerBlock("items")
 *     .keyCodec(stringCodec)
 *     .valueCodec(bytesCodec)
 *     .ttl(Duration.ofMinutes(10))
 *     .build();
 *
 * items.put("k1", data);
 * byte[] v = items.get("k1");
 *
 * cache.shutdown();
 * }</pre>
 */
public final class FlairCache implements Closeable {

    private static final Logger log = Logger.getLogger(FlairCache.class.getName());

    private static final long SHUTDOWN_FLUSH_TIMEOUT_MS  = 5_000L;
    private static final int  BOOTSTRAP_CHUNK_BYTES      = 65_536;
    private static final long BOOTSTRAP_SYNC_TIMEOUT_MS  = 30_000L;

    private final FlairCacheConfig config;
    private final MetricsRegistry  metricsRegistry;
    private final ConcurrentHashMap<String, CacheBlock<?, ?>> blocks;
    private final ConcurrentHashMap<String, WatchRegistry<?, ?>> watchRegistries;

    // Guards against concurrent start() calls. Set on entry to start(); never reset.
    private final AtomicBoolean started = new AtomicBoolean(false);
    // True only after start() completes successfully; cleared on shutdown().
    private final AtomicBoolean running = new AtomicBoolean(false);
    // Serializes concurrent bootstrapSync() calls: two simultaneous syncs would race on
    // onIncoming(), each overwriting the other's ReplicationBuffer callback.
    private final ReentrantLock bootstrapSyncLock = new ReentrantLock();

    // Initialized in start(); volatile for safe publication to other threads.
    private volatile HybridLogicalClock      hlc;
    private volatile TcpServer               tcpServer;
    private volatile GossipNode              gossipNode;
    private volatile ReplicationEngine       replicationEngine;
    private volatile ReplicationMetricsMBean replMetrics;

    // Package-private: FlairCacheBuilder.build() calls this; not for direct instantiation.
    FlairCache(FlairCacheConfig config) {
        this.config          = config;
        this.metricsRegistry = new MetricsRegistry();
        this.blocks          = new ConcurrentHashMap<>();
        this.watchRegistries = new ConcurrentHashMap<>();
    }

    // ── Public API ────────────────────────────────────────────────────────────

    /**
     * Boots all modules in strict dependency order. Blocks until the cluster is ready.
     * If any step fails, all previously started components are shut down cleanly before
     * the exception propagates.
     *
     * @return {@code this} for method chaining: {@code FlairCache.builder()...build().start()}
     * @throws IOException          if the TCP server or gossip socket cannot bind
     * @throws IllegalStateException if already started
     */
    public FlairCache start() throws IOException {
        if (!started.compareAndSet(false, true)) {
            throw new IllegalStateException("FlairCache already started");
        }
        List<Runnable> cleanup = new ArrayList<>();
        try {
            doStart(cleanup);
            running.set(true);
            log.info("FlairCache started: " + config);
            return this;
        } catch (Exception e) {
            // Roll back all successfully started components in reverse order.
            for (int i = cleanup.size() - 1; i >= 0; i--) {
                try { cleanup.get(i).run(); }
                catch (Exception ex) {
                    log.log(Level.WARNING, "Error during startup rollback", ex);
                }
            }
            if (e instanceof IOException) throw (IOException) e;
            throw new RuntimeException("FlairCache startup failed", e);
        }
    }

    /**
     * Returns the immutable configuration snapshot for this instance.
     * Available before {@link #start()} is called.
     */
    public FlairCacheConfig config() {
        return config;
    }

    /**
     * Returns a fluent builder for registering a new typed cache block under {@code name}.
     * The block is wired into replication, metrics, and watch when {@link BlockBuilder#build()}
     * is called. Each name must be unique within this instance — attempting to register a second
     * block under the same name throws {@link IllegalStateException} from {@code build()}.
     *
     * @throws IllegalStateException if the cache is not running
     */
    public <K, V> BlockBuilder<K, V> registerBlock(String name) {
        Objects.requireNonNull(name, "block name must not be null");
        checkRunning();
        return new BlockBuilder<>(name, this);
    }

    /**
     * Returns the already-registered {@link CacheBlock} for {@code name}.
     * The caller is responsible for supplying the correct {@code <K,V>} type parameters —
     * they must match those used when the block was originally registered via
     * {@link #registerBlock(String)}.
     *
     * @throws IllegalArgumentException if no block is registered under {@code name}
     * @throws IllegalStateException    if the cache is not running
     */
    @SuppressWarnings("unchecked")
    public <K, V> CacheBlock<K, V> registeredBlock(String name) {
        Objects.requireNonNull(name, "block name must not be null");
        checkRunning();
        CacheBlock<?, ?> block = blocks.get(name);
        if (block == null) {
            throw new IllegalArgumentException("No block registered as: '" + name + "'");
        }
        return (CacheBlock<K, V>) block;
    }

    /**
     * Returns a {@link QueryEngine} backed by a point-in-time snapshot of all registered blocks.
     * Each call creates a fresh engine over the current data — share and reuse the engine
     * within a single logical query batch for consistent results.
     *
     * @throws IllegalStateException if the cache is not running
     */
    public QueryEngine query() {
        checkRunning();
        DataRegistry registry = new DataRegistry();
        for (Map.Entry<String, CacheBlock<?, ?>> e : blocks.entrySet()) {
            @SuppressWarnings("unchecked")
            Map<Object, Object> snapshot = (Map<Object, Object>) e.getValue().snapshot();
            registry.register(e.getKey(), snapshot);
        }
        return QueryEngine.over(registry);
    }

    /**
     * Returns a typed {@link QueryEngine} over a named block's live snapshot, applying
     * {@code decoder} to every value before exposing it to the DSL.
     * Use this when the block's value type (e.g. {@code byte[]}) differs from the query type.
     *
     * <p>If {@code decoder.decode(v)} returns {@code null} for any entry, this method throws
     * {@link NullPointerException} identifying the key. Use
     * {@link #queryWith(String, Decoder, boolean) queryWith(name, decoder, true)} to
     * silently skip null-decoded entries instead.</p>
     *
     * @throws IllegalArgumentException if no block is registered under {@code blockName}
     * @throws NullPointerException     if decoder returns null for any entry value
     * @throws IllegalStateException    if the cache is not running
     */
    public <T> QueryEngine queryWith(String blockName, Decoder<T> decoder) {
        return queryWith(blockName, decoder, false);
    }

    /**
     * Returns a typed {@link QueryEngine} over a named block's live snapshot, applying
     * {@code decoder} to every value before exposing it to the DSL.
     *
     * @param skipNullResults if {@code true}, entries where {@code decoder.decode(v)} returns
     *                        {@code null} are excluded from the query engine silently;
     *                        if {@code false}, a {@code null} decode result throws
     *                        {@link NullPointerException} identifying the offending key
     * @throws IllegalArgumentException if no block is registered under {@code blockName}
     * @throws NullPointerException     if {@code skipNullResults} is false and decoder returns null
     * @throws IllegalStateException    if the cache is not running
     */
    public <T> QueryEngine queryWith(String blockName, Decoder<T> decoder, boolean skipNullResults) {
        checkRunning();
        Objects.requireNonNull(decoder, "decoder must not be null");
        CacheBlock<?, ?> block = blocks.get(blockName);
        if (block == null) {
            throw new IllegalArgumentException("No block registered as: '" + blockName + "'");
        }
        @SuppressWarnings("unchecked")
        Map<Object, Object> raw = (Map<Object, Object>) block.snapshot();
        Map<Object, Object> decoded = new LinkedHashMap<>(raw.size());
        for (Map.Entry<Object, Object> entry : raw.entrySet()) {
            Object result = decoder.decode(entry.getValue());
            if (result == null) {
                if (skipNullResults) continue;
                throw new NullPointerException(
                        "decoder returned null for key '" + entry.getKey() + "' in block '" + blockName + "'");
            }
            decoded.put(entry.getKey(), result);
        }
        DataRegistry registry = new DataRegistry();
        registry.register(blockName, decoded);
        return QueryEngine.over(registry);
    }

    /**
     * Returns the watch API for the named block.
     * Listeners registered here receive typed PUT/DELETE/EXPIRE events for that block.
     *
     * @throws IllegalArgumentException if no block is registered under {@code blockName}
     * @throws IllegalStateException    if the cache is not running
     */
    @SuppressWarnings("unchecked")
    public <K, V> WatchRegistry<K, V> watchRegistry(String blockName) {
        checkRunning();
        WatchRegistry<?, ?> registry = watchRegistries.get(blockName);
        if (registry == null) {
            throw new IllegalArgumentException("No block registered as: '" + blockName + "'");
        }
        return (WatchRegistry<K, V>) registry;
    }

    /** Returns the JMX metrics registry. Always non-null regardless of running state. */
    public MetricsRegistry metrics() {
        return metricsRegistry;
    }

    /**
     * Returns the current cluster membership view.
     *
     * @throws IllegalStateException if the cache is not running
     */
    public MembershipList cluster() {
        checkRunning();
        return gossipNode.members();
    }

    /** Returns {@code true} if {@link #start()} has completed and {@link #shutdown()} has not been called. */
    public boolean isRunning() {
        return running.get();
    }

    /**
     * Initiates a point-in-time state transfer from a donor peer for the named blocks.
     * Call this after building blocks when joining a cluster that already holds data.
     * If no seed peers are configured this method returns immediately (standalone node).
     *
     * <p>During sync, live replication events from peers are buffered internally and
     * re-applied after the snapshot transfer completes, ensuring no updates are lost.</p>
     *
     * @param blockNames block names to sync; if empty, all registered blocks are synced
     * @throws SyncTimeoutException     if the donor does not complete transfer within 30 s
     * @throws IOException              if all configured seed peers are unreachable
     * @throws IllegalArgumentException if any named block is not registered
     * @throws IllegalStateException    if the cache is not running
     */
    public void bootstrapSync(String... blockNames) throws SyncTimeoutException, IOException {
        checkRunning();
        List<String> seeds = config.seedPeers();
        if (seeds.isEmpty()) return;

        Map<String, CacheBlock<?, ?>> syncBlocks = new LinkedHashMap<>();
        if (blockNames.length == 0) {
            syncBlocks.putAll(blocks);
        } else {
            for (String name : blockNames) {
                CacheBlock<?, ?> block = blocks.get(name);
                if (block == null) {
                    throw new IllegalArgumentException("No block registered as: '" + name + "'");
                }
                syncBlocks.put(name, block);
            }
        }
        if (syncBlocks.isEmpty()) return;

        DonorSelector.Builder donorBuilder = DonorSelector.builder();
        boolean hasSeed = false;
        for (String peer : seeds) {
            int colon = peer.lastIndexOf(':');
            if (colon < 0) {
                log.warning("bootstrapSync: ignoring malformed seed peer '" + peer + "' (expected host:port)");
                continue;
            }
            String host = peer.substring(0, colon);
            int port;
            try {
                port = Integer.parseInt(peer.substring(colon + 1));
            } catch (NumberFormatException e) {
                log.warning("bootstrapSync: ignoring seed peer '" + peer + "' — invalid port");
                continue;
            }
            if (isSelf(host, port)) {
                log.fine("bootstrapSync: skipping self in seed list: " + peer);
                continue;
            }
            donorBuilder.seed(host, port);
            hasSeed = true;
        }
        if (!hasSeed) {
            log.warning("bootstrapSync: no valid seed peers found — skipping sync");
            return;
        }

        bootstrapSyncLock.lock();
        try {
            ReplicationBuffer buffer = new ReplicationBuffer();
            // Compose with the existing callback (e.g. the lag-metric sink wired in doStart)
            // so bootstrap buffering does not permanently discard any pre-registered listener.
            Consumer<ReplicationEvent> previous = replicationEngine.incomingCallback();
            replicationEngine.onIncoming(event -> {
                buffer.offer(event);
                if (previous != null) previous.accept(event);
            });
            try {
                BootstrapSync.builder()
                        .blocks(syncBlocks)
                        .localNodeId(config.nodeId())
                        .donorSelector(donorBuilder.build())
                        .syncTimeoutMs(BOOTSTRAP_SYNC_TIMEOUT_MS)
                        .replicationBuffer(buffer)
                        .build()
                        .syncFromPeer();
            } finally {
                replicationEngine.onIncoming(previous); // restore, not null
            }
        } finally {
            bootstrapSyncLock.unlock();
        }
    }

    private boolean isSelf(String host, int port) {
        if (port != config.bindPort()) return false;
        try {
            InetAddress addr = InetAddress.getByName(host);
            InetAddress bind = InetAddress.getByName(config.bindAddress());
            if (!bind.isAnyLocalAddress()) return bind.equals(addr);
            Enumeration<NetworkInterface> nics = NetworkInterface.getNetworkInterfaces();
            if (nics == null) return false;
            while (nics.hasMoreElements()) {
                Enumeration<InetAddress> ifAddrs = nics.nextElement().getInetAddresses();
                while (ifAddrs.hasMoreElements()) {
                    if (ifAddrs.nextElement().equals(addr)) return true;
                }
            }
        } catch (Exception ignored) {}
        return false;
    }

    /**
     * Gracefully shuts down all modules in reverse startup order.
     * Flushes pending replication frames before closing TCP connections.
     * Idempotent — safe to call multiple times.
     */
    public void shutdown() {
        if (!running.compareAndSet(true, false)) return;

        // 1. Flush pending replication frames before closing connections.
        flushReplication();

        // 2. Broadcast LEAVE and stop gossip (graceful departure).
        GossipNode gn = gossipNode;
        if (gn != null) gn.shutdown();

        // 3. Stop replication engine (closes per-peer writer threads).
        ReplicationEngine engine = replicationEngine;
        if (engine != null) engine.shutdown();

        // 4. Close TCP server (stop accepting connections).
        TcpServer tcp = tcpServer;
        if (tcp != null) tcp.shutdown();

        // 5. Close all blocks (stop expiry sweep threads).
        for (CacheBlock<?, ?> block : blocks.values()) {
            try { block.close(); }
            catch (Exception e) { log.log(Level.WARNING, "Error closing block", e); }
        }
        blocks.clear();

        // 6. Drain all watch dispatch queues.
        for (WatchRegistry<?, ?> registry : watchRegistries.values()) {
            try { registry.shutdown(); }
            catch (Exception e) { log.log(Level.WARNING, "Error shutting down WatchRegistry", e); }
        }
        watchRegistries.clear();

        // 7. Deregister all JMX MBeans.
        metricsRegistry.shutdown();

        log.info("FlairCache shutdown complete: nodeId=" + config.nodeId());
    }

    /** Delegates to {@link #shutdown()} to support try-with-resources. */
    @Override
    public void close() {
        shutdown();
    }

    // ── Startup internals ─────────────────────────────────────────────────────

    private void doStart(List<Runnable> cleanup) throws IOException {
        // Step 1: HLC — no network I/O; no cleanup needed on failure.
        hlc = new HybridLogicalClock();

        // Step 2: BootstrapServer serves SYNC_REQ frames for joining nodes.
        // It holds a live reference to the blocks map — populated as blocks are built.
        BootstrapServer bootstrapServer = new BootstrapServer(blocks, BOOTSTRAP_CHUNK_BYTES);

        // Step 3: Pre-build the ReplicationEngine.Builder to get its FrameHandler
        // before the TcpServer is constructed (they need each other).
        ReplicationEngine.Builder engineBuilder = ReplicationEngine.builder()
                .localNodeId(config.nodeId())
                .batchWindowMs(config.batchWindowMs())
                .batchMaxFrames(config.batchMaxFrames())
                .ackTimeoutMs(config.ackTimeoutMs())
                .keepaliveIntervalMs(config.keepaliveIntervalMs())
                .keepalivePongTimeoutMs(config.keepalivePongTimeoutMs())
                .tls(config.tls())
                .blockLookup(blocks::get);

        FrameHandler replHandler = engineBuilder.frameHandler();
        FrameHandler composite   = (conn, frame) -> {
            replHandler.onFrame(conn, frame);
            bootstrapServer.onFrame(conn, frame);
        };

        // Step 4: TcpServer — binds port; throws IOException on failure.
        TcpServer server = TcpServer.builder()
                .bindAddress(config.bindAddress())
                .port(config.bindPort())
                .handler(composite)
                .tls(config.tls())
                .selectorThreads(config.selectorThreads())
                .build();
        tcpServer = server;
        cleanup.add(server::shutdown);
        server.start();

        // Step 5: GossipNode — bind UDP socket before starting receive thread.
        GossipNode gossip;
        try {
            gossip = GossipNode.builder()
                    .nodeId(config.nodeId())
                    .bindAddress(config.bindAddress())
                    .bindPort(config.bindPort())
                    .seedPeers(config.seedPeers())
                    .build();
        } catch (IOException e) {
            // build() can only throw from InetAddress.getByName() — address resolution, not port bind.
            throw new IOException("GossipNode address resolution failed for '" + config.bindAddress() + "'", e);
        }
        gossipNode = gossip;

        // Step 6: Wire cluster metrics BEFORE gossip starts to capture all events.
        // Register metrics rollback NOW so that if gossip.start() (step 7) or any later step
        // throws, the JMX cluster MBean is deregistered and doesn't expose stale metrics.
        metricsRegistry.withCluster(gossip);
        cleanup.add(metricsRegistry::shutdown);

        // Step 7: Start gossip — send JOIN to seed peers.
        // cleanup.add must precede start() so the UDP socket is closed on rollback
        // even if start() throws between socket bind and successful return.
        cleanup.add(gossip::shutdown);
        try {
            gossip.start();
        } catch (IOException e) {
            throw new IOException("GossipNode UDP bind failed on port " + config.bindPort(), e);
        }

        // Step 8: Build and start ReplicationEngine.
        ReplicationEngine engine = engineBuilder
                .transport(server)
                .cluster(gossip)
                .build();
        replicationEngine = engine;
        cleanup.add(engine::shutdown);
        engine.start();

        // Step 9: Wire replication metrics. Route discarded-frame counts (from dead-peer queue
        // draining) into the DroppedFrameCount metric — replication cannot depend on the metrics
        // module, so the facade bridges the two here.
        // replMetrics is stored as an instance field so BlockBuilder.build() PutListeners can
        // record per-write replication lag without requiring a separate onIncoming() callback slot.
        replMetrics = metricsRegistry.withReplication(engine);
        engine.onFramesDropped(n -> {
            for (long i = 0; i < n; i++) {
                replMetrics.recordDroppedFrame();
            }
        });
    }

    private void flushReplication() {
        ReplicationEngine engine = replicationEngine;
        if (engine == null) return;
        long deadline = System.currentTimeMillis() + SHUTDOWN_FLUSH_TIMEOUT_MS;
        // Phase 1: wait for the send queue to drain (frames not yet sent to the wire).
        while (engine.pendingFrameCount() > 0 && System.currentTimeMillis() < deadline) {
            LockSupport.parkNanos(2_000_000L); // 2ms
        }
        // Phase 2: wait for in-flight ACKs to complete (frames sent, QUORUM/STRONG not yet confirmed).
        // Gossip LEAVE must not be broadcast until ACKs resolve; remote peers must not discard
        // connections before they can ACK frames already in their receive buffers.
        while (engine.pendingAckCount() > 0 && System.currentTimeMillis() < deadline) {
            LockSupport.parkNanos(2_000_000L);
        }
        long unsentFrames = engine.pendingFrameCount();
        long pendingAcks  = engine.pendingAckCount();
        if (unsentFrames > 0 || pendingAcks > 0) {
            log.warning("Shutdown: " + unsentFrames + " frames queued, " + pendingAcks
                    + " ACKs pending after " + SHUTDOWN_FLUSH_TIMEOUT_MS + "ms — proceeding anyway");
        }
    }

    private void checkRunning() {
        if (!running.get()) {
            throw new IllegalStateException(
                    "FlairCache is not running — call start() first, or it has already been shut down");
        }
    }

    // ── Factory ───────────────────────────────────────────────────────────────

    public static FlairCacheBuilder builder() {
        return new FlairCacheBuilder();
    }

    // ── BlockBuilder ──────────────────────────────────────────────────────────

    /**
     * Fluent builder for registering a typed cache block. Obtained via
     * {@link FlairCache#registerBlock(String)}. Wires the block into replication, metrics,
     * and watch on {@link #build()}.
     */
    public static final class BlockBuilder<K, V> {

        private final String     name;
        private final FlairCache cache;

        private Codec<K>       keyCodec;
        private Codec<V>       valueCodec;
        private Duration       ttl             = Duration.ZERO;
        private EvictionPolicy eviction        = EvictionPolicy.NONE;
        private int            maxEntries      = 0;
        private ConsistencyMode consistency;   // null → inherits FlairCache default

        private BlockBuilder(String name, FlairCache cache) {
            this.name  = name;
            this.cache = cache;
        }

        public BlockBuilder<K, V> keyCodec(Codec<K> codec) {
            this.keyCodec = Objects.requireNonNull(codec, "keyCodec must not be null");
            return this;
        }

        public BlockBuilder<K, V> valueCodec(Codec<V> codec) {
            this.valueCodec = Objects.requireNonNull(codec, "valueCodec must not be null");
            return this;
        }

        public BlockBuilder<K, V> ttl(Duration ttl) {
            Objects.requireNonNull(ttl, "ttl must not be null");
            if (ttl.isNegative()) throw new IllegalArgumentException("ttl must not be negative");
            this.ttl = ttl;
            return this;
        }

        public BlockBuilder<K, V> eviction(EvictionPolicy eviction) {
            this.eviction = Objects.requireNonNull(eviction, "eviction must not be null");
            return this;
        }

        /**
         * Maximum number of entries before eviction kicks in. {@code 0} (the default) means
         * unlimited — eviction is never triggered regardless of the {@link EvictionPolicy}.
         * Only meaningful when {@link #eviction(EvictionPolicy)} is set to a non-NONE policy
         * and {@code maxEntries > 0}.
         *
         * <p>Verified in {@link LocalStore}: {@code maxEntries <= 0} short-circuits the
         * eviction check, so {@code 0} is safe and does not mean "zero-capacity".</p>
         */
        public BlockBuilder<K, V> maxEntries(int maxEntries) {
            if (maxEntries < 0) throw new IllegalArgumentException("maxEntries must be >= 0");
            this.maxEntries = maxEntries;
            return this;
        }

        /** Overrides the FlairCache-level default consistency for writes from this block. */
        public BlockBuilder<K, V> consistency(ConsistencyMode consistency) {
            this.consistency = Objects.requireNonNull(consistency, "consistency must not be null");
            return this;
        }

        /**
         * Builds the block, registers it with the replication engine, metrics registry,
         * and watch registry. The block is immediately active.
         *
         * <p>{@code ttl(Duration.ZERO)} means no TTL (entries never expire by time).
         * {@code maxEntries(0)} means unlimited capacity.</p>
         *
         * @throws NullPointerException  if keyCodec or valueCodec is not set
         * @throws IllegalStateException if a block with this name is already registered
         */
        public CacheBlock<K, V> build() {
            Objects.requireNonNull(keyCodec,   "keyCodec is required");
            Objects.requireNonNull(valueCodec, "valueCodec is required");

            // Fast-fail before any allocation: check for a duplicate name using containsKey.
            // This is a best-effort pre-check for the common non-concurrent case.
            // putIfAbsent() below remains the definitive atomic guard for concurrent callers.
            if (cache.blocks.containsKey(name)) {
                throw new IllegalStateException(
                        "A block is already registered as: '" + name
                        + "' — each block name must be unique within a FlairCache instance");
            }

            ConsistencyMode mode = consistency != null
                    ? consistency
                    : cache.config.defaultConsistency();

            // Create the store-level block with the shared HLC and local node ID.
            // localNodeId is propagated so locally-written entries carry a stable UUID for
            // LWW tiebreaking — without it, concurrent writes from different nodes that share
            // the same HLC timestamp would compare against UUID(0,0) on the writing node but
            // the actual peer UUID on other nodes, causing inconsistent LWW resolution.
            CacheBlock<K, V> block = CacheBlock.<K, V>builder()
                    .name(name)
                    .keyCodec(keyCodec)
                    .valueCodec(valueCodec)
                    .ttl(ttl)
                    .eviction(eviction)
                    .maxEntries(maxEntries)
                    .hlc(cache.hlc)
                    .localNodeId(cache.config.nodeId())
                    .build();

            // Wire watch listeners BEFORE making the block visible in the blocks map.
            // This eliminates the window where IncomingHandler could apply a replication
            // frame and fire notifyPut() before the watch PutListener is registered.
            WatchRegistry<K, V> watchRegistry = new WatchRegistry<>();
            Codec<K> kc = keyCodec;
            Codec<V> vc = valueCodec;

            UUID localNodeId = cache.config.nodeId();

            block.addPutListener((rawKey, entry) -> {
                // Source determination: INCOMING flag takes priority over originNodeId.
                //
                // Source.LOCAL  — the calling thread invoked block.put() directly on this node,
                //                 in this process. originNodeId equals localNodeId for these writes.
                // Source.REPLICATED — the write arrived via the network: a peer's replication frame
                //                 (IncomingHandler sets INCOMING=true) or a bootstrap snapshot
                //                 (BootstrapSync.applyChunk sets INCOMING=true).
                //
                // NOTE — self-authored entries received during bootstrap replay fire as REPLICATED,
                // not LOCAL, even when originNodeId == localNodeId (i.e. this node wrote the entry
                // before a restart and now receives it back from a donor peer). This is intentional:
                //   1. Consistency — DeleteListener uses the same INCOMING signal. Using originNodeId
                //      for PUT but INCOMING for DELETE would give different semantics to the same
                //      concept in the same listener registration block.
                //   2. Delivery mechanism, not authorship — Source describes how the current *process*
                //      learned about the data, not who originally authored it. After a restart this
                //      process has no memory of the previous write; the data arrived from the network.
                //   3. Safety for external fan-out — a subscriber that writes to an external system
                //      on Source.LOCAL would silently skip bootstrap-replayed entries, potentially
                //      leaving the external system out of sync after a node restart.
                // Watch subscribers that need to distinguish "this node is the original author"
                // from "this data arrived from the network" should inspect
                // event.originNodeId().equals(localNodeId) directly.
                boolean isIncoming = ReplicationEngine.isIncomingReplication();
                UUID origin = entry.originNodeId();
                ChangeEvent.Source src = (!isIncoming && (origin == null || localNodeId.equals(origin)))
                        ? ChangeEvent.Source.LOCAL
                        : ChangeEvent.Source.REPLICATED;

                // Record replication lag before codec work so the metric captures the full
                // processing overhead, not just the dispatch time.
                if (src == ChangeEvent.Source.REPLICATED) {
                    ReplicationMetricsMBean rm = cache.replMetrics;
                    if (rm != null) {
                        rm.recordReplicationLag(System.currentTimeMillis() - entry.hlc().logical());
                    }
                }

                // Guard: skip codec work entirely when no subscribers exist.
                if (entry.value() == null || !watchRegistry.hasSubscribers()) return;
                try {
                    K key = kc.deserialize(ByteBuffer.wrap(rawKey));
                    V val = vc.deserialize(ByteBuffer.wrap(entry.value()));
                    // Pass the HLC logical timestamp as the source timestamp for lag-aware
                    // watch dispatch. -1L signals "no lag" to WatchRegistry for local writes.
                    long srcTs = src == ChangeEvent.Source.REPLICATED
                            ? entry.hlc().logical()
                            : -1L;
                    watchRegistry.dispatch(new ChangeEvent.PutEvent<>(key, val, null, src), srcTs);
                } catch (Exception e) {
                    if (log.isLoggable(Level.FINEST)) {
                        log.finest("Watch dispatch error for PUT block=" + name + ": " + e);
                    }
                }
            });

            block.addDeleteListener((rawKey, entry) -> {
                if (!watchRegistry.hasSubscribers()) return;
                try {
                    K key = kc.deserialize(ByteBuffer.wrap(rawKey));
                    // For DELETE events the entry carries the origin of the original PUT, not
                    // the deleter. Use the INCOMING ThreadLocal (set by IncomingHandler) to
                    // determine whether this delete arrived via replication.
                    ChangeEvent.Source src = ReplicationEngine.isIncomingReplication()
                            ? ChangeEvent.Source.REPLICATED
                            : ChangeEvent.Source.LOCAL;
                    watchRegistry.dispatch(new ChangeEvent.DeleteEvent<>(key, null, src));
                } catch (Exception e) {
                    if (log.isLoggable(Level.FINEST)) {
                        log.finest("Watch dispatch error for DELETE block=" + name + ": " + e);
                    }
                }
            });

            block.addExpireListener((rawKey, entry) -> {
                if (!watchRegistry.hasSubscribers()) return;
                try {
                    K key = kc.deserialize(ByteBuffer.wrap(rawKey));
                    watchRegistry.dispatch(new ChangeEvent.ExpireEvent<>(key, null));
                } catch (Exception e) {
                    if (log.isLoggable(Level.FINEST)) {
                        log.finest("Watch dispatch error for EXPIRE block=" + name + ": " + e);
                    }
                }
            });

            // Atomically register the block. If a concurrent build() for the same name
            // already won the race, discard this block and reject the caller.
            if (cache.blocks.putIfAbsent(name, block) != null) {
                block.close(); // stop expiry sweep thread started by CacheBlock.build()
                throw new IllegalStateException(
                        "A block is already registered as: '" + name
                        + "' — each block name must be unique within a FlairCache instance");
            }

            // Wire outgoing replication: local put/delete → fanout to peers.
            // V1 KNOWN LIMITATION (CLAUDE.md V1 hardening): attachBlock installs a PutListener
            // that catches ReplicationTimeoutException internally, so QUORUM/STRONG failures
            // are logged but never propagated to callers of CacheBlock.put(). The correct fix
            // requires a facade-level wrapper that intercepts put/delete and calls
            // engine.replicate() directly — but CacheBlock is final, so the wrapper would
            // require a new public type, changing the API surface beyond V1 scope. Track as
            // a V2 item; QUORUM/STRONG blocks currently behave as EVENTUAL under network failure.
            cache.replicationEngine.attachBlock(name, block, mode);

            // Register per-block JMX metrics.
            cache.metricsRegistry.registerBlock(name, block);

            cache.watchRegistries.put(name, watchRegistry);
            return block;
        }
    }
}
