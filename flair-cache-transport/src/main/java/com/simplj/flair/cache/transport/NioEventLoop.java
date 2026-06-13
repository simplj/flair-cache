package com.simplj.flair.cache.transport;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLEngineResult.HandshakeStatus;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class NioEventLoop implements Runnable {

    private static final Logger log = Logger.getLogger(NioEventLoop.class.getName());
    private static final long SELECT_TIMEOUT_MS = 50;
    // Global counter so every NioEventLoop instance gets a unique thread name —
    // critical for distinguishing selector threads in thread dumps when selectorThreads > 1.
    private static final AtomicInteger INSTANCE_COUNTER = new AtomicInteger();
    // Empty source buffer for SSLEngine.wrap() during TLS handshake — handshake records are
    // produced from the engine's internal state, not from application data.
    // Instance field (not static) so that two NioEventLoop instances on different selector threads
    // never share a ByteBuffer and introduce a JMM data race on its position/limit fields.
    private final ByteBuffer HANDSHAKE_EMPTY_SRC = ByteBuffer.allocate(0);

    private final Selector selector;
    private final FrameHandler frameHandler;
    private final ExecutorService workerPool;
    private final int readBufferBytes;
    private final int maxPayloadBytes;
    private final BackpressurePolicy backpressurePolicy;
    private final int queueCapacity;
    private final TlsConfig serverTlsConfig;

    // Per-registration futures for client connects — safe for concurrent TcpClient.connect() calls
    private final Queue<PendingConnect> pendingConnects = new ConcurrentLinkedQueue<>();

    // After NEED_TASK completes, the connection is queued here so the selector thread can
    // drive the next handshake step (typically NEED_WRAP) without waiting for OP_READ to fire.
    private final Queue<ConnectionImpl> pendingHandshakeResumes = new ConcurrentLinkedQueue<>();

    // Connection registry: SocketChannel → ConnectionImpl
    private final Map<SocketChannel, ConnectionImpl> connections = new ConcurrentHashMap<>();

    // Server-side accept listener (TcpServer.onConnect). Never used for client connects.
    private volatile Consumer<Connection> acceptListener;

    // When non-null, accepted SocketChannels are handed to this consumer instead of being
    // registered with this loop's selector. Used by TcpServer to distribute incoming
    // connections across a pool of event loops (selectorThreads > 1).
    private volatile Consumer<SocketChannel> acceptHandoff;

    private volatile boolean running = true;
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final Thread thread;

    // Pool of direct ByteBuffers used for TLS handshake wrap operations.
    // Size = workerThreads × 2; re-used across handshake steps to avoid per-step allocation.
    private final ByteBufferPool tlsWrapPool;

    NioEventLoop(FrameHandler frameHandler,
                 int workerThreads,
                 int readBufferBytes,
                 int maxPayloadBytes,
                 BackpressurePolicy backpressurePolicy,
                 int queueCapacity,
                 TlsConfig serverTlsConfig) {
        this.frameHandler = frameHandler;
        this.readBufferBytes = readBufferBytes;
        this.maxPayloadBytes = maxPayloadBytes;
        this.backpressurePolicy = backpressurePolicy;
        this.queueCapacity = queueCapacity;
        this.serverTlsConfig = serverTlsConfig;
        this.thread = new FlairCacheThreadFactory(
                "flaircache-nio-selector-" + INSTANCE_COUNTER.getAndIncrement()).newThread(this);

        int poolSize = Math.max(1, workerThreads);
        this.workerPool = Executors.newFixedThreadPool(
                poolSize, new FlairCacheThreadFactory("flaircache-transport-worker", true));
        // TLS wrap pool: 16 KB per buffer covers typical handshake record sizes
        this.tlsWrapPool = new ByteBufferPool(poolSize * 2, 16 * 1024);

        try {
            this.selector = Selector.open();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to open selector", e);
        }
    }

    void start() {
        if (started.compareAndSet(false, true)) {
            thread.start();
        }
    }

    void shutdown() {
        running = false;
        selector.wakeup();
        workerPool.shutdown();
        try {
            if (!workerPool.awaitTermination(5, TimeUnit.SECONDS)) {
                workerPool.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            workerPool.shutdownNow();
        }
        try {
            selector.close();
        } catch (IOException e) {
            log.log(Level.WARNING, "Error closing selector", e);
        }
    }

    void registerServerChannel(ServerSocketChannel ssc) throws IOException {
        ssc.configureBlocking(false);
        ssc.register(selector, SelectionKey.OP_ACCEPT);
    }

    /**
     * Registers a client channel for read/write via this event loop.
     * For plaintext connections the supplied future is completed as soon as the channel is
     * registered. For TLS connections the future is completed only after the handshake finishes
     * so callers cannot send application data before the record layer is ready. Each call gets
     * its own future — concurrent TcpClient.connect() calls on the same event loop are safe.
     */
    void registerClientChannel(SocketChannel sc, TlsConfig tlsConfig, CompletableFuture<Connection> future) {
        pendingConnects.offer(new PendingConnect(sc, tlsConfig, false, future));
        try {
            selector.wakeup();
        } catch (ClosedSelectorException e) {
            // Selector already closed — reject immediately rather than leaving the caller hung.
            try { sc.close(); } catch (IOException ignored) {}
            if (future != null) {
                future.completeExceptionally(new IOException("Event loop is shut down"));
            }
        }
    }

    /** Server-side notification for every newly accepted Connection. */
    void setAcceptListener(Consumer<Connection> listener) {
        this.acceptListener = listener;
    }

    /**
     * When set, accepted raw channels are forwarded to this consumer instead of being
     * registered with this loop. TcpServer uses this to distribute accepted connections
     * across a pool of worker loops (see {@code selectorThreads}).
     */
    void setAcceptHandoff(Consumer<SocketChannel> handoff) {
        this.acceptHandoff = handoff;
    }

    /**
     * Queues a server-accepted channel for registration with this event loop. Called by a
     * sibling accept loop when distributing incoming connections across the pool.
     * Uses this loop's {@code serverTlsConfig} since all loops in a pool share the same config.
     */
    void registerAcceptedChannel(SocketChannel sc) {
        pendingConnects.offer(new PendingConnect(sc, serverTlsConfig, true, null));
        try {
            selector.wakeup();
        } catch (ClosedSelectorException e) {
            try { sc.close(); } catch (IOException ignored) {}
        }
    }

    void disconnectAll() {
        for (ConnectionImpl conn : connections.values()) {
            conn.close();
        }
        connections.clear();
    }

    @Override
    public void run() {
        while (running) {
            try {
                selector.select(SELECT_TIMEOUT_MS);
                drainPendingConnects();
                drainPendingHandshakeResumes();
                processSelectedKeys();
            } catch (ClosedSelectorException e) {
                break;
            } catch (IOException e) {
                log.log(Level.SEVERE, "Selector error", e);
                break;
            }
        }
        disconnectAll();
    }

    private void drainPendingConnects() {
        PendingConnect pc;
        while ((pc = pendingConnects.poll()) != null) {
            // conn declared outside try so the catch block can shut down its WriteQueue if
            // setup fails partway through (e.g. register() throws after newConnection() starts the thread).
            ConnectionImpl conn = null;
            try {
                pc.channel.configureBlocking(false);
                conn = newConnection(pc.channel, pc.tlsConfig, pc.serverSide);
                connections.put(pc.channel, conn);
                SelectionKey key = pc.channel.register(selector, SelectionKey.OP_READ, conn);
                if (conn.engine != null) {
                    conn.engine.beginHandshake();
                    if (!pc.serverSide) {
                        // Client must send the first handshake message (ClientHello).
                        // Drive the initial NEED_WRAP step now on the selector thread.
                        // Store the future on the connection so notifyTlsReady() can complete it
                        // after the handshake finishes rather than completing it here prematurely.
                        conn.pendingFuture = pc.future;
                        try {
                            doHandshakeStep(conn, key);
                        } catch (IOException e) {
                            log.log(Level.WARNING, "TLS client handshake initiation failed", e);
                            if (pc.future != null) {
                                pc.future.completeExceptionally(e);
                            }
                            conn.pendingFuture = null;
                            closeKey(key);
                        }
                    }
                    // For server-accepted TLS connections the future is always null; the accept
                    // listener is notified after the handshake completes in checkTlsHandshakeComplete().
                } else {
                    // Plaintext: connection is ready immediately
                    if (pc.future != null) {
                        pc.future.complete(conn);       // outgoing client: unblock TcpClient.connect()
                    } else if (pc.serverSide) {
                        notifyAcceptListener(conn);     // server-accepted, handed off from accept loop
                    }
                }
            } catch (IOException e) {
                log.log(Level.WARNING, "Failed to register client channel", e);
                if (pc.future != null) {
                    pc.future.completeExceptionally(e);
                }
                if (conn != null) {
                    connections.remove(pc.channel);
                    conn.writeQueue.shutdown();
                }
                try { pc.channel.close(); } catch (IOException ignored) {}
            }
        }
    }

    /** Drive handshake steps for connections that had delegated tasks (NEED_TASK) complete. */
    private void drainPendingHandshakeResumes() {
        ConnectionImpl conn;
        while ((conn = pendingHandshakeResumes.poll()) != null) {
            if (!conn.alive) {
                continue;
            }
            SelectionKey key = conn.channel.keyFor(selector);
            if (key == null || !key.isValid()) {
                continue;
            }
            try {
                doHandshakeStep(conn, key);
                checkTlsHandshakeComplete(conn);
            } catch (IOException e) {
                log.log(Level.WARNING, "TLS post-task handshake failed", e);
                closeKey(key);
            }
        }
    }

    private void processSelectedKeys() throws IOException {
        Iterator<SelectionKey> it = selector.selectedKeys().iterator();
        while (it.hasNext()) {
            SelectionKey key = it.next();
            it.remove();
            if (!key.isValid()) {
                continue;
            }
            try {
                if (key.isAcceptable()) {
                    handleAccept((ServerSocketChannel) key.channel());
                } else if (key.isReadable()) {
                    handleRead(key);
                }
            } catch (IOException e) {
                log.log(Level.WARNING, "I/O error on key", e);
                closeKey(key);
            }
        }
    }

    private void handleAccept(ServerSocketChannel ssc) {
        SocketChannel sc;
        try {
            sc = ssc.accept();
        } catch (IOException e) {
            log.log(Level.WARNING, "Failed to accept incoming connection", e);
            return;
        }
        if (sc == null) {
            return;
        }

        // When a pool of event loops is active, hand the raw channel off to a worker loop
        // instead of registering it with this accept loop. The worker loop's
        // drainPendingConnects() handles configureBlocking + registration.
        Consumer<SocketChannel> handoff = acceptHandoff;
        if (handoff != null) {
            handoff.accept(sc);
            return;
        }

        // Single-loop path: register the accepted connection with this loop directly.
        // Isolate per-connection setup so that any IOException (e.g. client disconnected
        // immediately, SSLEngine failure) does NOT propagate to processSelectedKeys().
        // If it did, the catch there would call closeKey() on the ServerSocketChannel's
        // OP_ACCEPT key, permanently disabling all future accepts on this server.
        // conn declared outside try so the catch block can shut down its WriteQueue if
        // setup fails partway through (e.g. sc.register() throws after newConnection() starts the thread).
        ConnectionImpl conn = null;
        try {
            sc.configureBlocking(false);
            conn = newConnection(sc, serverTlsConfig, true);
            connections.put(sc, conn);
            sc.register(selector, SelectionKey.OP_READ, conn);
            if (conn.engine != null) {
                conn.engine.beginHandshake();
                // For TLS, the accept listener is notified after the handshake completes
                // in checkTlsHandshakeComplete so the listener cannot send plaintext data.
            } else {
                notifyAcceptListener(conn);
            }
            if (log.isLoggable(Level.FINE)) {
                log.fine("Accepted connection from " + sc.getRemoteAddress());
            }
        } catch (IOException e) {
            log.log(Level.WARNING, "Failed to set up accepted connection; closing it", e);
            if (conn != null) {
                connections.remove(sc);
                conn.writeQueue.shutdown();
            }
            try { sc.close(); } catch (IOException ignored) {}
        }
    }

    private void handleRead(SelectionKey key) throws IOException {
        ConnectionImpl conn = (ConnectionImpl) key.attachment();
        if (!conn.alive) {
            closeKey(key);
            return;
        }

        if (conn.engine != null) {
            handleTlsRead(conn, key);
        } else {
            handlePlainRead(conn, key);
        }

        if (conn.writeQueue.hasWriteFailed()) {
            closeConnection(conn, key);
        }
    }

    private void handlePlainRead(ConnectionImpl conn, SelectionKey key) throws IOException {
        int bytesRead = conn.channel.read(conn.readBuf);
        if (bytesRead == -1) {
            closeConnection(conn, key);
            return;
        }
        conn.readBuf.flip();
        if (!conn.assembler.feed(conn.readBuf, frame -> dispatchFrame(conn, frame))) {
            log.warning("Protocol error from " + conn.id + " — closing connection");
            closeConnection(conn, key);
            return;
        }
        conn.readBuf.compact();
    }

    private void handleTlsRead(ConnectionImpl conn, SelectionKey key) throws IOException {
        HandshakeStatus hs = conn.engine.getHandshakeStatus();
        if (hs != HandshakeStatus.NOT_HANDSHAKING && hs != HandshakeStatus.FINISHED) {
            if (!doHandshakeStep(conn, key)) {
                checkTlsHandshakeComplete(conn);
                return;
            }
            // Do NOT call checkTlsHandshakeComplete here — the post-data step below may
            // need to produce more handshake records (e.g. TLS 1.2 renegotiation NEED_WRAP).
            // Setting applicationDataReady=true here would let the writer call engine.wrap()
            // concurrently with the selector's post-data doHandshakeStep — violating the
            // SSLEngine contract. checkTlsHandshakeComplete is called unconditionally at the
            // end of this method, after the post-data step has completed.
        }

        int bytesRead = conn.channel.read(conn.tlsNetBuf);
        if (bytesRead == -1) {
            closeConnection(conn, key);
            return;
        }
        if (bytesRead == 0) {
            // No application data yet, but the handshake may have just completed inside
            // doHandshakeStep above. Call checkTlsHandshakeComplete so the future is resolved
            // and applicationDataReady is set. This path is safe: bytesRead==0 means
            // unwrapLoop never runs, so no renegotiation record can produce a concurrent
            // NEED_WRAP that races with the writer thread.
            checkTlsHandshakeComplete(conn);
            return;
        }

        conn.tlsNetBuf.flip();
        unwrapLoop(conn, key);
        conn.tlsNetBuf.compact();

        if (!conn.alive) {
            return; // unwrapLoop closed the connection (CLOSED alert or protocol error)
        }

        // Handle any post-data handshake steps (e.g. TLS 1.3 session tickets)
        hs = conn.engine.getHandshakeStatus();
        if (hs != HandshakeStatus.NOT_HANDSHAKING && hs != HandshakeStatus.FINISHED) {
            doHandshakeStep(conn, key);
        }
        checkTlsHandshakeComplete(conn);
    }

    /**
     * Notify the appropriate party when a TLS handshake finishes:
     * - Client connections: complete the pending future so TcpClient.connect() unblocks.
     * - Server connections: fire the accept listener.
     * Also enables application data writes in the WriteQueue.
     */
    private void checkTlsHandshakeComplete(ConnectionImpl conn) {
        if (conn.tlsHandshakeDone) {
            return;
        }
        HandshakeStatus hs = conn.engine.getHandshakeStatus();
        if (hs != HandshakeStatus.NOT_HANDSHAKING && hs != HandshakeStatus.FINISHED) {
            return;
        }
        conn.tlsHandshakeDone = true;
        conn.writeQueue.applicationDataReady = true; // allow writer thread to flush
        CompletableFuture<Connection> future = conn.pendingFuture;
        if (future != null) {
            conn.pendingFuture = null;
            future.complete(conn); // client: unblock TcpClient.connect()
        } else {
            notifyAcceptListener(conn); // server: fire onConnect listener
        }
    }

    /**
     * Unwrap all available TLS records from tlsNetBuf. Grows tlsAppBuf on BUFFER_OVERFLOW
     * rather than silently dropping the record.
     */
    private void unwrapLoop(ConnectionImpl conn, SelectionKey key) throws IOException {
        while (conn.tlsNetBuf.hasRemaining()) {
            // tlsAppBuf is in write mode here. position = bytes already written by prior unwrap calls
            // and preserved by compact() after the OK branch below. remaining() = capacity - position.
            // If there is not enough room for a full SSL record, grow the buffer.
            // compact() must NOT be called here — it would corrupt the preserved bytes.
            if (conn.tlsAppBuf.remaining() < conn.engine.getSession().getApplicationBufferSize()) {
                conn.growTlsAppBuf();
            }

            SSLEngineResult result = conn.engine.unwrap(conn.tlsNetBuf, conn.tlsAppBuf);

            switch (result.getStatus()) {
                case OK:
                    conn.tlsAppBuf.flip();
                    if (!conn.assembler.feed(conn.tlsAppBuf, frame -> dispatchFrame(conn, frame))) {
                        log.warning("TLS protocol error from " + conn.id + " — closing connection");
                        closeConnection(conn, key);
                        return;
                    }
                    conn.tlsAppBuf.compact();
                    break;
                case BUFFER_UNDERFLOW:
                    // Incomplete TLS record — wait for more channel data
                    return;
                case BUFFER_OVERFLOW:
                    // Grow appBuf and retry — should not happen after the check above
                    conn.growTlsAppBuf();
                    continue;
                case CLOSED:
                    closeConnection(conn, key);
                    return;
            }

            // Handle mid-stream handshake status changes (renegotiation, post-handshake auth)
            HandshakeStatus handshakeStatus = result.getHandshakeStatus();
            if (handshakeStatus == HandshakeStatus.NEED_WRAP || handshakeStatus == HandshakeStatus.NEED_TASK) {
                // tlsNetBuf is in read mode here (flipped before unwrapLoop entry). doHandshakeStep's
                // NEED_UNWRAP case calls channel.read(tlsNetBuf) which requires write mode, so compact first.
                conn.tlsNetBuf.compact();
                boolean done = doHandshakeStep(conn, key);
                if (!conn.alive) {
                    return; // doHandshakeStep closed the connection
                }
                conn.tlsNetBuf.flip(); // restore read mode for the next unwrap iteration
                if (!done) {
                    return; // NEED_TASK submitted or NEED_UNWRAP waiting for data — exit and re-enter on next read
                }
            }
        }
    }

    private boolean doHandshakeStep(ConnectionImpl conn, SelectionKey key) throws IOException {
        SSLEngine engine = conn.engine;
        HandshakeStatus status = engine.getHandshakeStatus();

        while (status != HandshakeStatus.NOT_HANDSHAKING && status != HandshakeStatus.FINISHED) {
            switch (status) {
                case NEED_WRAP: {
                    // Acquire from pool to avoid per-step allocation on the selector thread.
                    // Use fromPool flag so the finally block knows whether to return the buffer.
                    int packetSize = engine.getSession().getPacketBufferSize();
                    boolean fromPool = packetSize <= tlsWrapPool.bufferCapacity();
                    ByteBuffer net = fromPool ? tlsWrapPool.acquire() : ByteBuffer.allocateDirect(packetSize);
                    SSLEngineResult r;
                    try {
                        HANDSHAKE_EMPTY_SRC.clear();
                        r = engine.wrap(HANDSHAKE_EMPTY_SRC, net);
                        // BUFFER_OVERFLOW means net was too small; grow and retry.
                        while (r.getStatus() == SSLEngineResult.Status.BUFFER_OVERFLOW) {
                            if (fromPool) { tlsWrapPool.release(net); fromPool = false; }
                            net = ByteBuffer.allocateDirect(net.capacity() * 2);
                            HANDSHAKE_EMPTY_SRC.clear();
                            r = engine.wrap(HANDSHAKE_EMPTY_SRC, net);
                        }
                        net.flip();
                        // Non-blocking write: back off on a full send buffer, but cap the stall.
                        // Blocking the selector thread past 5 ms would stall all other connections.
                        long stallNanos = 0;
                        while (net.hasRemaining()) {
                            int written = conn.channel.write(net);
                            if (written == 0) {
                                LockSupport.parkNanos(100_000L);
                                stallNanos += 100_000L;
                                if (stallNanos >= 5_000_000L) {
                                    throw new IOException(
                                            "TLS handshake NEED_WRAP write stalled >5 ms; closing connection");
                                }
                            } else {
                                stallNanos = 0;
                            }
                        }
                        // CLOSED means the peer sent a fatal alert; the engine encoded the
                        // response alert into net (already flushed). Close the connection.
                        if (r.getStatus() == SSLEngineResult.Status.CLOSED) {
                            closeConnection(conn, key);
                            return false;
                        }
                        status = r.getHandshakeStatus();
                    } finally {
                        if (fromPool) { tlsWrapPool.release(net); }
                    }
                    break;
                }
                case NEED_UNWRAP: {
                    int read = conn.channel.read(conn.tlsNetBuf);
                    if (read == -1) {
                        closeConnection(conn, key);
                        return false;
                    }
                    // If read==0 but tlsNetBuf already has buffered bytes (position > 0),
                    // process those rather than returning early. After NEED_TASK the engine
                    // may have left handshake bytes in tlsNetBuf that the channel.read() did
                    // not see (OS buffer was empty). Returning early would stall the handshake
                    // permanently because OP_READ will never fire if the peer is waiting for us.
                    if (read == 0 && conn.tlsNetBuf.position() == 0) {
                        return false;
                    }
                    conn.tlsNetBuf.flip();
                    // Handshake output goes into a local scratch buffer, not tlsAppBuf.
                    // tlsAppBuf may already hold partial application-frame bytes that arrived
                    // before a mid-stream renegotiation — clearing it would permanently lose them.
                    // Handshake-layer output is not meaningful to the application and is discarded.
                    ByteBuffer scratch = ByteBuffer.allocate(engine.getSession().getApplicationBufferSize());
                    SSLEngineResult r;
                    do {
                        r = engine.unwrap(conn.tlsNetBuf, scratch);
                        if (r.getStatus() == SSLEngineResult.Status.BUFFER_OVERFLOW) {
                            scratch = ByteBuffer.allocate(scratch.capacity() * 2);
                        }
                    } while (r.getStatus() == SSLEngineResult.Status.BUFFER_OVERFLOW);
                    conn.tlsNetBuf.compact();
                    if (r.getStatus() == SSLEngineResult.Status.CLOSED) {
                        closeConnection(conn, key);
                        return false;
                    }
                    if (r.getStatus() == SSLEngineResult.Status.BUFFER_UNDERFLOW) {
                        // Incomplete TLS handshake record — the record was split across TCP segments.
                        // The partial bytes are preserved in tlsNetBuf by compact(). Return now and
                        // wait for OP_READ to fire with the remaining bytes. Looping again would spin
                        // the selector thread at 100% CPU until the second segment arrives.
                        return false;
                    }
                    status = r.getHandshakeStatus();
                    break;
                }
                case NEED_TASK: {
                    // Delegated tasks (certificate validation, CRL checks) may block for seconds.
                    // Run them on the worker pool; when done enqueue the connection for handshake
                    // resumption on the selector thread so the next step (often NEED_WRAP) runs
                    // without waiting for an OP_READ event that may never come.
                    Runnable task;
                    CompletableFuture<Void> all = CompletableFuture.completedFuture(null);
                    while ((task = engine.getDelegatedTask()) != null) {
                        final Runnable t = task;
                        all = all.thenRunAsync(t, workerPool);
                    }
                    final ConnectionImpl connRef = conn;
                    all.whenComplete((v, ex) -> {
                        if (ex != null) {
                            // A delegated task threw an unchecked exception (e.g. a custom
                            // TrustManager throwing RuntimeException). Fail the pending future
                            // immediately so TcpClient.connect() is not left until timeout.
                            // Offer the connection to pendingHandshakeResumes anyway so the selector
                            // thread discovers the broken engine state and closes the connection.
                            log.log(Level.WARNING,
                                    "TLS delegated task threw unchecked exception; closing connection "
                                            + connRef.id, ex);
                            CompletableFuture<Connection> f = connRef.pendingFuture;
                            if (f != null) {
                                connRef.pendingFuture = null;
                                f.completeExceptionally(new IOException("TLS handshake task failed", ex));
                            }
                        }
                        if (running) {
                            pendingHandshakeResumes.offer(connRef);
                            try {
                                selector.wakeup();
                            } catch (ClosedSelectorException ignored) {
                                // Shutdown raced with wakeup — fall through to else logic below.
                            }
                        } else {
                            // Shutdown raced with this delegated task. Complete any pending connect
                            // future so TcpClient.connect() is not left hanging until timeout.
                            CompletableFuture<Connection> f = connRef.pendingFuture;
                            if (f != null) {
                                connRef.pendingFuture = null;
                                f.completeExceptionally(
                                        new IOException("Event loop shut down during TLS handshake"));
                            }
                        }
                    });
                    // Return to selector loop; drainPendingHandshakeResumes() will continue
                    return false;
                }
                default:
                    return true;
            }
        }
        return true;
    }

    private void closeConnection(ConnectionImpl conn, SelectionKey key) {
        conn.alive = false;
        connections.remove(conn.channel);
        key.cancel();
        conn.writeQueue.shutdown();
        try {
            conn.channel.close();
        } catch (IOException e) {
            log.log(Level.FINEST, "Error closing channel", e);
        }
        if (log.isLoggable(Level.FINE)) {
            log.fine("Connection closed: " + conn.id);
        }
    }

    private void closeKey(SelectionKey key) {
        Object att = key.attachment();
        key.cancel();
        if (att instanceof ConnectionImpl) {
            ConnectionImpl conn = (ConnectionImpl) att;
            conn.alive = false;
            connections.remove(conn.channel);
            conn.writeQueue.shutdown();
            try { conn.channel.close(); } catch (IOException ignored) {}
        }
    }

    private void dispatchFrame(ConnectionImpl conn, RawFrame frame) {
        workerPool.submit(() -> {
            try {
                frameHandler.onFrame(conn, frame);
            } catch (Exception e) {
                log.log(Level.WARNING, "FrameHandler threw exception", e);
            }
        });
    }

    private ConnectionImpl newConnection(SocketChannel channel, TlsConfig tlsConfig, boolean serverSide)
            throws IOException {
        SSLEngine engine = null;
        if (tlsConfig != null && tlsConfig.isEnabled()) {
            engine = tlsConfig.sslContext().createSSLEngine();
            engine.setUseClientMode(!serverSide);
            if (serverSide && tlsConfig.requireClientAuth()) {
                engine.setNeedClientAuth(true);
            }
        }
        return new ConnectionImpl(channel, readBufferBytes, maxPayloadBytes,
                backpressurePolicy, queueCapacity, engine);
    }

    private void notifyAcceptListener(Connection conn) {
        Consumer<Connection> listener = acceptListener;
        if (listener != null) {
            try {
                listener.accept(conn);
            } catch (Exception e) {
                log.log(Level.WARNING, "acceptListener threw exception", e);
            }
        }
    }

    // ── Inner types ───────────────────────────────────────────────────────────

    private static final class PendingConnect {
        final SocketChannel channel;
        final TlsConfig tlsConfig;
        final boolean serverSide;
        final CompletableFuture<Connection> future; // null for server-accepted channels

        PendingConnect(SocketChannel channel, TlsConfig tlsConfig, boolean serverSide,
                       CompletableFuture<Connection> future) {
            this.channel = channel;
            this.tlsConfig = tlsConfig;
            this.serverSide = serverSide;
            this.future = future;
        }
    }

    boolean isRunning() {
        return started.get() && running;
    }

    Map<SocketChannel, ConnectionImpl> connections() {
        return connections;
    }
}
