package com.simplj.flair.cache.transport;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class TcpClient {

    private static final Logger log = Logger.getLogger(TcpClient.class.getName());

    private final String remoteAddress;
    private final int remotePort;
    private final int connectTimeoutMs;
    private final TlsConfig tls;
    private final NioEventLoop eventLoop;
    private final boolean ownedEventLoop;

    private TcpClient(Builder b, NioEventLoop loop, boolean ownedEventLoop) {
        this.remoteAddress = b.remoteAddress;
        this.remotePort = b.remotePort;
        this.connectTimeoutMs = b.connectTimeoutMs;
        this.tls = b.tls;
        this.eventLoop = loop;
        this.ownedEventLoop = ownedEventLoop;
    }

    /**
     * Opens a persistent TCP connection to the configured remote address.
     * Blocks until the connection is fully registered with the event loop or
     * {@code connectTimeoutMs} elapses.
     *
     * <p>Thread-safe: multiple calls on the same TcpClient (and concurrent calls across
     * TcpClient instances sharing the same NioEventLoop) are all safe because each call
     * uses its own CompletableFuture — there is no shared listener state to race on.
     */
    public Connection connect() throws IOException {
        // Open and connect before starting the event loop so that if either step fails
        // no selector thread is leaked. start() is idempotent — safe for concurrent callers.
        SocketChannel sc = SocketChannel.open();
        long startNanos = System.nanoTime();
        try {
            sc.socket().connect(new InetSocketAddress(remoteAddress, remotePort), connectTimeoutMs);
        } catch (IOException e) {
            try { sc.close(); } catch (IOException ignored) {}
            throw e;
        }

        if (ownedEventLoop) {
            eventLoop.start();
        }

        if (log.isLoggable(Level.FINE)) {
            log.fine("TCP connected to " + remoteAddress + ":" + remotePort + " — registering with event loop");
        }

        // Each connect() call gets its own future — concurrent calls on a shared NioEventLoop
        // do not interfere with each other.
        CompletableFuture<Connection> future = new CompletableFuture<>();
        eventLoop.registerClientChannel(sc, tls, future);

        // Subtract the time already spent on TCP connect so the total wait never exceeds connectTimeoutMs.
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
        long remainingMs = Math.max(1L, connectTimeoutMs - elapsedMs);
        try {
            return future.get(remainingMs, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            try { sc.close(); } catch (IOException ignored) {}
            // Shut down the owned event loop so the selector thread does not leak.
            // For shared event loops this must NOT be called — the loop serves other connections.
            if (ownedEventLoop) {
                eventLoop.shutdown();
            }
            throw new IOException("Timed out waiting for event-loop registration ("
                    + connectTimeoutMs + " ms)", e);
        } catch (ExecutionException e) {
            if (ownedEventLoop) {
                eventLoop.shutdown();
            }
            throw new IOException("Connection registration failed", e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            try { sc.close(); } catch (IOException ignored) {}
            if (ownedEventLoop) {
                eventLoop.shutdown();
            }
            throw new IOException("Interrupted during connection setup", e);
        }
    }

    /**
     * Shuts down the event loop only if it was created by this client.
     * Safe to call multiple times.
     */
    public void shutdown() {
        if (ownedEventLoop) {
            eventLoop.shutdown();
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private String remoteAddress;
        private int remotePort;
        private int connectTimeoutMs = 3000;
        private TlsConfig tls = TlsConfig.disabled();
        private FrameHandler handler;
        private NioEventLoop eventLoop;
        private int workerThreads = 2;
        private int readBufferBytes = 65536;
        private BackpressurePolicy backpressurePolicy = BackpressurePolicy.DROP_OLDEST;
        private int queueCapacity = WriteQueue.DEFAULT_CAPACITY;
        private int maxPayloadBytes = RawFrame.DEFAULT_MAX_PAYLOAD;

        public Builder remoteAddress(String remoteAddress) {
            this.remoteAddress = remoteAddress;
            return this;
        }

        public Builder remotePort(int remotePort) {
            this.remotePort = remotePort;
            return this;
        }

        public Builder connectTimeoutMs(int connectTimeoutMs) {
            this.connectTimeoutMs = connectTimeoutMs;
            return this;
        }

        public Builder tls(TlsConfig tls) {
            this.tls = tls;
            return this;
        }

        public Builder handler(FrameHandler handler) {
            this.handler = handler;
            return this;
        }

        /**
         * Share an existing event loop (e.g., from a TcpServer) so all TCP channels
         * — incoming and outgoing — are multiplexed through one selector thread.
         * If omitted, the client creates its own event loop started on first connect().
         */
        public Builder eventLoop(NioEventLoop eventLoop) {
            this.eventLoop = eventLoop;
            return this;
        }

        public Builder workerThreads(int workerThreads) {
            this.workerThreads = workerThreads;
            return this;
        }

        public Builder readBufferBytes(int readBufferBytes) {
            this.readBufferBytes = readBufferBytes;
            return this;
        }

        public Builder backpressurePolicy(BackpressurePolicy backpressurePolicy) {
            this.backpressurePolicy = backpressurePolicy;
            return this;
        }

        public Builder queueCapacity(int queueCapacity) {
            this.queueCapacity = queueCapacity;
            return this;
        }

        public Builder maxPayloadBytes(int maxPayloadBytes) {
            this.maxPayloadBytes = maxPayloadBytes;
            return this;
        }

        public TcpClient build() {
            if (remoteAddress == null || remoteAddress.isEmpty()) {
                throw new IllegalStateException("remoteAddress must be set");
            }
            if (remotePort <= 0 || remotePort > 65535) {
                throw new IllegalStateException("remotePort must be 1–65535");
            }
            if (handler == null && eventLoop == null) {
                throw new IllegalStateException("Either handler or eventLoop must be set");
            }
            if (connectTimeoutMs <= 0) {
                throw new IllegalStateException("connectTimeoutMs must be positive");
            }
            if (readBufferBytes <= 0) {
                throw new IllegalStateException("readBufferBytes must be positive");
            }
            if (queueCapacity <= 0) {
                throw new IllegalStateException("queueCapacity must be positive");
            }
            if (maxPayloadBytes < 0 || maxPayloadBytes > RawFrame.ABSOLUTE_MAX_PAYLOAD) {
                throw new IllegalStateException(
                        "maxPayloadBytes out of range [0, " + RawFrame.ABSOLUTE_MAX_PAYLOAD + "]");
            }

            boolean ownedLoop = (eventLoop == null);
            NioEventLoop loop;
            if (ownedLoop) {
                // Do NOT start the event loop here — start it lazily in connect() to avoid
                // leaking the selector thread if connect() is never called.
                loop = new NioEventLoop(handler, workerThreads, readBufferBytes,
                        maxPayloadBytes, backpressurePolicy, queueCapacity, TlsConfig.disabled());
            } else {
                loop = eventLoop;
            }
            return new TcpClient(this, loop, ownedLoop);
        }
    }
}
