package com.simplj.flair.cache.transport;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class TcpServer {

    private static final Logger log = Logger.getLogger(TcpServer.class.getName());

    private final ServerSocketChannel serverChannel;
    private final NioEventLoop[] eventLoops;
    private final AtomicInteger rrCounter = new AtomicInteger(0);

    private TcpServer(ServerSocketChannel serverChannel, NioEventLoop[] eventLoops) {
        this.serverChannel = serverChannel;
        this.eventLoops    = eventLoops;
    }

    public void start() {
        for (NioEventLoop loop : eventLoops) {
            loop.start();
        }
        log.info("TcpServer started on " + serverChannel.socket().getLocalSocketAddress()
                + " selectorThreads=" + eventLoops.length);
    }

    public void shutdown() {
        // Close the server channel first so no new accepts race with event-loop shutdown
        try {
            serverChannel.close();
        } catch (IOException e) {
            log.log(Level.WARNING, "Error closing server channel", e);
        }
        for (NioEventLoop loop : eventLoops) {
            loop.shutdown();
        }
        log.info("TcpServer stopped");
    }

    /**
     * Returns an event loop from the pool using round-robin selection.
     * All outgoing connections should be opened through this method so that I/O is
     * distributed evenly across selector threads.
     */
    public NioEventLoop eventLoop() {
        int idx = Math.floorMod(rrCounter.getAndIncrement(), eventLoops.length);
        return eventLoops[idx];
    }

    public int localPort() {
        return serverChannel.socket().getLocalPort();
    }

    public void onConnect(Consumer<Connection> listener) {
        // All loops must receive the listener: loops[0] fires it for single-loop mode,
        // loops[1..N-1] fire it for handed-off connections in multi-loop mode.
        for (NioEventLoop loop : eventLoops) {
            loop.setAcceptListener(listener);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private String             bindAddress        = "0.0.0.0";
        private int                port               = 0;
        private FrameHandler       handler;
        private int                workerThreads      = 4;
        private int                selectorThreads    = 1;
        private int                readBufferBytes    = 65536;
        private TlsConfig          tls                = TlsConfig.disabled();
        private BackpressurePolicy backpressurePolicy = BackpressurePolicy.DROP_OLDEST;
        private int                queueCapacity      = WriteQueue.DEFAULT_CAPACITY;
        private int                maxPayloadBytes    = RawFrame.DEFAULT_MAX_PAYLOAD;

        public Builder bindAddress(String bindAddress) {
            this.bindAddress = bindAddress;
            return this;
        }

        public Builder port(int port) {
            this.port = port;
            return this;
        }

        public Builder handler(FrameHandler handler) {
            this.handler = handler;
            return this;
        }

        public Builder workerThreads(int workerThreads) {
            this.workerThreads = workerThreads;
            return this;
        }

        /**
         * Number of NIO selector threads. Default is 1, which is sufficient for clusters
         * up to ~20 nodes. For larger clusters (50–100+ nodes) increase this to 2–4 so
         * that incoming frame reads are distributed across multiple selector threads.
         *
         * <p>Each selector thread has its own worker pool of {@code workerThreads} threads,
         * so total I/O workers = {@code selectorThreads × workerThreads}.</p>
         *
         * <p>Rule of thumb: 1 selector per ~30 nodes, capped at the number of CPU cores.</p>
         */
        public Builder selectorThreads(int selectorThreads) {
            if (selectorThreads < 1) {
                throw new IllegalArgumentException("selectorThreads must be >= 1");
            }
            this.selectorThreads = selectorThreads;
            return this;
        }

        public Builder readBufferBytes(int readBufferBytes) {
            this.readBufferBytes = readBufferBytes;
            return this;
        }

        public Builder tls(TlsConfig tls) {
            this.tls = tls;
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

        public TcpServer build() throws IOException {
            if (handler == null) {
                throw new IllegalStateException("FrameHandler must be set");
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

            ServerSocketChannel ssc = ServerSocketChannel.open();
            try {
                ssc.socket().setReuseAddress(true);
                ssc.bind(new InetSocketAddress(bindAddress, port));
            } catch (IOException e) {
                try { ssc.close(); } catch (IOException ignored) {}
                throw e;
            }

            // Create the event-loop pool. All loops share the same FrameHandler and TLS config.
            NioEventLoop[] loops = new NioEventLoop[selectorThreads];
            try {
                for (int i = 0; i < selectorThreads; i++) {
                    loops[i] = new NioEventLoop(handler, workerThreads, readBufferBytes,
                            maxPayloadBytes, backpressurePolicy, queueCapacity, tls);
                }

                // loops[0] is the accept loop: owns the OP_ACCEPT key.
                loops[0].registerServerChannel(ssc);

                if (selectorThreads > 1) {
                    // Wire round-robin handoff so accepted connections are distributed across
                    // all loops evenly. Each loop handles both accepted and outgoing connections,
                    // so there is no distinction between "accept loop" and "worker loop" beyond
                    // loops[0] owning the ServerSocketChannel key.
                    AtomicInteger rr = new AtomicInteger(0);
                    NioEventLoop[] loopsRef = loops;
                    loops[0].setAcceptHandoff(sc -> {
                        int idx = Math.floorMod(rr.getAndIncrement(), loopsRef.length);
                        loopsRef[idx].registerAcceptedChannel(sc);
                    });
                }
            } catch (IOException e) {
                for (NioEventLoop loop : loops) {
                    if (loop != null) loop.shutdown();
                }
                try { ssc.close(); } catch (IOException ignored) {}
                throw e;
            }

            return new TcpServer(ssc, loops);
        }
    }
}
