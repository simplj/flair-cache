package com.simplj.flair.cache.transport;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class TcpServer {

    private static final Logger log = Logger.getLogger(TcpServer.class.getName());

    private final ServerSocketChannel serverChannel;
    private final NioEventLoop eventLoop;

    private TcpServer(ServerSocketChannel serverChannel, NioEventLoop eventLoop) {
        this.serverChannel = serverChannel;
        this.eventLoop = eventLoop;
    }

    public void start() {
        eventLoop.start();
        log.info("TcpServer started on " + serverChannel.socket().getLocalSocketAddress());
    }

    public void shutdown() {
        // Close the server channel first so no new accepts race with the event-loop shutdown
        try {
            serverChannel.close();
        } catch (IOException e) {
            log.log(Level.WARNING, "Error closing server channel", e);
        }
        eventLoop.shutdown();
        log.info("TcpServer stopped");
    }

    public NioEventLoop eventLoop() {
        return eventLoop;
    }

    public int localPort() {
        return serverChannel.socket().getLocalPort();
    }

    public void onConnect(Consumer<Connection> listener) {
        eventLoop.setAcceptListener(listener);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private String bindAddress = "0.0.0.0";
        private int port = 0;
        private FrameHandler handler;
        private int workerThreads = 4;
        private int readBufferBytes = 65536;
        private TlsConfig tls = TlsConfig.disabled();
        private BackpressurePolicy backpressurePolicy = BackpressurePolicy.DROP_OLDEST;
        private int queueCapacity = WriteQueue.DEFAULT_CAPACITY;
        private int maxPayloadBytes = RawFrame.DEFAULT_MAX_PAYLOAD;

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
                ssc.socket().setReuseAddress(true); // must be set before bind() to take effect
                ssc.bind(new InetSocketAddress(bindAddress, port));
            } catch (IOException e) {
                try { ssc.close(); } catch (IOException ignored) {}
                throw e;
            }

            NioEventLoop loop = new NioEventLoop(handler, workerThreads, readBufferBytes,
                    maxPayloadBytes, backpressurePolicy, queueCapacity, tls);
            try {
                loop.registerServerChannel(ssc);
            } catch (IOException e) {
                loop.shutdown();
                throw e;
            }

            return new TcpServer(ssc, loop);
        }
    }
}
