package com.simplj.flair.cache.transport;

import javax.net.ssl.SSLEngine;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Level;
import java.util.logging.Logger;

final class ConnectionImpl implements Connection {

    private static final Logger log = Logger.getLogger(ConnectionImpl.class.getName());

    private static final int TLS_PACKET_BUFFER_SIZE = 32 * 1024; // 32 KB minimum

    final UUID id;
    final SocketChannel channel;
    final FrameAssembler assembler;
    final WriteQueue writeQueue;

    // Pre-allocated read buffer — used by selector thread only, never shared
    final ByteBuffer readBuf;

    // TLS — null if plaintext
    final SSLEngine engine;
    final ByteBuffer tlsNetBuf;  // encrypted bytes from network (unwrap input)
    ByteBuffer tlsAppBuf;        // decrypted application bytes (unwrap output) — may grow

    // Set for TLS connections: completed by the selector thread once the handshake finishes
    // (client side) or used to decide whether to notify the accept listener (server side).
    volatile boolean tlsHandshakeDone;
    // For TLS client connections: the CompletableFuture that TcpClient.connect() is waiting on.
    // Completed after TLS handshake so the caller cannot send application data before the
    // record layer is established. Null for server-accepted connections.
    volatile CompletableFuture<Connection> pendingFuture;

    private final InetAddress remoteAddress;
    volatile boolean alive = true;

    ConnectionImpl(SocketChannel channel, int readBufferBytes, int maxPayloadBytes,
                   BackpressurePolicy policy, int queueCapacity, SSLEngine engine) {
        this.id = UUID.randomUUID();
        this.channel = channel;
        this.remoteAddress = channel.socket().getInetAddress();
        this.assembler = new FrameAssembler(maxPayloadBytes);
        this.readBuf = ByteBuffer.allocateDirect(readBufferBytes);
        this.engine = engine;
        // Pass engine so WriteQueue can TLS-wrap outbound application data.
        // WriteQueue gates writes behind applicationDataReady; for plaintext it is immediately true.
        this.writeQueue = new WriteQueue(id, channel, queueCapacity, policy, engine);
        if (engine != null) {
            int netSize = engine.getSession().getPacketBufferSize();
            int appSize = engine.getSession().getApplicationBufferSize();
            this.tlsNetBuf = ByteBuffer.allocateDirect(Math.max(netSize, TLS_PACKET_BUFFER_SIZE));
            this.tlsAppBuf = ByteBuffer.allocateDirect(Math.max(appSize, readBufferBytes));
        } else {
            this.tlsNetBuf = null;
            this.tlsAppBuf = null;
        }
    }

    /** Doubles the TLS application buffer when SSLEngine reports BUFFER_OVERFLOW. */
    void growTlsAppBuf() {
        int newCapacity = tlsAppBuf.capacity() * 2;
        ByteBuffer larger = ByteBuffer.allocateDirect(newCapacity);
        tlsAppBuf.flip();
        larger.put(tlsAppBuf);
        tlsAppBuf = larger;
    }

    @Override
    public UUID id() {
        return id;
    }

    @Override
    public InetAddress remoteAddress() {
        return remoteAddress;
    }

    @Override
    public void send(RawFrame frame) {
        // Check both alive and write-health so frames are never enqueued after a failure
        if (!isAlive()) {
            return;
        }
        writeQueue.enqueue(frame);
    }

    @Override
    public void close() {
        alive = false;
        writeQueue.shutdown();
        try {
            channel.close();
        } catch (IOException e) {
            log.log(Level.FINEST, "Error closing channel " + id, e);
        }
    }

    @Override
    public int pendingWrites() {
        return writeQueue.pendingCount();
    }

    @Override
    public void closeGraceful(long flushTimeoutMs) {
        alive = false;
        // Bounded flush: let the writer drain queued frames for up to flushTimeoutMs.
        writeQueue.flushAndShutdown(flushTimeoutMs);
        try {
            channel.close();
        } catch (IOException e) {
            log.log(Level.FINEST, "Error closing channel " + id, e);
        }
    }

    @Override
    public int closeAndDiscard() {
        alive = false;
        int discarded = writeQueue.drainAndDiscard();
        try {
            channel.close();
        } catch (IOException e) {
            log.log(Level.FINEST, "Error closing channel " + id, e);
        }
        return discarded;
    }

    @Override
    public boolean isAlive() {
        return alive && !writeQueue.hasWriteFailed();
    }
}
