package com.simplj.flair.cache.transport;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.logging.Level;
import java.util.logging.Logger;

final class WriteQueue {

    private static final Logger log = Logger.getLogger(WriteQueue.class.getName());

    static final int DEFAULT_CAPACITY = 4096;
    private static final int BATCH_SIZE = 64;
    private static final long FLUSH_NANOS = 2_000_000L;   // 2 ms
    private static final int WRITE_BUF_BYTES = 512 * 1024; // 512 KB pre-allocated per writer thread

    private final ArrayBlockingQueue<RawFrame> queue;
    private final BackpressurePolicy policy;
    private final SocketChannel channel;
    private final SSLEngine engine; // null for plaintext
    private ByteBuffer tlsNetBuf;   // ciphertext output buffer; may grow on BUFFER_OVERFLOW
    private final Thread worker;
    private volatile boolean running = true;
    private volatile boolean writeFailed = false;

    // Controlled by NioEventLoop: false until TLS handshake completes; always true for plaintext.
    // The writer thread holds all frames until this is set so application data is never sent before
    // the TLS record layer is established. SSLEngine.wrap() during an active handshake is unsafe
    // because the selector thread concurrently calls wrap() for handshake records.
    volatile boolean applicationDataReady;

    WriteQueue(UUID peerId, SocketChannel channel, int capacity, BackpressurePolicy policy,
               SSLEngine engine) {
        this.channel = channel;
        this.policy = policy;
        this.engine = engine;
        this.queue = new ArrayBlockingQueue<>(capacity);
        this.applicationDataReady = (engine == null); // plaintext is always ready
        if (engine != null) {
            this.tlsNetBuf = ByteBuffer.allocateDirect(engine.getSession().getPacketBufferSize());
        }
        this.worker = new FlairCacheThreadFactory("flaircache-writer-" + peerId).newThread(this::drainLoop);
        this.worker.start();
    }

    void enqueue(RawFrame frame) {
        if (!running || writeFailed) {
            return;
        }
        switch (policy) {
            case BLOCK:
                // Use offer() with timeout rather than put() so the loop can re-check
                // writeFailed every millisecond. If put() were used and the writer thread died
                // after the initial running/writeFailed check, the caller would block forever on
                // a full queue that nothing is draining.
                try {
                    while (!queue.offer(frame, 1, TimeUnit.MILLISECONDS)) {
                        if (!running || writeFailed) {
                            return;
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                break;
            case DROP_OLDEST:
                // Retry until the offer succeeds; each iteration makes space by polling the
                // oldest frame. Under concurrent senders the loop may need more than one pass.
                while (!queue.offer(frame)) {
                    queue.poll(); // evict oldest to make room
                }
                break;
            case DROP_NEWEST:
                queue.offer(frame); // silently drops new frame if full
                break;
        }
    }

    void shutdown() {
        running = false;
        worker.interrupt();
    }

    boolean hasWriteFailed() {
        return writeFailed;
    }

    private void drainLoop() {
        // Pre-allocated per writer thread — never re-allocated in the hot path
        ByteBuffer writeBuf = ByteBuffer.allocateDirect(WRITE_BUF_BYTES);
        List<RawFrame> batch = new ArrayList<>(BATCH_SIZE);
        long lastFlushNanos = System.nanoTime();

        while (running) {
            try {
                RawFrame frame = queue.poll(1, TimeUnit.MILLISECONDS);
                if (frame != null) {
                    batch.add(frame);
                }

                if (!applicationDataReady) {
                    // TLS handshake not yet complete — hold frames until the selector thread
                    // signals readiness. Short park avoids spinning while waiting.
                    LockSupport.parkNanos(1_000_000L); // 1 ms
                    continue;
                }

                long now = System.nanoTime();
                boolean timedOut = (now - lastFlushNanos) >= FLUSH_NANOS;
                boolean full = batch.size() >= BATCH_SIZE;

                if ((timedOut || full) && !batch.isEmpty()) {
                    flush(batch, writeBuf);
                    batch.clear();
                    lastFlushNanos = now;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (IOException e) {
                log.log(Level.WARNING, "Write error on peer channel", e);
                writeFailed = true;
                break;
            }
        }

        // Flush whatever remains in the current batch
        if (!writeFailed && !batch.isEmpty()) {
            try {
                flush(batch, writeBuf);
            } catch (IOException e) {
                log.log(Level.WARNING, "Final batch flush error", e);
                writeFailed = true;
            }
            batch.clear();
        }

        // Drain any frames still in the queue
        if (!writeFailed) {
            RawFrame remaining;
            while ((remaining = queue.poll()) != null) {
                batch.add(remaining);
                if (batch.size() >= BATCH_SIZE) {
                    try {
                        flush(batch, writeBuf);
                    } catch (IOException e) {
                        log.log(Level.WARNING, "Drain flush error", e);
                        writeFailed = true;
                        break;
                    }
                    batch.clear();
                }
            }
            if (!writeFailed && !batch.isEmpty()) {
                try {
                    flush(batch, writeBuf);
                } catch (IOException e) {
                    log.log(Level.WARNING, "Terminal drain flush error", e);
                }
            }
        }
    }

    private void flush(List<RawFrame> frames, ByteBuffer writeBuf) throws IOException {
        if (engine != null) {
            flushTls(frames, writeBuf);
            return;
        }
        for (RawFrame frame : frames) {
            int needed = frame.encodedLength();
            if (needed > writeBuf.capacity()) {
                // Single frame exceeds the pre-allocated buffer; flush buffer then write frame directly
                if (writeBuf.position() > 0) {
                    writeBuf.flip();
                    writeAll(writeBuf);
                    writeBuf.clear();
                }
                ByteBuffer large = ByteBuffer.allocateDirect(needed);
                frame.encodeTo(large);
                large.flip();
                writeAll(large);
            } else {
                if (writeBuf.remaining() < needed) {
                    writeBuf.flip();
                    writeAll(writeBuf);
                    writeBuf.clear();
                }
                frame.encodeTo(writeBuf);
            }
        }
        if (writeBuf.position() > 0) {
            writeBuf.flip();
            writeAll(writeBuf);
            writeBuf.clear();
        }
    }

    // SSLEngine safety: one thread calling wrap() (this writer thread) and one thread calling
    // unwrap() (selector thread) concurrently is explicitly permitted by the SSLEngine contract.
    // We never call wrap() during the handshake (applicationDataReady gate above ensures this).
    private void flushTls(List<RawFrame> frames, ByteBuffer plainBuf) throws IOException {
        plainBuf.clear();
        for (RawFrame frame : frames) {
            int needed = frame.encodedLength();
            if (needed > plainBuf.capacity()) {
                // Frame exceeds pre-allocated buffer; flush current contents then wrap frame directly
                if (plainBuf.position() > 0) {
                    plainBuf.flip();
                    tlsWrapAll(plainBuf);
                    plainBuf.clear();
                }
                ByteBuffer large = ByteBuffer.allocateDirect(needed);
                frame.encodeTo(large);
                large.flip();
                tlsWrapAll(large);
            } else {
                if (plainBuf.remaining() < needed) {
                    plainBuf.flip();
                    tlsWrapAll(plainBuf);
                    plainBuf.clear();
                }
                frame.encodeTo(plainBuf);
            }
        }
        if (plainBuf.position() > 0) {
            plainBuf.flip();
            tlsWrapAll(plainBuf);
            plainBuf.clear();
        }
    }

    private void tlsWrapAll(ByteBuffer plain) throws IOException {
        while (plain.hasRemaining()) {
            tlsNetBuf.clear();
            SSLEngineResult r = engine.wrap(plain, tlsNetBuf);
            switch (r.getStatus()) {
                case OK:
                    tlsNetBuf.flip();
                    writeAll(tlsNetBuf);
                    break;
                case BUFFER_OVERFLOW:
                    // SSLEngine needs a larger network buffer; grow and retry
                    tlsNetBuf = ByteBuffer.allocateDirect(tlsNetBuf.capacity() * 2);
                    break;
                case CLOSED:
                    throw new IOException("SSLEngine closed during application data write");
                case BUFFER_UNDERFLOW:
                    return; // cannot happen on wrap(); treat as done
            }
        }
    }

    private void writeAll(ByteBuffer buf) throws IOException {
        while (buf.hasRemaining()) {
            int written = channel.write(buf);
            if (written == 0) {
                if (!running) {
                    // Shutdown was requested while we were waiting for the send buffer to drain.
                    // Stop immediately — the channel will be closed by ConnectionImpl.close().
                    throw new IOException("Write queue shut down");
                }
                LockSupport.parkNanos(100_000L); // 100 µs — channel send buffer temporarily full
            }
        }
    }
}
