package com.simplj.flair.cache.transport;

import com.simplj.flair.cache.commons.FlairCacheThreadFactory;
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
import java.util.concurrent.atomic.AtomicInteger;
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
    // Counts frames that are enqueued OR have been dequeued into the batch but not yet flushed
    // to the socket. Incremented in enqueue(), decremented after flush() completes. This means
    // pendingCount() returns 0 only when the OS send buffer has received every frame, not merely
    // when the in-memory queue is empty (which happens before the 2ms batch window elapses).
    private final AtomicInteger pending = new AtomicInteger(0);
    private final BackpressurePolicy policy;
    private final SocketChannel channel;
    private final SSLEngine engine; // null for plaintext
    private ByteBuffer tlsNetBuf;   // ciphertext output buffer; may grow on BUFFER_OVERFLOW
    private final Thread worker;
    private volatile boolean running = true;
    private volatile boolean writeFailed = false;
    // Set by drainAndDiscard() so the worker's post-loop drain does NOT flush queued frames.
    private volatile boolean discarding = false;

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
                // Claim the pending slot BEFORE offering so that the drainLoop can never
                // dequeue and batch the frame while pending is still 0. If offer() fails
                // (shutdown or interrupt), decrement to undo the claim.
                pending.incrementAndGet();
                try {
                    while (!queue.offer(frame, 1, TimeUnit.MILLISECONDS)) {
                        if (!running || writeFailed) {
                            pending.decrementAndGet();
                            return;
                        }
                    }
                } catch (InterruptedException e) {
                    pending.decrementAndGet();
                    Thread.currentThread().interrupt();
                }
                break;
            case DROP_OLDEST:
                // Claim slot for the new frame first. Each evicted frame was already counted
                // in pending when it was originally enqueued; decrement for it here since it
                // will never be flushed.
                pending.incrementAndGet();
                while (!queue.offer(frame)) {
                    if (queue.poll() != null) {
                        pending.decrementAndGet();
                    }
                }
                break;
            case DROP_NEWEST:
                // Claim slot first; undo immediately if the frame is dropped.
                pending.incrementAndGet();
                if (!queue.offer(frame)) {
                    pending.decrementAndGet();
                }
                break;
        }
    }

    void shutdown() {
        running = false;
        worker.interrupt();
    }

    /**
     * Frames that are either in the queue OR have been dequeued into the batch but not yet
     * flushed to the TCP socket. Returns 0 only when the OS send buffer has received every frame.
     */
    int pendingCount() {
        return pending.get();
    }

    /**
     * Bounded flush: signals the writer to stop accepting new work and to drain what remains,
     * then waits up to {@code timeoutMs} for the queue to empty and the worker to finish.
     * Does NOT interrupt the worker while it is flushing — interruption is only used as a last
     * resort once the timeout elapses. Used for a graceful peer leave.
     */
    void flushAndShutdown(long timeoutMs) {
        running = false; // stops the drainLoop's while(running); it then drains the queue best-effort
        long deadline = System.nanoTime() + timeoutMs * 1_000_000L;
        try {
            while (worker.isAlive() && System.nanoTime() < deadline) {
                worker.join(1L);
                if (queue.isEmpty() && !worker.isAlive()) break;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        // Timed out (or interrupted) with the worker still running — force it down.
        if (worker.isAlive()) {
            worker.interrupt();
        }
    }

    /**
     * Drains and discards every queued frame without sending, then shuts the writer down.
     * Returns the number of frames discarded. Used when the peer is unreachable (dead) and
     * flushing would only block.
     */
    int drainAndDiscard() {
        // Set discarding before stopping the loop so the worker's post-loop drain skips flushing.
        discarding = true;
        running = false;
        worker.interrupt(); // break the worker out of its 1ms poll so it exits without flushing
        try {
            worker.join(100L);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        int discarded = 0;
        while (queue.poll() != null) {
            discarded++;
        }
        return discarded;
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
                    int batchSize = batch.size();
                    flush(batch, writeBuf);
                    pending.addAndGet(-batchSize);
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

        // If we are discarding (peer declared dead), do NOT flush — the caller will drain the
        // queue and count the dropped frames.
        if (discarding) {
            return;
        }

        // Flush whatever remains in the current batch
        if (!writeFailed && !batch.isEmpty()) {
            try {
                int batchSize = batch.size();
                flush(batch, writeBuf);
                pending.addAndGet(-batchSize);
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
                        int batchSize = batch.size();
                        flush(batch, writeBuf);
                        pending.addAndGet(-batchSize);
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
                    int batchSize = batch.size();
                    flush(batch, writeBuf);
                    pending.addAndGet(-batchSize);
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
