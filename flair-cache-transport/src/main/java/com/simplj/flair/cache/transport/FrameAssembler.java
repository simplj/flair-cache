package com.simplj.flair.cache.transport;

import java.nio.ByteBuffer;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class FrameAssembler {

    private static final Logger log = Logger.getLogger(FrameAssembler.class.getName());

    private enum State { AWAIT_HEADER, AWAIT_PAYLOAD }

    private final int maxPayloadBytes;
    // Pre-allocated — reused across all frames on this connection
    private final ByteBuffer headerBuf = ByteBuffer.allocate(RawFrame.HEADER_BYTES);
    private State state = State.AWAIT_HEADER;
    private byte pendingType;
    private ByteBuffer payloadBuf;   // allocated once per frame

    public FrameAssembler() {
        this(RawFrame.DEFAULT_MAX_PAYLOAD);
    }

    public FrameAssembler(int maxPayloadBytes) {
        if (maxPayloadBytes < 0 || maxPayloadBytes > RawFrame.ABSOLUTE_MAX_PAYLOAD) {
            throw new IllegalArgumentException("maxPayloadBytes out of range: " + maxPayloadBytes);
        }
        this.maxPayloadBytes = maxPayloadBytes;
    }

    /**
     * Feed bytes from {@code src} into the assembler.
     * Complete frames are delivered to {@code frameConsumer} in order.
     * May be called multiple times as bytes arrive; {@code src} is fully consumed on return.
     * If {@code frameConsumer} throws, the assembler resets to a clean state so subsequent
     * calls can continue processing the next frame.
     *
     * @return {@code false} if a protocol error was detected (bad magic, bad version, or
     *         oversized payload); the caller MUST close the connection immediately because the
     *         stream is desynced and subsequent bytes cannot be reliably parsed.
     */
    public boolean feed(ByteBuffer src, Consumer<RawFrame> frameConsumer) {
        while (src.hasRemaining()) {
            if (state == State.AWAIT_HEADER) {
                drainInto(src, headerBuf);
                if (!headerBuf.hasRemaining()) {
                    headerBuf.flip();
                    if (!parseHeader()) {
                        return false; // protocol error — caller must close the connection
                    }
                    // Zero-length payload: dispatch immediately without another loop iteration
                    if (!payloadBuf.hasRemaining()) {
                        deliverFrame(frameConsumer);
                    }
                }
            } else {
                drainInto(src, payloadBuf);
                if (!payloadBuf.hasRemaining()) {
                    deliverFrame(frameConsumer);
                }
            }
        }
        return true;
    }

    private void deliverFrame(Consumer<RawFrame> frameConsumer) {
        byte type = pendingType;
        byte[] payload = payloadBuf.array();
        resetHeader();
        // Reset before dispatch so consumer exceptions leave the assembler in a clean state
        try {
            frameConsumer.accept(new RawFrame(type, payload));
        } catch (Exception e) {
            log.log(Level.WARNING, "FrameConsumer threw exception; frame discarded", e);
        }
    }

    private boolean parseHeader() {
        byte m0 = headerBuf.get();
        byte m1 = headerBuf.get();
        if (m0 != RawFrame.MAGIC_0 || m1 != RawFrame.MAGIC_1) {
            if (log.isLoggable(Level.WARNING)) {
                log.warning(String.format("Invalid frame magic: 0x%02X%02X", m0 & 0xFF, m1 & 0xFF));
            }
            resetHeader();
            return false;
        }
        byte ver = headerBuf.get();
        if (ver != RawFrame.VERSION) {
            if (log.isLoggable(Level.WARNING)) {
                log.warning("Unsupported frame version: " + (ver & 0xFF));
            }
            resetHeader();
            return false;
        }
        pendingType = headerBuf.get();
        int len = headerBuf.getInt();
        if (len < 0 || len > maxPayloadBytes) {
            if (log.isLoggable(Level.WARNING)) {
                log.warning("Invalid payload length: " + len);
            }
            resetHeader();
            return false;
        }
        try {
            payloadBuf = ByteBuffer.allocate(len);
        } catch (OutOfMemoryError e) {
            // A malicious or buggy peer sent a valid-but-huge payload length. OOM here must not
            // propagate to the selector thread and kill the whole process.
            log.log(Level.SEVERE, "Out of memory allocating payload buffer (" + len + " B); closing connection", e);
            resetHeader();
            return false;
        }
        state = State.AWAIT_PAYLOAD;
        return true;
    }

    private void resetHeader() {
        headerBuf.clear();
        payloadBuf = null;
        state = State.AWAIT_HEADER;
    }

    private static void drainInto(ByteBuffer src, ByteBuffer dst) {
        int n = Math.min(src.remaining(), dst.remaining());
        int savedLimit = src.limit();
        src.limit(src.position() + n);
        dst.put(src);
        src.limit(savedLimit);
    }
}
