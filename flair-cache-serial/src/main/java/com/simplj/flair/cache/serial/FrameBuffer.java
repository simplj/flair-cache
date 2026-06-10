package com.simplj.flair.cache.serial;

import java.nio.ByteBuffer;

/**
 * Pre-allocated, reusable write buffer backed by a direct ByteBuffer.
 * One instance per writer thread — never allocate per frame.
 */
public final class FrameBuffer {

    private final ByteBuffer buffer;

    private FrameBuffer(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    public static FrameBuffer allocateDirect(int capacityBytes) {
        return new FrameBuffer(ByteBuffer.allocateDirect(capacityBytes));
    }

    public ByteBuffer buffer() {
        return buffer;
    }

    public void reset() {
        buffer.clear();
    }

    /**
     * Returns a snapshot of the bytes written so far without disturbing the buffer's write position.
     */
    public byte[] toBytes() {
        ByteBuffer dup = buffer.duplicate();
        dup.flip();
        byte[] bytes = new byte[dup.remaining()];
        dup.get(bytes);
        return bytes;
    }
}
