package com.simplj.flair.cache.transport;

import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

final class ByteBufferPool {

    private static final Logger log = Logger.getLogger(ByteBufferPool.class.getName());

    private final ArrayBlockingQueue<ByteBuffer> pool;
    private final int bufferCapacity;

    ByteBufferPool(int poolSize, int bufferCapacity) {
        this.bufferCapacity = bufferCapacity;
        this.pool = new ArrayBlockingQueue<>(poolSize);
        for (int i = 0; i < poolSize; i++) {
            pool.offer(ByteBuffer.allocateDirect(bufferCapacity));
        }
    }

    ByteBuffer acquire() {
        ByteBuffer buf = pool.poll();
        if (buf == null) {
            if (log.isLoggable(Level.FINEST)) {
                log.finest("Pool exhausted — allocating temporary direct buffer");
            }
            buf = ByteBuffer.allocateDirect(bufferCapacity);
        }
        buf.clear();
        return buf;
    }

    void release(ByteBuffer buf) {
        buf.clear();
        pool.offer(buf);
    }

    int bufferCapacity() {
        return bufferCapacity;
    }
}
