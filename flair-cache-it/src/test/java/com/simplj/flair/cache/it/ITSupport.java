package com.simplj.flair.cache.it;

import com.simplj.flair.cache.serial.Codec;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.locks.LockSupport;

/**
 * Shared utilities for all FlairCache integration tests.
 *
 * <p>All methods are static. Tests should import statically or call via class name.</p>
 */
final class ITSupport {

    private ITSupport() {}

    // ── Codec helpers ─────────────────────────────────────────────────────────

    /** Simple String codec: 2-byte length prefix + UTF-8 bytes. */
    static final Codec<String> STRING_CODEC = new Codec<String>() {
        @Override
        public void serialize(String value, ByteBuffer buf) {
            byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
            buf.putShort((short) bytes.length);
            buf.put(bytes);
        }
        @Override
        public String deserialize(ByteBuffer buf) {
            int len = Short.toUnsignedInt(buf.getShort());
            byte[] bytes = new byte[len];
            buf.get(bytes);
            return new String(bytes, StandardCharsets.UTF_8);
        }
        @Override
        public int sizeOf(String value) {
            return 2 + value.getBytes(StandardCharsets.UTF_8).length;
        }
    };

    /** Integer codec: 4 bytes, big-endian. */
    static final Codec<Integer> INT_CODEC = new Codec<Integer>() {
        @Override
        public void serialize(Integer value, ByteBuffer buf) { buf.putInt(value); }
        @Override
        public Integer deserialize(ByteBuffer buf)           { return buf.getInt(); }
        @Override
        public int sizeOf(Integer value)                     { return 4; }
    };

    /** Double codec: 8 bytes, big-endian. */
    static final Codec<Double> DOUBLE_CODEC = new Codec<Double>() {
        @Override
        public void serialize(Double value, ByteBuffer buf) { buf.putDouble(value); }
        @Override
        public Double deserialize(ByteBuffer buf)           { return buf.getDouble(); }
        @Override
        public int sizeOf(Double value)                     { return 8; }
    };

    // ── Port allocation ───────────────────────────────────────────────────────

    /**
     * Finds a free ephemeral port by opening a ServerSocket at port 0 and reading the
     * OS-assigned port. The socket is closed immediately so the port is free for binding.
     *
     * <p>There is a small TOCTOU window between close and the caller's bind.
     * Tests use loopback only and run on dedicated fork JVMs so conflicts are rare.</p>
     */
    static int freePort() {
        try (ServerSocket s = new ServerSocket(0)) {
            s.setReuseAddress(true);
            return s.getLocalPort();
        } catch (IOException e) {
            throw new RuntimeException("Cannot allocate a free port", e);
        }
    }

    /**
     * Allocates {@code n} distinct free ports simultaneously so no port is reused between calls.
     * All n sockets are opened before any is closed, minimising reuse collisions.
     */
    static int[] freePorts(int n) {
        ServerSocket[] sockets = new ServerSocket[n];
        int[] ports = new int[n];
        try {
            for (int i = 0; i < n; i++) {
                sockets[i] = new ServerSocket(0);
                sockets[i].setReuseAddress(true);
                ports[i] = sockets[i].getLocalPort();
            }
        } catch (IOException e) {
            throw new RuntimeException("Cannot allocate free ports", e);
        } finally {
            for (ServerSocket s : sockets) {
                if (s != null) try { s.close(); } catch (IOException ignored) {}
            }
        }
        return ports;
    }

    // ── Polling helpers ───────────────────────────────────────────────────────

    /**
     * Polls {@code condition} every {@code pollMs} milliseconds until it returns {@code true}
     * or {@code timeout} elapses. Returns whether the condition was satisfied.
     */
    static boolean awaitCondition(Duration timeout, long pollMs, java.util.function.BooleanSupplier condition) {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) return true;
            LockSupport.parkNanos(pollMs * 1_000_000L);
        }
        return condition.getAsBoolean(); // one final check
    }
}
