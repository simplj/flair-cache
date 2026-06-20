package com.simplj.flair.cache.benchmarks;

import com.simplj.flair.cache.serial.Codec;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/** Shared codec instances for all benchmark classes. */
final class BenchmarkCodecs {

    static final Codec<String> STRING = new Codec<String>() {
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

    private BenchmarkCodecs() {}
}
