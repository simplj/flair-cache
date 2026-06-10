package com.simplj.flair.cache.serial.codecs;

import com.simplj.flair.cache.serial.Codec;

import java.nio.ByteBuffer;

public final class PrimitiveCodecs {

    private PrimitiveCodecs() {}

    public static final Codec<Byte> BYTE = new Codec<>() {
        @Override
        public void serialize(Byte v, ByteBuffer buf) {
            buf.put(v);
        }

        @Override
        public int sizeOf(Byte v) {
            return 1;
        }

        @Override
        public Byte deserialize(ByteBuffer buf) {
            return buf.get();
        }
    };

    public static final Codec<Short> SHORT = new Codec<>() {
        @Override
        public void serialize(Short v, ByteBuffer buf) {
            buf.putShort(v);
        }

        @Override
        public int sizeOf(Short v) {
            return 2;
        }

        @Override
        public Short deserialize(ByteBuffer buf) {
            return buf.getShort();
        }
    };

    public static final Codec<Integer> INT = new Codec<>() {
        @Override
        public void serialize(Integer v, ByteBuffer buf) {
            buf.putInt(v);
        }

        @Override
        public int sizeOf(Integer v) {
            return 4;
        }

        @Override
        public Integer deserialize(ByteBuffer buf) {
            return buf.getInt();
        }
    };

    public static final Codec<Long> LONG = new Codec<>() {
        @Override
        public void serialize(Long v, ByteBuffer buf) {
            buf.putLong(v);
        }

        @Override
        public int sizeOf(Long v) {
            return 8;
        }

        @Override
        public Long deserialize(ByteBuffer buf) {
            return buf.getLong();
        }
    };

    public static final Codec<Float> FLOAT = new Codec<>() {
        @Override
        public void serialize(Float v, ByteBuffer buf) {
            buf.putFloat(v);
        }

        @Override
        public int sizeOf(Float v) {
            return 4;
        }

        @Override
        public Float deserialize(ByteBuffer buf) {
            return buf.getFloat();
        }
    };

    public static final Codec<Double> DOUBLE = new Codec<>() {
        @Override
        public void serialize(Double v, ByteBuffer buf) {
            buf.putDouble(v);
        }

        @Override
        public int sizeOf(Double v) {
            return 8;
        }

        @Override
        public Double deserialize(ByteBuffer buf) {
            return buf.getDouble();
        }
    };

    public static final Codec<Boolean> BOOLEAN = new Codec<>() {
        @Override
        public void serialize(Boolean v, ByteBuffer buf) {
            buf.put(v ? (byte) 1 : (byte) 0);
        }

        @Override
        public int sizeOf(Boolean v) {
            return 1;
        }

        @Override
        public Boolean deserialize(ByteBuffer buf) {
            return buf.get() != 0;
        }
    };

    public static final Codec<Character> CHAR = new Codec<>() {
        @Override
        public void serialize(Character v, ByteBuffer buf) {
            buf.putChar(v);
        }

        @Override
        public int sizeOf(Character v) {
            return 2;
        }

        @Override
        public Character deserialize(ByteBuffer buf) {
            return buf.getChar();
        }
    };
}
