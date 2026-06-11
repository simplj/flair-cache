package com.simplj.flair.cache.transport;

import java.nio.ByteBuffer;

public final class RawFrame {

    public static final int HEADER_BYTES = 8;
    static final byte MAGIC_0 = (byte) 0xCA;
    static final byte MAGIC_1 = (byte) 0xFE;
    static final byte VERSION = 0x01;
    static final int DEFAULT_MAX_PAYLOAD = 1 << 20;       // 1 MB
    static final int ABSOLUTE_MAX_PAYLOAD = 64 << 20;     // 64 MB

    private final byte type;
    private final byte[] payload;

    public RawFrame(byte type, byte[] payload) {
        this.type = type;
        this.payload = payload;
    }

    public byte type() {
        return type;
    }

    public byte[] payload() {
        return payload;
    }

    public int encodedLength() {
        return HEADER_BYTES + payload.length;
    }

    public void encodeTo(ByteBuffer buf) {
        buf.put(MAGIC_0);
        buf.put(MAGIC_1);
        buf.put(VERSION);
        buf.put(type);
        buf.putInt(payload.length);
        buf.put(payload);
    }
}
