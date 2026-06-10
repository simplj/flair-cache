package com.simplj.flair.cache.serial.codecs;

import com.simplj.flair.cache.serial.Codec;

import java.nio.ByteBuffer;

public final class ByteArrayCodec implements Codec<byte[]> {

    public static final ByteArrayCodec INSTANCE = new ByteArrayCodec();

    private ByteArrayCodec() {}

    @Override
    public void serialize(byte[] obj, ByteBuffer buf) {
        if (obj == null || obj.length == 0) {
            buf.putInt(0);
            return;
        }
        buf.putInt(obj.length);
        buf.put(obj);
    }

    @Override
    public int sizeOf(byte[] obj) {
        return 4 + (obj == null ? 0 : obj.length);
    }

    @Override
    public byte[] deserialize(ByteBuffer buf) {
        int len = buf.getInt();
        if (len == 0) return new byte[0];
        byte[] bytes = new byte[len];
        buf.get(bytes);
        return bytes;
    }
}
