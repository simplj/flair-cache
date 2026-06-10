package com.simplj.flair.cache.serial.codecs;

import com.simplj.flair.cache.serial.Codec;

import java.nio.ByteBuffer;
import java.util.UUID;

public final class UuidCodec implements Codec<UUID> {

    public static final UuidCodec INSTANCE = new UuidCodec();

    private UuidCodec() {}

    @Override
    public void serialize(UUID obj, ByteBuffer buf) {
        buf.putLong(obj.getMostSignificantBits());
        buf.putLong(obj.getLeastSignificantBits());
    }

    @Override
    public int sizeOf(UUID obj) {
        return 16;
    }

    @Override
    public UUID deserialize(ByteBuffer buf) {
        long msb = buf.getLong();
        long lsb = buf.getLong();
        return new UUID(msb, lsb);
    }
}
