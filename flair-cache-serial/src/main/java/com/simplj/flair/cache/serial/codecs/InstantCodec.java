package com.simplj.flair.cache.serial.codecs;

import com.simplj.flair.cache.serial.Codec;

import java.nio.ByteBuffer;
import java.time.Instant;

public final class InstantCodec implements Codec<Instant> {

    public static final InstantCodec INSTANCE = new InstantCodec();

    private InstantCodec() {}

    @Override
    public void serialize(Instant obj, ByteBuffer buf) {
        buf.putLong(obj.getEpochSecond());
        buf.putInt(obj.getNano());
    }

    @Override
    public int sizeOf(Instant obj) {
        return 12;
    }

    @Override
    public Instant deserialize(ByteBuffer buf) {
        long epochSecond = buf.getLong();
        int nano = buf.getInt();
        return Instant.ofEpochSecond(epochSecond, nano);
    }
}
