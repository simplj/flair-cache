package com.simplj.flair.cache.serial.codecs;

import com.simplj.flair.cache.serial.Codec;

import java.nio.ByteBuffer;

public final class EnumCodec<E extends Enum<E>> implements Codec<E> {

    private final E[] values;

    public EnumCodec(Class<E> enumClass) {
        this.values = enumClass.getEnumConstants();
        if (this.values.length > 256) {
            throw new IllegalArgumentException(
                    "EnumCodec encodes ordinal as 1 byte (max 256 constants); "
                    + enumClass.getSimpleName() + " has " + this.values.length);
        }
    }

    @Override
    public void serialize(E obj, ByteBuffer buf) {
        buf.put((byte) obj.ordinal());
    }

    @Override
    public int sizeOf(E obj) {
        return 1;
    }

    @Override
    public E deserialize(ByteBuffer buf) {
        return values[buf.get() & 0xFF];
    }
}
