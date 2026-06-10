package com.simplj.flair.cache.serial.codecs;

import com.simplj.flair.cache.serial.Codec;

import java.math.BigInteger;
import java.nio.ByteBuffer;

/**
 * Codec for {@link BigInteger}. Wire format: length(4) + two's-complement bytes.
 *
 * <p>{@code null} serializes as length=0 and deserializes back as {@link BigInteger#ZERO}.
 * If you need to distinguish null from zero, wrap with {@link OptionalCodec}.
 *
 * <p>Note: {@link #sizeOf} calls {@link BigInteger#toByteArray()} internally — one allocation
 * per call is unavoidable. Avoid calling {@code sizeOf} in hot loops.
 */
public final class BigIntegerCodec implements Codec<BigInteger> {

    public static final BigIntegerCodec INSTANCE = new BigIntegerCodec();

    private BigIntegerCodec() {}

    @Override
    public void serialize(BigInteger obj, ByteBuffer buf) {
        if (obj == null) { buf.putInt(0); return; }
        byte[] bytes = obj.toByteArray();
        buf.putInt(bytes.length);
        buf.put(bytes);
    }

    @Override
    public int sizeOf(BigInteger obj) {
        return obj == null ? 4 : 4 + obj.toByteArray().length;
    }

    @Override
    public BigInteger deserialize(ByteBuffer buf) {
        int len = buf.getInt();
        if (len == 0) return BigInteger.ZERO;
        byte[] bytes = new byte[len];
        buf.get(bytes);
        return new BigInteger(bytes);
    }
}
