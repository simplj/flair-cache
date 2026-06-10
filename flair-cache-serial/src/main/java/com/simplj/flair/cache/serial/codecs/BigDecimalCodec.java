package com.simplj.flair.cache.serial.codecs;

import com.simplj.flair.cache.serial.Codec;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;

/**
 * Codec for {@link BigDecimal}. Wire format: unscaledValue ({@link BigIntegerCodec} —
 * variable) + scale(4).
 *
 * <p>{@code null} serializes as unscaled=ZERO + scale=0, which deserializes as
 * {@link BigDecimal#ZERO}. If you need to distinguish null from zero
 * (e.g. in financial contexts where "unknown" ≠ "zero"), wrap with {@link OptionalCodec}.
 */
public final class BigDecimalCodec implements Codec<BigDecimal> {

    public static final BigDecimalCodec INSTANCE = new BigDecimalCodec();

    private BigDecimalCodec() {}

    @Override
    public void serialize(BigDecimal obj, ByteBuffer buf) {
        if (obj == null) {
            BigIntegerCodec.INSTANCE.serialize(null, buf);
            buf.putInt(0);
            return;
        }
        BigIntegerCodec.INSTANCE.serialize(obj.unscaledValue(), buf);
        buf.putInt(obj.scale());
    }

    @Override
    public int sizeOf(BigDecimal obj) {
        if (obj == null) return BigIntegerCodec.INSTANCE.sizeOf(null) + 4;
        return BigIntegerCodec.INSTANCE.sizeOf(obj.unscaledValue()) + 4;
    }

    @Override
    public BigDecimal deserialize(ByteBuffer buf) {
        BigInteger unscaled = BigIntegerCodec.INSTANCE.deserialize(buf);
        return new BigDecimal(unscaled, buf.getInt());
    }
}
