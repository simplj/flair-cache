package com.simplj.flair.cache.serial.codecs;

import com.simplj.flair.cache.serial.Codec;

import java.nio.ByteBuffer;
import java.time.Period;

/**
 * Codec for {@link Period}. Wire format: years(4) + months(4) + days(4) = 12 bytes.
 *
 * <p>{@code null} throws {@link NullPointerException}. Use
 * {@code new OptionalCodec<>(PeriodCodec.INSTANCE)} for nullable period fields.
 */
public final class PeriodCodec implements Codec<Period> {

    public static final PeriodCodec INSTANCE = new PeriodCodec();

    private PeriodCodec() {}

    @Override
    public void serialize(Period p, ByteBuffer buf) {
        buf.putInt(p.getYears());
        buf.putInt(p.getMonths());
        buf.putInt(p.getDays());
    }

    @Override
    public int sizeOf(Period p) { return 12; }

    @Override
    public Period deserialize(ByteBuffer buf) {
        return Period.of(buf.getInt(), buf.getInt(), buf.getInt());
    }
}
