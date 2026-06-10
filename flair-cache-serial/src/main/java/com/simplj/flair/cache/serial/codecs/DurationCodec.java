package com.simplj.flair.cache.serial.codecs;

import com.simplj.flair.cache.serial.Codec;

import java.nio.ByteBuffer;
import java.time.Duration;

/**
 * Codec for {@link Duration}. Wire format: seconds(8) + nanos(4) = 12 bytes.
 *
 * <p>Nanos are always in [0, 999_999_999]; the sign of the duration is carried by
 * the seconds field. Negative durations round toward negative infinity
 * (e.g. -1.5s is stored as seconds=-2, nanos=500_000_000).
 *
 * <p>{@code null} throws {@link NullPointerException}. Use
 * {@code new OptionalCodec<>(DurationCodec.INSTANCE)} for nullable duration fields.
 */
public final class DurationCodec implements Codec<Duration> {

    public static final DurationCodec INSTANCE = new DurationCodec();

    private DurationCodec() {}

    @Override
    public void serialize(Duration d, ByteBuffer buf) {
        buf.putLong(d.getSeconds());
        buf.putInt(d.getNano());
    }

    @Override
    public int sizeOf(Duration d) { return 12; }

    @Override
    public Duration deserialize(ByteBuffer buf) {
        return Duration.ofSeconds(buf.getLong(), buf.getInt());
    }
}
