package com.simplj.flair.cache.serial.codecs;

import com.simplj.flair.cache.serial.Codec;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

/**
 * Codec for {@link OffsetDateTime}. Wire format:
 * epochSecond(8) + nanos(4) + totalOffsetSeconds(4) = 16 bytes.
 *
 * <p>The point in time and the UTC offset are both preserved, so the local time
 * components round-trip exactly.
 *
 * <p>{@code null} throws {@link NullPointerException}. Use
 * {@code new OptionalCodec<>(OffsetDateTimeCodec.INSTANCE)} for nullable fields.
 */
public final class OffsetDateTimeCodec implements Codec<OffsetDateTime> {

    public static final OffsetDateTimeCodec INSTANCE = new OffsetDateTimeCodec();

    private OffsetDateTimeCodec() {}

    @Override
    public void serialize(OffsetDateTime odt, ByteBuffer buf) {
        buf.putLong(odt.toEpochSecond());
        buf.putInt(odt.getNano());
        buf.putInt(odt.getOffset().getTotalSeconds());
    }

    @Override
    public int sizeOf(OffsetDateTime odt) { return 16; }

    @Override
    public OffsetDateTime deserialize(ByteBuffer buf) {
        Instant instant = Instant.ofEpochSecond(buf.getLong(), buf.getInt());
        return OffsetDateTime.ofInstant(instant, ZoneOffset.ofTotalSeconds(buf.getInt()));
    }
}
