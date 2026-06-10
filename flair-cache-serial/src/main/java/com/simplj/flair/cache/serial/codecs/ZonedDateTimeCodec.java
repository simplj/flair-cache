package com.simplj.flair.cache.serial.codecs;

import com.simplj.flair.cache.serial.Codec;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;

/**
 * Codec for {@link ZonedDateTime}. Wire format:
 * epochSecond(8) + nanos(4) + zoneId(String) = 12 + variable bytes.
 *
 * <p>The zone ID is serialized as a UTF-8 string (e.g. {@code "America/New_York"} or
 * {@code "+05:30"}). The point-in-time is preserved; the resulting local time after
 * deserialization equals the original local time in that zone.
 *
 * <p>{@code null} throws {@link NullPointerException}. Use
 * {@code new OptionalCodec<>(ZonedDateTimeCodec.INSTANCE)} for nullable fields.
 */
public final class ZonedDateTimeCodec implements Codec<ZonedDateTime> {

    public static final ZonedDateTimeCodec INSTANCE = new ZonedDateTimeCodec();

    private ZonedDateTimeCodec() {}

    @Override
    public void serialize(ZonedDateTime zdt, ByteBuffer buf) {
        buf.putLong(zdt.toEpochSecond());
        buf.putInt(zdt.getNano());
        StringCodec.INSTANCE.serialize(zdt.getZone().getId(), buf);
    }

    @Override
    public int sizeOf(ZonedDateTime zdt) {
        return 12 + StringCodec.INSTANCE.sizeOf(zdt.getZone().getId());
    }

    @Override
    public ZonedDateTime deserialize(ByteBuffer buf) {
        Instant instant = Instant.ofEpochSecond(buf.getLong(), buf.getInt());
        return ZonedDateTime.ofInstant(instant, ZoneId.of(StringCodec.INSTANCE.deserialize(buf)));
    }
}
