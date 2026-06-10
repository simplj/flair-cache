package com.simplj.flair.cache.serial.codecs;

import com.simplj.flair.cache.serial.Codec;

import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

/**
 * Codec for {@link LocalDateTime}. Wire format:
 * year(4) + month(1) + day(1) + nanoOfDay(8) = 14 bytes.
 *
 * <p>{@code null} throws {@link NullPointerException}. Use
 * {@code new OptionalCodec<>(LocalDateTimeCodec.INSTANCE)} for nullable datetime fields.
 */
public final class LocalDateTimeCodec implements Codec<LocalDateTime> {

    public static final LocalDateTimeCodec INSTANCE = new LocalDateTimeCodec();

    private LocalDateTimeCodec() {}

    @Override
    public void serialize(LocalDateTime dt, ByteBuffer buf) {
        buf.putInt(dt.getYear());
        buf.put((byte) dt.getMonthValue());
        buf.put((byte) dt.getDayOfMonth());
        buf.putLong(dt.toLocalTime().toNanoOfDay());
    }

    @Override
    public int sizeOf(LocalDateTime dt) { return 14; }

    @Override
    public LocalDateTime deserialize(ByteBuffer buf) {
        int year = buf.getInt();
        LocalDate date = LocalDate.of(year, buf.get() & 0xFF, buf.get() & 0xFF);
        return LocalDateTime.of(date, LocalTime.ofNanoOfDay(buf.getLong()));
    }
}
