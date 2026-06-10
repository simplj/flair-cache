package com.simplj.flair.cache.serial.codecs;

import com.simplj.flair.cache.serial.Codec;

import java.nio.ByteBuffer;
import java.time.LocalDate;

/**
 * Codec for {@link LocalDate}. Wire format: year(4) + month(1) + day(1) = 6 bytes.
 * Supports the full range of {@code LocalDate} (year ±999,999,999).
 *
 * <p>{@code null} throws {@link NullPointerException}. Use
 * {@code new OptionalCodec<>(LocalDateCodec.INSTANCE)} for nullable date fields.
 */
public final class LocalDateCodec implements Codec<LocalDate> {

    public static final LocalDateCodec INSTANCE = new LocalDateCodec();

    private LocalDateCodec() {}

    @Override
    public void serialize(LocalDate d, ByteBuffer buf) {
        buf.putInt(d.getYear());
        buf.put((byte) d.getMonthValue());
        buf.put((byte) d.getDayOfMonth());
    }

    @Override
    public int sizeOf(LocalDate d) { return 6; }

    @Override
    public LocalDate deserialize(ByteBuffer buf) {
        int year = buf.getInt();
        return LocalDate.of(year, buf.get() & 0xFF, buf.get() & 0xFF);
    }
}
