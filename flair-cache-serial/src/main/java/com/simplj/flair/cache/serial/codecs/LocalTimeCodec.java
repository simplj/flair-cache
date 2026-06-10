package com.simplj.flair.cache.serial.codecs;

import com.simplj.flair.cache.serial.Codec;

import java.nio.ByteBuffer;
import java.time.LocalTime;

/**
 * Codec for {@link LocalTime}. Wire format: nanoOfDay(8) = 8 bytes.
 *
 * <p>{@code null} throws {@link NullPointerException}. Use
 * {@code new OptionalCodec<>(LocalTimeCodec.INSTANCE)} for nullable time fields.
 */
public final class LocalTimeCodec implements Codec<LocalTime> {

    public static final LocalTimeCodec INSTANCE = new LocalTimeCodec();

    private LocalTimeCodec() {}

    @Override
    public void serialize(LocalTime t, ByteBuffer buf) { buf.putLong(t.toNanoOfDay()); }

    @Override
    public int sizeOf(LocalTime t) { return 8; }

    @Override
    public LocalTime deserialize(ByteBuffer buf) { return LocalTime.ofNanoOfDay(buf.getLong()); }
}
